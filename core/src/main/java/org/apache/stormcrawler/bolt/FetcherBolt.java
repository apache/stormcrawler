/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.stormcrawler.bolt;

import crawlercommons.domains.PaidLevelDomain;
import crawlercommons.robots.BaseRobotRules;
import java.io.File;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.UnknownHostException;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHeaders;
import org.apache.storm.Config;
import org.apache.storm.metric.api.MeanReducer;
import org.apache.storm.metric.api.MultiCountMetric;
import org.apache.storm.metric.api.MultiReducedMetric;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.TupleUtils;
import org.apache.storm.utils.Utils;
import org.apache.stormcrawler.Constants;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.persistence.Status;
import org.apache.stormcrawler.protocol.Protocol;
import org.apache.stormcrawler.protocol.ProtocolFactory;
import org.apache.stormcrawler.protocol.ProtocolResponse;
import org.apache.stormcrawler.protocol.RobotRules;
import org.apache.stormcrawler.util.ConfUtils;
import org.apache.stormcrawler.util.PerSecondReducer;
import org.slf4j.LoggerFactory;

/**
 * A multithreaded, queue-based fetcher adapted from Apache Nutch. Enforces the politeness and
 * handles the fetching threads itself.
 */
public class FetcherBolt extends StatusEmitterBolt {

    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(FetcherBolt.class);

    private static final String SITEMAP_DISCOVERY_PARAM_KEY = "sitemap.discovery";

    /**
     * Acks URLs which have spent too much time in the queue, should be set to a value equals to the
     * topology timeout.
     */
    public static final String QUEUED_TIMEOUT_PARAM_KEY = "fetcher.timeout.queue";

    /** Key name of the custom crawl delay for a queue that may be present in the metadata. */
    private static final String CRAWL_DELAY_KEY_NAME = "crawl.delay";

    /**
     * Key name of the custom crawl delay for a queue that may be present in the metadata when
     * multi-threading is allowed for a queue.
     */
    private static final String CRAWL_MIN_DELAY_KEY_NAME = "crawl.min.delay";

    /** Key name of the custom max number of threads that may be present in the metadata. */
    private static final String CRAWL_MAX_THREAD_KEY_NAME = "max.threads.queue";

    private final AtomicInteger activeThreads = new AtomicInteger(0);
    private final AtomicInteger spinWaiting = new AtomicInteger(0);

    private FetchItemQueues fetchQueues;

    private MultiCountMetric eventCounter;
    private MultiReducedMetric averagedMetrics;

    private ProtocolFactory protocolFactory;

    private int taskId = -1;

    boolean sitemapsAutoDiscovery = false;

    private MultiReducedMetric perSecMetrics;

    private File debugfiletrigger;

    /** blocks the processing of new URLs if this value is reached. * */
    private int maxNumberUrlsInQueues = -1;

    private String[] beingFetched;

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Config conf = new Config();
        int tickFrequencyInSeconds = 5;
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, tickFrequencyInSeconds);
        return conf;
    }

    /** This class described the item to be fetched. */
    private static class FetchItem {

        String queueId;
        String url;
        Tuple tuple;
        long creationTime;

        private FetchItem(String url, Tuple t, String queueId) {
            this.url = url;
            this.queueId = queueId;
            this.tuple = t;
            this.creationTime = System.currentTimeMillis();
        }

        /**
         * Create an item. Queue id will be created based on <code>queueMode</code> argument, either
         * as a protocol + hostname pair, protocol + IP address pair or protocol+domain pair.
         */
        public static FetchItem create(URL u, String url, Tuple t, String queueMode) {

            String queueId;

            String key = null;
            // reuse any key that might have been given
            // be it the hostname, domain or IP
            if (t.contains("key")) {
                key = t.getStringByField("key");
            }
            if (StringUtils.isNotBlank(key)) {
                queueId = key.toLowerCase(Locale.ROOT);
                return new FetchItem(url, t, queueId);
            }

            if (FetchItemQueues.QUEUE_MODE_IP.equalsIgnoreCase(queueMode)) {
                try {
                    final InetAddress addr = InetAddress.getByName(u.getHost());
                    key = addr.getHostAddress();
                } catch (final UnknownHostException e) {
                    LOG.warn("Unable to resolve IP for {}, using hostname as key.", u.getHost());
                    key = u.getHost();
                }
            } else if (FetchItemQueues.QUEUE_MODE_DOMAIN.equalsIgnoreCase(queueMode)) {
                key = PaidLevelDomain.getPLD(u.getHost());
                if (key == null) {
                    LOG.warn("Unknown domain for url: {}, using hostname as key", url);
                    key = u.getHost();
                }
            } else {
                key = u.getHost();
            }

            if (key == null) {
                LOG.warn("Unknown host for url: {}, using URL string as key", url);
                key = u.toExternalForm();
            }

            queueId = key.toLowerCase(Locale.ROOT);
            return new FetchItem(url, t, queueId);
        }
    }

    /**
     * This class handles FetchItems which come from the same host ID (be it a proto/hostname or
     * proto/IP pair). It also keeps track of requests in progress and elapsed time between
     * requests.
     */
    private static class FetchItemQueue {
        final BlockingDeque<FetchItem> queue;

        private final AtomicInteger inProgress = new AtomicInteger();
        private final AtomicLong nextFetchTime = new AtomicLong();

        private long minCrawlDelay;
        private final int maxThreads;

        long crawlDelay;

        public FetchItemQueue(
                int maxThreads, long crawlDelay, long minCrawlDelay, int maxQueueSize) {
            this.maxThreads = maxThreads;
            this.crawlDelay = crawlDelay;
            this.minCrawlDelay = minCrawlDelay;
            this.queue = new LinkedBlockingDeque<>(maxQueueSize);
            // ready to start
            setNextFetchTime(System.currentTimeMillis(), true);
        }

        public int getQueueSize() {
            return queue.size();
        }

        public int getInProgressSize() {
            return inProgress.get();
        }

        public void finishFetchItem(FetchItem it, boolean asap) {
            if (it != null) {
                inProgress.decrementAndGet();
                setNextFetchTime(System.currentTimeMillis(), asap);
            }
        }

        public boolean addFetchItem(FetchItem it) {
            return queue.offer(it);
        }

        public FetchItem getFetchItem() {
            if (inProgress.get() >= maxThreads) {
                return null;
            }
            if (nextFetchTime.get() > System.currentTimeMillis()) {
                return null;
            }
            FetchItem it = queue.pollFirst();
            if (it != null) {
                inProgress.incrementAndGet();
            }
            return it;
        }

        private void setNextFetchTime(long endTime, boolean asap) {
            if (!asap) {
                nextFetchTime.set(endTime + (maxThreads > 1 ? minCrawlDelay : crawlDelay));
            } else {
                nextFetchTime.set(endTime);
            }
        }
    }

    /**
     * Convenience class - a collection of queues that keeps track of the total number of items, and
     * provides items eligible for fetching from any queue.
     */
    private static class FetchItemQueues {
        final Map<String, FetchItemQueue> queues =
                Collections.synchronizedMap(new LinkedHashMap<>());

        AtomicInteger inQueues = new AtomicInteger(0);

        final int defaultMaxThread;
        final long crawlDelay;
        final long minCrawlDelay;

        int maxQueueSize;

        final Config conf;

        public static final String QUEUE_MODE_HOST = "byHost";
        public static final String QUEUE_MODE_DOMAIN = "byDomain";
        public static final String QUEUE_MODE_IP = "byIP";

        String queueMode;

        final Map<Pattern, Integer> customMaxThreads = new HashMap<>();

        public FetchItemQueues(Config conf) {
            this.conf = conf;
            this.defaultMaxThread = ConfUtils.getInt(conf, "fetcher.threads.per.queue", 1);
            queueMode = ConfUtils.getString(conf, "fetcher.queue.mode", QUEUE_MODE_HOST);
            // check that the mode is known
            if (!queueMode.equals(QUEUE_MODE_IP)
                    && !queueMode.equals(QUEUE_MODE_DOMAIN)
                    && !queueMode.equals(QUEUE_MODE_HOST)) {
                LOG.error("Unknown partition mode : {} - forcing to byHost", queueMode);
                queueMode = QUEUE_MODE_HOST;
            }
            LOG.info("Using queue mode : {}", queueMode);

            this.crawlDelay =
                    (long) (ConfUtils.getFloat(conf, "fetcher.server.delay", 1.0f) * 1000);
            this.minCrawlDelay =
                    (long) (ConfUtils.getFloat(conf, "fetcher.server.min.delay", 0.0f) * 1000);
            this.maxQueueSize = ConfUtils.getInt(conf, "fetcher.max.queue.size", -1);
            if (this.maxQueueSize == -1) {
                this.maxQueueSize = Integer.MAX_VALUE;
            }

            // order is not guaranteed
            for (Entry<String, Object> e : conf.entrySet()) {
                String key = e.getKey();
                if (!key.startsWith("fetcher.maxThreads.")) {
                    continue;
                }
                Pattern patt = Pattern.compile(key.substring("fetcher.maxThreads.".length()));
                customMaxThreads.put(patt, ((Number) e.getValue()).intValue());
            }
        }

        /**
         * Adds item to the queue.
         *
         * @return true if the URL has been added, false otherwise.
         */
        public synchronized boolean addFetchItem(URL u, String url, Tuple input) {
            FetchItem it = FetchItem.create(u, url, input, queueMode);
            final Metadata metadata = (Metadata) input.getValueByField("metadata");
            FetchItemQueue fiq = getFetchItemQueue(it.queueId, metadata);
            boolean added = fiq.addFetchItem(it);
            if (added) {
                inQueues.incrementAndGet();
            }

            LOG.debug("{} added to queue {}", url, it.queueId);

            return added;
        }

        public synchronized void finishFetchItem(FetchItem it, boolean asap) {
            FetchItemQueue fiq = queues.get(it.queueId);
            if (fiq == null) {
                LOG.warn("Attempting to finish item from unknown queue: {}", it.queueId);
                return;
            }
            fiq.finishFetchItem(it, asap);
        }

        public synchronized FetchItemQueue getFetchItemQueue(String id, Metadata metadata) {
            FetchItemQueue fiq = queues.get(id);

            long delay = crawlDelay;
            long minDelay = minCrawlDelay;

            if (metadata != null) {
                // custom crawl delay from metadata?
                String v = metadata.getFirstValue(CRAWL_DELAY_KEY_NAME);
                if (v != null) {
                    delay = Long.parseLong(v);
                }
                // custom min crawl delay from metadata?
                v = metadata.getFirstValue(CRAWL_MIN_DELAY_KEY_NAME);
                if (v != null) {
                    minDelay = Long.parseLong(v);
                }
            }

            if (fiq == null) {
                int threadVal = defaultMaxThread;
                // custom maxThread value?
                for (Entry<Pattern, Integer> p : customMaxThreads.entrySet()) {
                    if (p.getKey().matcher(id).matches()) {
                        threadVal = p.getValue();
                        break;
                    }
                }

                // overridden at URL level
                // custom thread number from metadata?
                if (metadata != null) {
                    final String val = metadata.getFirstValue(CRAWL_MAX_THREAD_KEY_NAME);
                    if (val != null) {
                        threadVal = Integer.parseInt(val);
                    }
                }

                // initialize queue
                fiq = new FetchItemQueue(threadVal, delay, minDelay, maxQueueSize);
                queues.put(id, fiq);
            }

            // in cases where we have different pages with the same key that will fall in the same
            // queue, each one with a custom min crawl delay, we take the less aggressive
            if (fiq.minCrawlDelay < minDelay) {
                fiq.minCrawlDelay = minDelay;
            }
            // same for the normal delay
            if (fiq.crawlDelay < delay) {
                fiq.crawlDelay = delay;
            }
            return fiq;
        }

        public synchronized FetchItem getFetchItem() {
            if (queues.isEmpty()) {
                return null;
            }

            FetchItemQueue start = null;

            do {
                Iterator<Entry<String, FetchItemQueue>> i = queues.entrySet().iterator();

                if (!i.hasNext()) {
                    return null;
                }

                Map.Entry<String, FetchItemQueue> nextEntry = i.next();

                if (nextEntry == null) {
                    return null;
                }

                FetchItemQueue fiq = nextEntry.getValue();

                // We remove the entry and put it at the end of the map
                i.remove();

                // reap empty queues
                if (fiq.getQueueSize() == 0 && fiq.getInProgressSize() == 0) {
                    continue;
                }

                // Put the entry at the end no matter the result
                queues.put(nextEntry.getKey(), nextEntry.getValue());

                // In case of we are looping
                if (start == null) {
                    start = fiq;
                } else if (fiq == start) {
                    return null;
                }

                FetchItem fit = fiq.getFetchItem();

                if (fit != null) {
                    inQueues.decrementAndGet();
                    return fit;
                }

            } while (!queues.isEmpty());

            return null;
        }
    }

    /** This class picks items from queues and fetches the pages. */
    private class FetcherThread extends Thread {

        // max. delay accepted from robots.txt
        private final long maxCrawlDelay;
        // whether maxCrawlDelay overwrites the longer value in robots.txt
        // (otherwise URLs in this queue are skipped)
        private final boolean maxCrawlDelayForce;
        // whether the default delay is used even if the robots.txt
        // specifies a shorter crawl-delay
        private final boolean crawlDelayForce;
        private final int threadNum;

        private long timeoutInQueues = -1;

        // by default remains as is-pre 1.17
        private String protocolMetadataPrefix = "";

        public FetcherThread(Config conf, int num) {
            this.setDaemon(true); // don't hang JVM on exit
            this.setName("FetcherThread #" + num); // use an informative name

            this.maxCrawlDelay = ConfUtils.getInt(conf, "fetcher.max.crawl.delay", 30) * 1000L;
            this.maxCrawlDelayForce =
                    ConfUtils.getBoolean(conf, "fetcher.max.crawl.delay.force", false);
            this.crawlDelayForce = ConfUtils.getBoolean(conf, "fetcher.server.delay.force", false);
            this.threadNum = num;
            timeoutInQueues = ConfUtils.getLong(conf, QUEUED_TIMEOUT_PARAM_KEY, timeoutInQueues);
            protocolMetadataPrefix =
                    ConfUtils.getString(
                            conf,
                            ProtocolResponse.PROTOCOL_MD_PREFIX_PARAM,
                            protocolMetadataPrefix);
        }

        @Override
        public void run() {
            while (true) {
                FetchItem fit = fetchQueues.getFetchItem();
                if (fit == null) {
                    LOG.trace("{} spin-waiting ...", getName());
                    // spin-wait.
                    spinWaiting.incrementAndGet();
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        LOG.error("{} caught interrupted exception", getName());
                        Thread.currentThread().interrupt();
                    }
                    spinWaiting.decrementAndGet();
                    continue;
                }

                activeThreads.incrementAndGet(); // count threads

                beingFetched[threadNum] = fit.url;

                LOG.debug(
                        "[Fetcher #{}] {}  => activeThreads={}, spinWaiting={}, queueID={}",
                        taskId,
                        getName(),
                        activeThreads,
                        spinWaiting,
                        fit.queueId);

                LOG.debug("[Fetcher #{}] {} : Fetching {}", taskId, getName(), fit.url);

                Metadata metadata = null;

                if (fit.tuple.contains("metadata")) {
                    metadata = (Metadata) fit.tuple.getValueByField("metadata");
                }
                if (metadata == null) {
                    metadata = new Metadata();
                }

                // https://github.com/apache/stormcrawler/issues/813
                metadata.remove("fetch.exception");

                boolean asap = false;

                try {
                    URL url = new URL(fit.url);
                    Protocol protocol = protocolFactory.getProtocol(url);

                    if (protocol == null) {
                        throw new RuntimeException(
                                "No protocol implementation found for " + fit.url);
                    }

                    BaseRobotRules rules = protocol.getRobotRules(fit.url);
                    boolean fromCache = false;
                    if (rules instanceof RobotRules
                            && ((RobotRules) rules).getContentLengthFetched().length == 0) {
                        fromCache = true;
                        eventCounter.scope("robots.fromCache").incrBy(1);
                    } else {
                        eventCounter.scope("robots.fetched").incrBy(1);
                    }

                    // autodiscovery of sitemaps
                    // the sitemaps will be sent down the topology
                    // if the robot file did not come from the cache
                    // to avoid sending them unnecessarily

                    // check in the metadata if discovery setting has been
                    // overridden

                    String localSitemapDiscoveryVal =
                            metadata.getFirstValue(SITEMAP_DISCOVERY_PARAM_KEY);

                    boolean smautodisco;

                    if ("true".equalsIgnoreCase(localSitemapDiscoveryVal)) {
                        smautodisco = true;
                    } else if ("false".equalsIgnoreCase(localSitemapDiscoveryVal)) {
                        smautodisco = false;
                    } else {
                        smautodisco = sitemapsAutoDiscovery;
                    }

                    if (!fromCache && smautodisco) {
                        for (String sitemapUrl : rules.getSitemaps()) {
                            if (rules.isAllowed(sitemapUrl)) {
                                emitOutlink(
                                        fit.tuple,
                                        url,
                                        sitemapUrl,
                                        metadata,
                                        SiteMapParserBolt.isSitemapKey,
                                        "true");
                            }
                        }
                    }

                    // has found sitemaps
                    // https://github.com/apache/stormcrawler/issues/710
                    // note: we don't care if the sitemap URLs where actually
                    // kept
                    boolean foundSitemap = (rules.getSitemaps().size() > 0);
                    metadata.setValue(
                            SiteMapParserBolt.foundSitemapKey, Boolean.toString(foundSitemap));

                    if (!rules.isAllowed(fit.url)) {
                        LOG.info("Denied by robots.txt: {}", fit.url);
                        // pass the info about denied by robots
                        metadata.setValue(Constants.STATUS_ERROR_CAUSE, "robots.txt");
                        collector.emit(
                                org.apache.stormcrawler.Constants.StatusStreamName,
                                fit.tuple,
                                new Values(fit.url, metadata, Status.ERROR));
                        // no need to wait next time as we won't request from
                        // that site
                        asap = true;
                        continue;
                    }
                    FetchItemQueue fiq = fetchQueues.getFetchItemQueue(fit.queueId, metadata);
                    if (rules.getCrawlDelay() > 0 && rules.getCrawlDelay() != fiq.crawlDelay) {
                        if (rules.getCrawlDelay() > maxCrawlDelay && maxCrawlDelay >= 0) {
                            boolean force = false;
                            String msg = "skipping";
                            if (maxCrawlDelayForce) {
                                force = true;
                                msg = "using value of fetcher.max.crawl.delay instead";
                            }
                            LOG.info(
                                    "Crawl-Delay for {} too long ({}), {}",
                                    fit.url,
                                    rules.getCrawlDelay(),
                                    msg);
                            if (force) {
                                fiq.crawlDelay = maxCrawlDelay;
                            } else {
                                // pass the info about crawl delay
                                metadata.setValue(Constants.STATUS_ERROR_CAUSE, "crawl_delay");
                                collector.emit(
                                        org.apache.stormcrawler.Constants.StatusStreamName,
                                        fit.tuple,
                                        new Values(fit.url, metadata, Status.ERROR));
                                // no need to wait next time as we won't request
                                // from that site
                                asap = true;
                                continue;
                            }
                        } else if (rules.getCrawlDelay() < fetchQueues.crawlDelay
                                && crawlDelayForce) {
                            fiq.crawlDelay = fetchQueues.crawlDelay;
                            LOG.info(
                                    "Crawl delay for {} too short ({}), "
                                            + "set to fetcher.server.delay",
                                    fit.url,
                                    rules.getCrawlDelay());
                        } else {
                            fiq.crawlDelay = rules.getCrawlDelay();
                            LOG.info(
                                    "Crawl delay for queue: {}  is set to {} "
                                            + "as per robots.txt. url: {}",
                                    fit.queueId,
                                    fiq.crawlDelay,
                                    fit.url);
                        }
                    }

                    long start = System.currentTimeMillis();
                    long timeInQueues = start - fit.creationTime;

                    // been in the queue far too long and already failed
                    // by the timeout - let's not fetch it
                    if (timeoutInQueues != -1 && timeInQueues > timeoutInQueues * 1000) {
                        LOG.info(
                                "[Fetcher #{}] Waited in queue for too long - {}", taskId, fit.url);
                        // no need to wait next time as we won't request from
                        // that site
                        asap = true;
                        continue;
                    }

                    ProtocolResponse response = protocol.getProtocolOutput(fit.url, metadata);

                    long timeFetching = System.currentTimeMillis() - start;

                    final int byteLength = response.getContent().length;

                    // get any metrics from the protocol metadata
                    // expect Longs
                    response.getMetadata().keySet("metrics.").stream()
                            .forEach(
                                    s ->
                                            averagedMetrics
                                                    .scope(s.substring(8))
                                                    .update(
                                                            Long.parseLong(
                                                                    response.getMetadata()
                                                                            .getFirstValue(s))));

                    averagedMetrics.scope("fetch_time").update(timeFetching);
                    averagedMetrics.scope("time_in_queues").update(timeInQueues);
                    averagedMetrics.scope("bytes_fetched").update(byteLength);
                    perSecMetrics.scope("bytes_fetched_perSec").update(byteLength);
                    perSecMetrics.scope("fetched_perSec").update(1);
                    eventCounter.scope("fetched").incrBy(1);
                    eventCounter.scope("bytes_fetched").incrBy(byteLength);

                    LOG.info(
                            "[Fetcher #{}] Fetched {} with status {} in msec {}",
                            taskId,
                            fit.url,
                            response.getStatusCode(),
                            timeFetching);

                    // merges the original MD and the ones returned by the
                    // protocol
                    Metadata mergedMetadata = new Metadata();
                    mergedMetadata.putAll(metadata);

                    // add a prefix to avoid confusion, preserve protocol
                    // metadata persisted or transferred from previous fetches
                    mergedMetadata.putAll(response.getMetadata(), protocolMetadataPrefix);

                    mergedMetadata.setValue(
                            "fetch.statusCode", Integer.toString(response.getStatusCode()));

                    mergedMetadata.setValue("fetch.byteLength", Integer.toString(byteLength));

                    mergedMetadata.setValue("fetch.loadingTime", Long.toString(timeFetching));

                    mergedMetadata.setValue("fetch.timeInQueues", Long.toString(timeInQueues));

                    // determine the status based on the status code
                    final Status status = Status.fromHTTPCode(response.getStatusCode());

                    eventCounter.scope("status_" + response.getStatusCode()).incrBy(1);

                    final Values tupleToSend = new Values(fit.url, mergedMetadata, status);

                    // if the status is OK emit on default stream
                    if (status.equals(Status.FETCHED)) {
                        if (response.getStatusCode() == 304) {
                            // mark this URL as fetched so that it gets
                            // rescheduled
                            // but do not try to parse or index
                            collector.emit(Constants.StatusStreamName, fit.tuple, tupleToSend);
                        } else {
                            // send content for parsing
                            collector.emit(
                                    Utils.DEFAULT_STREAM_ID,
                                    fit.tuple,
                                    new Values(fit.url, response.getContent(), mergedMetadata));
                        }
                    } else if (status.equals(Status.REDIRECTION)) {

                        // find the URL it redirects to
                        String redirection =
                                response.getMetadata().getFirstValue(HttpHeaders.LOCATION);

                        // stores the URL it redirects to
                        // used for debugging mainly - do not resolve the target
                        // URL
                        if (StringUtils.isNotBlank(redirection)) {
                            mergedMetadata.setValue("_redirTo", redirection);
                        }

                        // https://github.com/apache/stormcrawler/issues/954
                        if (allowRedirs() && StringUtils.isNotBlank(redirection)) {
                            emitOutlink(fit.tuple, url, redirection, mergedMetadata);
                        }

                        // mark this URL as redirected
                        collector.emit(Constants.StatusStreamName, fit.tuple, tupleToSend);
                    } else {
                        // error
                        collector.emit(Constants.StatusStreamName, fit.tuple, tupleToSend);
                    }

                } catch (Exception exece) {
                    String message = exece.getMessage();
                    if (message == null) {
                        message = "";
                    }

                    // common exceptions for which we log only a short message
                    if (exece.getCause() instanceof java.util.concurrent.TimeoutException
                            || message.contains(" timed out")) {
                        LOG.info("Socket timeout fetching {}", fit.url);
                        message = "Socket timeout fetching";
                    } else if (exece.getCause() instanceof java.net.UnknownHostException
                            || exece instanceof java.net.UnknownHostException) {
                        LOG.info("Unknown host {}", fit.url);
                        message = "Unknown host";
                    } else {
                        message = exece.getClass().getName();
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Exception while fetching {}", fit.url, exece);
                        } else {
                            LOG.info("Exception while fetching {} -> {}", fit.url, message);
                        }
                    }

                    if (metadata.size() == 0) {
                        metadata = new Metadata();
                    }
                    // add the reason of the failure in the metadata
                    metadata.setValue("fetch.exception", message);

                    // send to status stream
                    collector.emit(
                            Constants.StatusStreamName,
                            fit.tuple,
                            new Values(fit.url, metadata, Status.FETCH_ERROR));

                    eventCounter.scope("exception").incrBy(1);
                } finally {
                    fetchQueues.finishFetchItem(fit, asap);
                    activeThreads.decrementAndGet(); // count threads
                    // ack it whatever happens
                    collector.ack(fit.tuple);
                    beingFetched[threadNum] = "";
                }
            }
        }
    }

    private void checkConfiguration(Config stormConf) {

        // ensure that a value has been set for the agent name and that that
        // agent name is the first value in the agents we advertise for robot
        // rules parsing
        String agentName = (String) stormConf.get("http.agent.name");
        if (agentName == null || agentName.trim().length() == 0) {
            String message = "Fetcher: No agents listed in 'http.agent.name'" + " property.";
            LOG.error(message);
            throw new IllegalArgumentException(message);
        }
    }

    @Override
    public void prepare(
            Map<String, Object> stormConf, TopologyContext context, OutputCollector collector) {

        super.prepare(stormConf, context, collector);

        Config conf = new Config();
        conf.putAll(stormConf);

        checkConfiguration(conf);

        LOG.info("[Fetcher #{}] : starting at {}", taskId, Instant.now());

        int metricsTimeBucketSecs = ConfUtils.getInt(conf, "fetcher.metrics.time.bucket.secs", 10);

        // Register a "MultiCountMetric" to count different events in this bolt
        // Storm will emit the counts every n seconds to a special bolt via a
        // system stream
        // The data can be accessed by registering a "MetricConsumer" in the
        // topology
        this.eventCounter =
                context.registerMetric(
                        "fetcher_counter", new MultiCountMetric(), metricsTimeBucketSecs);

        // create gauges
        context.registerMetric(
                "activethreads",
                () -> {
                    return activeThreads.get();
                },
                metricsTimeBucketSecs);

        context.registerMetric(
                "in_queues",
                () -> {
                    return fetchQueues.inQueues.get();
                },
                metricsTimeBucketSecs);

        context.registerMetric(
                "num_queues",
                () -> {
                    return fetchQueues.queues.size();
                },
                metricsTimeBucketSecs);

        this.averagedMetrics =
                context.registerMetric(
                        "fetcher_average_perdoc",
                        new MultiReducedMetric(new MeanReducer()),
                        metricsTimeBucketSecs);

        this.perSecMetrics =
                context.registerMetric(
                        "fetcher_average_persec",
                        new MultiReducedMetric(new PerSecondReducer()),
                        metricsTimeBucketSecs);

        protocolFactory = ProtocolFactory.getInstance(conf);

        this.fetchQueues = new FetchItemQueues(conf);

        this.taskId = context.getThisTaskId();

        int threadCount = ConfUtils.getInt(conf, "fetcher.threads.number", 10);
        int startDelay = ConfUtils.getInt(conf, "fetcher.threads.start.delay", 10);

        for (int i = 0; i < threadCount; i++) {
            if (startDelay > 0 && i > 0) {
                // short delay to avoid that DNS or other resources are temporarily
                // exhausted by all threads fetching simultaneously the first pages
                try {
                    Thread.sleep(startDelay);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            new FetcherThread(conf, i).start();
        }

        // keep track of the URLs in fetching
        beingFetched = new String[threadCount];
        Arrays.fill(beingFetched, "");

        sitemapsAutoDiscovery = ConfUtils.getBoolean(stormConf, SITEMAP_DISCOVERY_PARAM_KEY, false);

        maxNumberUrlsInQueues = ConfUtils.getInt(conf, "fetcher.max.urls.in.queues", -1);

        /*
         * If set to a valid path e.g. /tmp/fetcher-dump-{port} on a worker node, the content of the
         * queues will be dumped to the logs for debugging. The port number needs to match the one
         * used by the FetcherBolt instance.
         */
        String debugfiletriggerpattern =
                ConfUtils.getString(conf, "fetcherbolt.queue.debug.filepath");

        if (StringUtils.isNotBlank(debugfiletriggerpattern)) {
            debugfiletrigger =
                    new File(
                            debugfiletriggerpattern.replaceAll(
                                    "\\{port\\}", Integer.toString(context.getThisWorkerPort())));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        super.declareOutputFields(declarer);
        declarer.declare(new Fields("url", "content", "metadata"));
    }

    @Override
    public void cleanup() {
        protocolFactory.cleanup();
    }

    @Override
    public void execute(Tuple input) {

        if (TupleUtils.isTick(input)) {
            // detect whether there is a file indicating that we should
            // dump the content of the queues to the log
            if (debugfiletrigger != null && debugfiletrigger.exists()) {
                LOG.info("Found trigger file {}", debugfiletrigger);
                logQueuesContent();
                debugfiletrigger.delete();
            }
            return;
        }

        if (this.maxNumberUrlsInQueues != -1) {
            while (this.activeThreads.get() + this.fetchQueues.inQueues.get()
                    >= maxNumberUrlsInQueues) {
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    LOG.error("Interrupted exception caught in execute method");
                    Thread.currentThread().interrupt();
                }
                LOG.debug(
                        "[Fetcher #{}] Threads : {}\tqueues : {}\tin_queues : {}",
                        taskId,
                        this.activeThreads.get(),
                        this.fetchQueues.queues.size(),
                        this.fetchQueues.inQueues.get());
            }
        }

        final String urlString = input.getStringByField("url");
        if (StringUtils.isBlank(urlString)) {
            LOG.info("[Fetcher #{}] Missing value for field url in tuple {}", taskId, input);
            // ignore silently
            collector.ack(input);
            return;
        }

        LOG.debug("Received in Fetcher {}", urlString);

        URL url;

        try {
            url = new URL(urlString);
        } catch (MalformedURLException e) {
            LOG.error("{} is a malformed URL", urlString);

            Metadata metadata = (Metadata) input.getValueByField("metadata");
            if (metadata == null) {
                metadata = new Metadata();
            }
            // Report to status stream and ack
            metadata.setValue(Constants.STATUS_ERROR_CAUSE, "malformed URL");
            collector.emit(
                    org.apache.stormcrawler.Constants.StatusStreamName,
                    input,
                    new Values(urlString, metadata, Status.ERROR));
            collector.ack(input);
            return;
        }

        boolean added = fetchQueues.addFetchItem(url, urlString, input);
        if (!added) {
            collector.fail(input);
        }
    }

    private void logQueuesContent() {
        StringBuilder sb = new StringBuilder();
        synchronized (fetchQueues.queues) {
            sb.append("\nNum queues : ").append(fetchQueues.queues.size());
            for (Entry<String, FetchItemQueue> entry : fetchQueues.queues.entrySet()) {
                sb.append("\nQueue ID : ").append(entry.getKey());
                FetchItemQueue fiq = entry.getValue();
                sb.append("\t size : ").append(fiq.getQueueSize());
                sb.append("\t in progress : ").append(fiq.getInProgressSize());
                for (FetchItem fetchItem : fiq.queue) {
                    sb.append("\n\t").append(fetchItem.url);
                }
            }
            LOG.info("Dumping queue content {}", sb.toString());

            StringBuilder sb2 = new StringBuilder("\n");
            // dump the list of URLs being fetched
            for (int i = 0; i < beingFetched.length; i++) {
                if (beingFetched[i].length() > 0) {
                    sb2.append("\n\tThread #").append(i).append(": ").append(beingFetched[i]);
                }
            }
            LOG.info("URLs being fetched {}", sb2.toString());
        }
    }
}
