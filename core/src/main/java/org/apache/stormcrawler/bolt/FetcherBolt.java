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

import crawlercommons.robots.BaseRobotRules;
import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.time.Instant;
import java.util.Arrays;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHeaders;
import org.apache.storm.Config;
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
import org.apache.stormcrawler.bolt.FetchItemQueues.FetchItemQueue;
import org.apache.stormcrawler.metrics.CrawlerMetrics;
import org.apache.stormcrawler.metrics.ScopedCounter;
import org.apache.stormcrawler.metrics.ScopedReducedMetric;
import org.apache.stormcrawler.persistence.Status;
import org.apache.stormcrawler.protocol.AbstractHttpProtocol;
import org.apache.stormcrawler.protocol.FetchTimeoutException;
import org.apache.stormcrawler.protocol.Protocol;
import org.apache.stormcrawler.protocol.ProtocolFactory;
import org.apache.stormcrawler.protocol.ProtocolResponse;
import org.apache.stormcrawler.util.ConfUtils;
import org.apache.stormcrawler.util.URLUtil;
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

    /**
     * @deprecated since 4.0, use {@link Constants#FETCH_TIMEOUT_PARAM_KEY}
     */
    @Deprecated(since = "4.0", forRemoval = true)
    public static final String FETCH_TIMEOUT_PARAM_KEY = Constants.FETCH_TIMEOUT_PARAM_KEY;

    private final AtomicInteger activeThreads = new AtomicInteger(0);
    private final AtomicInteger spinWaiting = new AtomicInteger(0);

    private FetchItemQueues fetchQueues;

    private ScopedCounter eventCounter;
    private ScopedReducedMetric averagedMetrics;

    private ProtocolFactory protocolFactory;

    private int taskId = -1;

    boolean sitemapsAutoDiscovery = false;

    private ScopedReducedMetric perSecMetrics;

    private File debugfiletrigger;

    /** blocks the processing of new URLs if this value is reached. * */
    private int maxNumberUrlsInQueues = -1;

    private String[] beingFetched;

    /** Runs protocol calls under fetcher.thread.timeout, see {@link FetchTimeoutHelpers}. */
    private FetchTimeoutHelpers fetchHelpers;

    private RobotRulesLookup robotsLookup;

    private CrawlDelayPolicy crawlDelayPolicy;

    /** Largest number of helper threads ever alive; for tests. */
    int helperPoolSize() {
        return fetchHelpers == null ? 0 : fetchHelpers.largestPoolSize();
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Config conf = new Config();
        int tickFrequencyInSeconds = 5;
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, tickFrequencyInSeconds);
        return conf;
    }

    /** This class picks items from queues and fetches the pages. */
    private class FetcherThread extends Thread {

        private final int threadNum;

        private long timeoutInQueues = -1;

        // by default remains as is-pre 1.17
        private String protocolMetadataPrefix = "";

        public FetcherThread(Config conf, int num) {
            this.setDaemon(true); // don't hang JVM on exit
            this.setName("FetcherThread #" + num); // use an informative name

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
                metadata.remove(Constants.ROBOTS_CRAWL_DELAY_KEY);

                String robotsCrawlDelaySecs = null;

                boolean asap = false;
                // the fetch never ran because every helper thread was busy
                boolean saturated = false;

                try {
                    URL url = URLUtil.toURL(fit.url);
                    Protocol protocol = protocolFactory.getProtocol(url);

                    if (protocol == null) {
                        throw new RuntimeException(
                                "No protocol implementation found for " + fit.url);
                    }

                    RobotRulesLookup.Result robots =
                            robotsLookup.lookup(protocol, fit.url, metadata);
                    BaseRobotRules rules = robots.rules();

                    discoverSitemaps(fit.tuple, url, metadata, robots);

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
                    CrawlDelayPolicy.Decision decision =
                            crawlDelayPolicy.decide(
                                    fit.url,
                                    fit.queueId,
                                    rules.getCrawlDelay(),
                                    fiq.crawlDelay.get());
                    if (decision.action() == CrawlDelayPolicy.Action.SKIP) {
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
                    if (decision.action() == CrawlDelayPolicy.Action.APPLY) {
                        fiq.crawlDelay.set(decision.delay());
                        robotsCrawlDelaySecs = decision.robotsCrawlDelaySecs();
                        if (robotsCrawlDelaySecs != null) {
                            metadata.setValue(
                                    Constants.ROBOTS_CRAWL_DELAY_KEY, robotsCrawlDelaySecs);
                        }
                    }

                    long start = System.currentTimeMillis();
                    long timeInQueues = start - fit.creationTime;

                    // waited longer than fetcher.timeout.queue: not fetched, acked without a
                    // status (see finally)
                    if (timeoutInQueues != -1 && timeInQueues > timeoutInQueues * 1000) {
                        LOG.info(
                                "[Fetcher #{}] Waited in queue for too long - {}", taskId, fit.url);
                        eventCounter.scope("queue.timeout").incrBy(1);
                        // no need to wait next time as we won't request from
                        // that site
                        asap = true;
                        continue;
                    }

                    final Metadata fetchMetadata = metadata;
                    ProtocolResponse response;
                    response =
                            fetchHelpers.call(
                                    () -> protocol.getProtocolOutput(fit.url, fetchMetadata),
                                    protocol,
                                    fit.url,
                                    fetchMetadata);

                    long timeFetching = System.currentTimeMillis() - start;

                    final int byteLength = response.getContent().length;

                    // get any metrics from the protocol metadata
                    ProtocolMetrics.update(averagedMetrics, response.getMetadata());

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

                    // Only the locally parsed robots.txt value may populate this control signal.
                    // A colliding protocol prefix/header must not pace an unrelated queue.
                    mergedMetadata.remove(Constants.ROBOTS_CRAWL_DELAY_KEY);

                    // Request shaping comes from the configuration, never from a fetched page: a
                    // response header named set-header lands on exactly the key the protocols read
                    // to add headers to an outgoing request. Any value configured for this URL is
                    // put back after the response metadata has been dropped.
                    final String setHeaderKey =
                            protocolMetadataPrefix + AbstractHttpProtocol.SET_HEADER_BY_REQUEST;
                    mergedMetadata.remove(setHeaderKey);
                    mergedMetadata.setValues(setHeaderKey, metadata.getValues(setHeaderKey));
                    if (robotsCrawlDelaySecs != null) {
                        mergedMetadata.setValue(
                                Constants.ROBOTS_CRAWL_DELAY_KEY, robotsCrawlDelaySecs);
                    }

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
                            // a sitemap which redirects (e.g. /sitemap.xml to
                            // /sitemap_index.xml) must stay a sitemap: the key
                            // is persisted, not transferred to outlinks by
                            // default, so carry it onto the redirect target
                            if (Boolean.parseBoolean(
                                    mergedMetadata.getFirstValue(SiteMapParserBolt.isSitemapKey))) {
                                emitOutlink(
                                        fit.tuple,
                                        url,
                                        redirection,
                                        mergedMetadata,
                                        SiteMapParserBolt.isSitemapKey,
                                        "true");
                            } else {
                                emitOutlink(fit.tuple, url, redirection, mergedMetadata);
                            }
                        }

                        // mark this URL as redirected
                        collector.emit(Constants.StatusStreamName, fit.tuple, tupleToSend);
                    } else {
                        // error
                        collector.emit(Constants.StatusStreamName, fit.tuple, tupleToSend);
                    }

                } catch (FetchTimeoutHelpers.SaturatedException e) {
                    // the URL never reached the network: like a URL which waited too long in
                    // the queue, it is acked without a status so that the spout retries it
                    // later, rather than taking a strike towards max.fetch.errors. The queue
                    // is backed off rather than made ready at once (see finally): the helpers
                    // are shared and retrying immediately would only drain the queue into
                    // more rejections
                    eventCounter.scope("fetch.helper.rejected").incrBy(1);
                    LOG.warn(
                            "[Fetcher #{}] {}: all {} fetch helpers are busy",
                            taskId,
                            e.getMessage(),
                            fetchHelpers.maxHelpers());
                    saturated = true;
                } catch (Exception exece) {
                    String message = exece.getMessage();
                    if (message == null) {
                        message = "";
                    }

                    // common exceptions for which we log only a short message
                    if (exece instanceof java.io.InterruptedIOException
                            || message.contains(" timed out")) {
                        LOG.info("Socket timeout fetching {}", fit.url);
                        message = "Socket timeout fetching";
                        eventCounter.scope("fetch.timeout").incrBy(1);
                        if (exece instanceof FetchTimeoutException) {
                            // the hard deadline, as opposed to the protocol's socket timeouts
                            eventCounter.scope("fetch.deadline").incrBy(1);
                        }
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
                    if (saturated) {
                        long delay = fetchQueues.backOffFetchItem(fit);
                        LOG.debug(
                                "[Fetcher #{}] queue {} backed off for {} ms",
                                taskId,
                                fit.queueId,
                                delay);
                    } else {
                        fetchQueues.finishFetchItem(fit, asap);
                    }
                    activeThreads.decrementAndGet(); // count threads
                    // ack it whatever happens
                    collector.ack(fit.tuple);
                    beingFetched[threadNum] = "";
                }
            }
        }
    }

    /**
     * Sends the sitemaps of robots.txt which its rules allow as outlinks, when sitemap discovery is
     * on for the URL (its metadata overrides the configuration) and the rules did not come from the
     * cache. Sets foundSitemap in the metadata to whether robots.txt declares any sitemap, sent or
     * not (#710).
     */
    private void discoverSitemaps(
            Tuple tuple, URL url, Metadata metadata, RobotRulesLookup.Result robots) {
        BaseRobotRules rules = robots.rules();

        String localSitemapDiscoveryVal = metadata.getFirstValue(SITEMAP_DISCOVERY_PARAM_KEY);

        boolean smautodisco;

        if ("true".equalsIgnoreCase(localSitemapDiscoveryVal)) {
            smautodisco = true;
        } else if ("false".equalsIgnoreCase(localSitemapDiscoveryVal)) {
            smautodisco = false;
        } else {
            smautodisco = sitemapsAutoDiscovery;
        }

        if (!robots.fromCache() && smautodisco) {
            for (String sitemapUrl : rules.getSitemaps()) {
                if (rules.isAllowed(sitemapUrl)) {
                    emitOutlink(
                            tuple,
                            url,
                            sitemapUrl,
                            metadata,
                            SiteMapParserBolt.isSitemapKey,
                            "true");
                }
            }
        }

        boolean foundSitemap = (rules.getSitemaps().size() > 0);
        metadata.setValue(SiteMapParserBolt.foundSitemapKey, Boolean.toString(foundSitemap));
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
                CrawlerMetrics.registerCounter(
                        context, stormConf, "fetcher_counter", metricsTimeBucketSecs);

        // create gauges
        CrawlerMetrics.registerGauge(
                context, stormConf, "activethreads", activeThreads::get, metricsTimeBucketSecs);

        CrawlerMetrics.registerGauge(
                context,
                stormConf,
                "in_queues",
                () -> fetchQueues.inQueues.get(),
                metricsTimeBucketSecs);

        CrawlerMetrics.registerGauge(
                context,
                stormConf,
                "num_queues",
                () -> fetchQueues.queues.size(),
                metricsTimeBucketSecs);

        this.averagedMetrics =
                CrawlerMetrics.registerMeanMetric(
                        context, stormConf, "fetcher_average_perdoc", metricsTimeBucketSecs);

        this.perSecMetrics =
                CrawlerMetrics.registerPerSecMetric(
                        context, stormConf, "fetcher_average_persec", metricsTimeBucketSecs);

        protocolFactory = ProtocolFactory.getInstance(conf);

        this.fetchQueues = new FetchItemQueues(conf);

        crawlDelayPolicy =
                new CrawlDelayPolicy(
                        ConfUtils.getInt(conf, "fetcher.max.crawl.delay", 30) * 1000L,
                        ConfUtils.getBoolean(conf, "fetcher.max.crawl.delay.force", false),
                        fetchQueues.crawlDelay,
                        ConfUtils.getBoolean(conf, "fetcher.server.delay.force", false));

        this.taskId = context.getThisTaskId();

        int threadCount = ConfUtils.getInt(conf, "fetcher.threads.number", 10);
        int startDelay = ConfUtils.getInt(conf, "fetcher.threads.start.delay", 10);

        // helpers for protocols which can not cancel a fetch themselves; no thread until needed
        fetchHelpers =
                new FetchTimeoutHelpers(conf, Math.max(1, threadCount * 2), "FetcherTimeout-");
        fetchHelpers.registerMetrics(context, stormConf, metricsTimeBucketSecs);
        robotsLookup = new RobotRulesLookup(fetchHelpers, eventCounter, taskId);

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
        super.cleanup();
        if (fetchHelpers != null) {
            fetchHelpers.shutdown();
        }
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
            url = URLUtil.toURL(urlString);
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

    /**
     * Logs the content of the queues and the URLs being fetched. Not synchronized: the queues and
     * their items are iterated with weakly consistent iterators while the fetcher threads keep
     * working, so the dump is a smear over the time it takes to produce it rather than a
     * point-in-time snapshot. Sizes and item lists may not add up exactly.
     */
    private void logQueuesContent() {
        StringBuilder sb = new StringBuilder();
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
