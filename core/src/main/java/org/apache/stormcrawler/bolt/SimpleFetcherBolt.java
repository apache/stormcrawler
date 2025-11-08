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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import crawlercommons.domains.PaidLevelDomain;
import crawlercommons.robots.BaseRobotRules;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHeaders;
import org.apache.storm.Config;
import org.apache.storm.metric.api.IMetric;
import org.apache.storm.metric.api.MeanReducer;
import org.apache.storm.metric.api.MultiCountMetric;
import org.apache.storm.metric.api.MultiReducedMetric;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
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
 * A simple fetcher with no internal queues. This bolt either enforces the delay set by the
 * configuration or robots.txt by either sleeping or resending the tuple to itself on the
 * THROTTLE_STREAM using Direct grouping.
 *
 * <pre>
 * .directGrouping("fetch", "throttle")
 * </pre>
 */
public class SimpleFetcherBolt extends StatusEmitterBolt {

    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(SimpleFetcherBolt.class);

    private static final String SITEMAP_DISCOVERY_PARAM_KEY = "sitemap.discovery";

    public static final String QUEUE_MODE_HOST = "byHost";
    public static final String QUEUE_MODE_DOMAIN = "byDomain";
    public static final String QUEUE_MODE_IP = "byIP";

    public static final String THROTTLE_STREAM = "throttle";

    private Config conf;

    private MultiCountMetric eventCounter;
    private MultiReducedMetric averagedMetrics;
    private MultiReducedMetric perSecMetrics;

    private ProtocolFactory protocolFactory;

    private int taskId = -1;

    boolean sitemapsAutoDiscovery = false;

    // TODO: configure the max time
    private Cache<String, Long> throttler =
            Caffeine.newBuilder().expireAfterAccess(30, TimeUnit.SECONDS).build();

    private String queueMode;

    /** default crawl delay in msec, can be overridden by robots directives. */
    private long crawlDelay = 1000;

    /** max value accepted from robots.txt. */
    private long maxCrawlDelay = 30000;

    /**
     * whether to enforce the configured max. delay, or to skip URLs from queues with overlong
     * crawl-delay
     */
    private boolean maxCrawlDelayForce = true;

    /** whether the default delay is used even if the robots.txt specifies a shorter crawl-delay. */
    private boolean crawlDelayForce = false;

    private final AtomicInteger activeThreads = new AtomicInteger(0);

    /**
     * Amount of time the bolt will sleep to enforce politeness, if the time needed to wait is above
     * it, the tuple is sent back to the Storm internal queue. Deactivate by default i.e. nothing is
     * sent back to the bolt via the throttle stream.
     */
    private long maxThrottleSleepMilliSec = Long.MAX_VALUE;

    // by default remains as is-pre 1.17
    private String protocolMetadataPrefix = "";

    private void checkConfiguration() {

        // ensure that a value has been set for the agent name and that that
        // agent name is the first value in the agents we advertise for robot
        // rules parsing
        String agentName = (String) getConf().get("http.agent.name");
        if (agentName == null || agentName.trim().length() == 0) {
            String message = "Fetcher: No agents listed in 'http.agent.name'" + " property.";
            LOG.error(message);
            throw new IllegalArgumentException(message);
        }
    }

    private Config getConf() {
        return this.conf;
    }

    @Override
    public void prepare(
            Map<String, Object> stormConf, TopologyContext context, OutputCollector collector) {
        super.prepare(stormConf, context, collector);
        this.conf = new Config();
        this.conf.putAll(stormConf);

        checkConfiguration();

        this.taskId = context.getThisTaskId();

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.ENGLISH);
        long start = System.currentTimeMillis();
        LOG.info("[Fetcher #{}] : starting at {}", taskId, sdf.format(start));

        // Register a "MultiCountMetric" to count different events in this bolt
        // Storm will emit the counts every n seconds to a special bolt via a
        // system stream
        // The data can be accessed by registering a "MetricConsumer" in the
        // topology

        int metricsTimeBucketSecs = ConfUtils.getInt(conf, "fetcher.metrics.time.bucket.secs", 10);

        this.eventCounter =
                context.registerMetric(
                        "fetcher_counter", new MultiCountMetric(), metricsTimeBucketSecs);

        this.averagedMetrics =
                context.registerMetric(
                        "fetcher_average",
                        new MultiReducedMetric(new MeanReducer()),
                        metricsTimeBucketSecs);

        this.perSecMetrics =
                context.registerMetric(
                        "fetcher_average_persec",
                        new MultiReducedMetric(new PerSecondReducer()),
                        metricsTimeBucketSecs);

        // create gauges
        context.registerMetric(
                "activethreads",
                new IMetric() {
                    @Override
                    public Object getValueAndReset() {
                        return activeThreads.get();
                    }
                },
                metricsTimeBucketSecs);

        context.registerMetric(
                "throttler_size",
                new IMetric() {
                    @Override
                    public Object getValueAndReset() {
                        return throttler.estimatedSize();
                    }
                },
                metricsTimeBucketSecs);

        protocolFactory = ProtocolFactory.getInstance(conf);

        sitemapsAutoDiscovery = ConfUtils.getBoolean(stormConf, SITEMAP_DISCOVERY_PARAM_KEY, false);

        queueMode = ConfUtils.getString(conf, "fetcher.queue.mode", QUEUE_MODE_HOST);
        // check that the mode is known
        if (!queueMode.equals(QUEUE_MODE_IP)
                && !queueMode.equals(QUEUE_MODE_DOMAIN)
                && !queueMode.equals(QUEUE_MODE_HOST)) {
            LOG.error("Unknown partition mode : {} - forcing to byHost", queueMode);
            queueMode = QUEUE_MODE_HOST;
        }
        LOG.info("Using queue mode : {}", queueMode);

        this.crawlDelay = (long) (ConfUtils.getFloat(conf, "fetcher.server.delay", 1.0f) * 1000);

        this.maxCrawlDelay = (long) ConfUtils.getInt(conf, "fetcher.max.crawl.delay", 30) * 1000;

        this.maxCrawlDelayForce =
                ConfUtils.getBoolean(conf, "fetcher.max.crawl.delay.force", false);
        this.crawlDelayForce = ConfUtils.getBoolean(conf, "fetcher.server.delay.force", false);

        this.maxThrottleSleepMilliSec = ConfUtils.getLong(conf, "fetcher.max.throttle.sleep", -1);

        this.protocolMetadataPrefix =
                ConfUtils.getString(
                        conf, ProtocolResponse.PROTOCOL_MD_PREFIX_PARAM, protocolMetadataPrefix);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        super.declareOutputFields(declarer);
        declarer.declare(new Fields("url", "content", "metadata"));
        declarer.declareStream(THROTTLE_STREAM, true, new Fields("url", "metadata"));
    }

    @Override
    public void cleanup() {
        protocolFactory.cleanup();
    }

    @Override
    public void execute(Tuple input) {

        String urlString = input.getStringByField("url");
        if (StringUtils.isBlank(urlString)) {
            LOG.info("[Fetcher #{}] Missing value for field url in tuple {}", taskId, input);
            // ignore silently
            collector.ack(input);
            return;
        }

        Metadata metadata = null;

        if (input.contains("metadata")) {
            metadata = (Metadata) input.getValueByField("metadata");
        }
        if (metadata == null) {
            metadata = new Metadata();
        }

        // https://github.com/apache/stormcrawler/issues/813
        metadata.remove("fetch.exception");

        URL url;

        try {
            url = new URL(urlString);
        } catch (MalformedURLException e) {
            LOG.error("{} is a malformed URL", urlString);
            // Report to status stream and ack
            metadata.setValue(Constants.STATUS_ERROR_CAUSE, "malformed URL");
            collector.emit(
                    org.apache.stormcrawler.Constants.StatusStreamName,
                    input,
                    new Values(urlString, metadata, Status.ERROR));
            collector.ack(input);
            return;
        }

        String key = getPolitenessKey(url);
        long delay = 0;

        try {
            activeThreads.incrementAndGet();

            Protocol protocol = protocolFactory.getProtocol(url);

            BaseRobotRules rules = protocol.getRobotRules(urlString);
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
            String localSitemapDiscoveryVal = metadata.getFirstValue(SITEMAP_DISCOVERY_PARAM_KEY);

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
                                input,
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
            metadata.setValue(SiteMapParserBolt.foundSitemapKey, Boolean.toString(foundSitemap));

            activeThreads.decrementAndGet();

            if (!rules.isAllowed(urlString)) {
                LOG.info("Denied by robots.txt: {}", urlString);

                metadata.setValue(Constants.STATUS_ERROR_CAUSE, "robots.txt");

                // Report to status stream and ack
                collector.emit(
                        org.apache.stormcrawler.Constants.StatusStreamName,
                        input,
                        new Values(urlString, metadata, Status.ERROR));
                collector.ack(input);
                return;
            }

            // check when we are allowed to process it
            long timeWaiting = 0;

            Long timeAllowed = throttler.getIfPresent(key);

            if (timeAllowed != null) {
                long now = System.currentTimeMillis();
                long timeToWait = timeAllowed - now;
                if (timeToWait > 0) {
                    // too long -> send it to the back of the internal queue
                    if (maxThrottleSleepMilliSec != -1 && timeToWait > maxThrottleSleepMilliSec) {
                        collector.emitDirect(
                                this.taskId,
                                THROTTLE_STREAM,
                                input,
                                new Values(urlString, metadata));
                        collector.ack(input);
                        LOG.debug("[Fetcher #{}] sent back to the queue {}", taskId, urlString);
                        eventCounter.scope("sentBackToQueue").incrBy(1);
                        return;
                    }
                    // not too much of a wait - sleep here
                    timeWaiting = timeToWait;
                    try {
                        Thread.sleep(timeToWait);
                    } catch (InterruptedException e) {
                        LOG.error("[Fetcher #{}] caught InterruptedException caught while waiting");
                        Thread.currentThread().interrupt();
                    }
                }
            }

            delay = this.crawlDelay;

            // get the delay from robots
            // value is negative when not set
            long robotsDelay = rules.getCrawlDelay();
            if (robotsDelay > 0) {
                if (robotsDelay > maxCrawlDelay) {
                    if (maxCrawlDelayForce) {
                        // cap the value to a maximum
                        // as some sites specify ridiculous values
                        LOG.debug("Delay from robots capped at {} for {}", robotsDelay, url);
                        delay = maxCrawlDelay;
                    } else {
                        LOG.debug(
                                "Skipped URL from queue with overlong crawl-delay ({}): {}",
                                robotsDelay,
                                url);
                        metadata.setValue(Constants.STATUS_ERROR_CAUSE, "crawl_delay");
                        collector.emit(
                                org.apache.stormcrawler.Constants.StatusStreamName,
                                input,
                                new Values(urlString, metadata, Status.ERROR));
                        collector.ack(input);
                        return;
                    }
                } else if (robotsDelay < crawlDelay && crawlDelayForce) {
                    LOG.debug(
                            "Crawl delay for {} too short ({}), set to fetcher.server.delay",
                            url,
                            robotsDelay);
                    delay = crawlDelay;
                } else {
                    delay = robotsDelay;
                }
            }

            LOG.debug("[Fetcher #{}] : Fetching {}", taskId, urlString);

            activeThreads.incrementAndGet();

            long start = System.currentTimeMillis();
            ProtocolResponse response = protocol.getProtocolOutput(urlString, metadata);
            long timeFetching = System.currentTimeMillis() - start;

            final int byteLength = response.getContent().length;

            // get any metrics from the protocol metadata
            response.getMetadata().keySet("metrics.").stream()
                    .forEach(
                            s ->
                                    averagedMetrics
                                            .scope(s.substring(8))
                                            .update(
                                                    Long.parseLong(
                                                            response.getMetadata()
                                                                    .getFirstValue(s))));

            averagedMetrics.scope("wait_time").update(timeWaiting);
            averagedMetrics.scope("fetch_time").update(timeFetching);
            averagedMetrics.scope("bytes_fetched").update(byteLength);
            eventCounter.scope("fetched").incrBy(1);
            eventCounter.scope("bytes_fetched").incrBy(byteLength);
            perSecMetrics.scope("bytes_fetched_perSec").update(byteLength);
            perSecMetrics.scope("fetched_perSec").update(1);

            LOG.info(
                    "[Fetcher #{}] Fetched {} with status {} in {} after waiting {}",
                    taskId,
                    urlString,
                    response.getStatusCode(),
                    timeFetching,
                    timeWaiting);

            Metadata mergedMetadata = new Metadata();
            mergedMetadata.putAll(metadata);

            // add a prefix to avoid confusion, preserve protocol metadata
            // persisted or transferred from previous fetches
            mergedMetadata.putAll(response.getMetadata(), protocolMetadataPrefix);

            mergedMetadata.setValue("fetch.statusCode", Integer.toString(response.getStatusCode()));

            mergedMetadata.setValue("fetch.loadingTime", Long.toString(timeFetching));

            mergedMetadata.setValue("fetch.byteLength", Integer.toString(byteLength));

            // determine the status based on the status code
            final Status status = Status.fromHTTPCode(response.getStatusCode());

            eventCounter.scope("status_" + response.getStatusCode()).incrBy(1);

            // used when sending to status stream
            final Values values4status = new Values(urlString, mergedMetadata, status);

            // if the status is OK emit on default stream
            if (status.equals(Status.FETCHED)) {
                if (response.getStatusCode() == 304) {
                    // mark this URL as fetched so that it gets
                    // rescheduled
                    // but do not try to parse or index
                    collector.emit(
                            org.apache.stormcrawler.Constants.StatusStreamName,
                            input,
                            values4status);
                } else {
                    collector.emit(
                            Utils.DEFAULT_STREAM_ID,
                            input,
                            new Values(urlString, response.getContent(), mergedMetadata));
                }
            } else if (status.equals(Status.REDIRECTION)) {

                // find the URL it redirects to
                String redirection = response.getMetadata().getFirstValue(HttpHeaders.LOCATION);

                // stores the URL it redirects to
                // used for debugging mainly - do not resolve the target
                // URL
                if (StringUtils.isNotBlank(redirection)) {
                    mergedMetadata.setValue("_redirTo", redirection);
                }

                if (allowRedirs() && StringUtils.isNotBlank(redirection)) {
                    emitOutlink(input, url, redirection, mergedMetadata);
                }
                // Mark URL as redirected
                collector.emit(
                        org.apache.stormcrawler.Constants.StatusStreamName, input, values4status);
            } else {
                // Error
                collector.emit(
                        org.apache.stormcrawler.Constants.StatusStreamName, input, values4status);
            }

        } catch (Exception exece) {

            String message = exece.getMessage();
            if (message == null) {
                message = "";
            }

            // common exceptions for which we log only a short message
            if (exece.getCause() instanceof java.util.concurrent.TimeoutException
                    || message.contains(" timed out")) {
                LOG.error("Socket timeout fetching {}", urlString);
                message = "Socket timeout fetching";
            } else if (exece.getCause() instanceof java.net.UnknownHostException
                    || exece instanceof java.net.UnknownHostException) {
                LOG.error("Unknown host {}", urlString);
                message = "Unknown host";
            } else {
                LOG.error("Exception while fetching {}", urlString, exece);
                message = exece.getClass().getName();
            }
            eventCounter.scope("exception").incrBy(1);

            // could be an empty, immutable Metadata
            if (metadata.size() == 0) {
                metadata = new Metadata();
            }

            // add the reason of the failure in the metadata
            metadata.setValue("fetch.exception", message);

            collector.emit(
                    org.apache.stormcrawler.Constants.StatusStreamName,
                    input,
                    new Values(urlString, metadata, Status.FETCH_ERROR));
        }
        activeThreads.decrementAndGet();

        // update the throttler
        throttler.put(key, System.currentTimeMillis() + delay);

        collector.ack(input);
    }

    private String getPolitenessKey(URL u) {
        String key;
        if (QUEUE_MODE_IP.equalsIgnoreCase(queueMode)) {
            try {
                final InetAddress addr = InetAddress.getByName(u.getHost());
                key = addr.getHostAddress();
            } catch (final UnknownHostException e) {
                // unable to resolve it, so don't fall back to host name
                LOG.warn("Unable to resolve: {}, skipping.", u.getHost());
                return null;
            }
        } else if (QUEUE_MODE_DOMAIN.equalsIgnoreCase(queueMode)) {
            key = PaidLevelDomain.getPLD(u.getHost());
            if (key == null) {
                LOG.warn("Unknown domain for url: {}, using hostname as key", u.toExternalForm());
                key = u.getHost();
            }
        } else {
            key = u.getHost();
            if (key == null) {
                LOG.warn("Unknown host for url: {}, using URL string as key", u.toExternalForm());
                key = u.toExternalForm();
            }
        }
        return key.toLowerCase(Locale.ROOT);
    }
}
