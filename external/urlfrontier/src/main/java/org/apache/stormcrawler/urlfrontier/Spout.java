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

package org.apache.stormcrawler.urlfrontier;

import static org.apache.stormcrawler.urlfrontier.Constants.DEPTH_LE_METRIC_NAME;
import static org.apache.stormcrawler.urlfrontier.Constants.DEPTH_METRIC_INF_SCOPE;
import static org.apache.stormcrawler.urlfrontier.Constants.DEPTH_METRIC_NAME;
import static org.apache.stormcrawler.urlfrontier.Constants.DEPTH_METRIC_UNKNOWN_SCOPE;
import static org.apache.stormcrawler.urlfrontier.Constants.URLFRONTIER_ADDRESS_KEY;
import static org.apache.stormcrawler.urlfrontier.Constants.URLFRONTIER_CRAWL_ID_KEY;
import static org.apache.stormcrawler.urlfrontier.Constants.URLFRONTIER_DEFAULT_HOST;
import static org.apache.stormcrawler.urlfrontier.Constants.URLFRONTIER_DEFAULT_PORT;
import static org.apache.stormcrawler.urlfrontier.Constants.URLFRONTIER_DELAY_REQUESTABLE_KEY;
import static org.apache.stormcrawler.urlfrontier.Constants.URLFRONTIER_DEPTH_METRIC_MAX_DEFAULT;
import static org.apache.stormcrawler.urlfrontier.Constants.URLFRONTIER_DEPTH_METRIC_MAX_KEY;
import static org.apache.stormcrawler.urlfrontier.Constants.URLFRONTIER_DEPTH_WINDOW_DEFAULT;
import static org.apache.stormcrawler.urlfrontier.Constants.URLFRONTIER_DEPTH_WINDOW_KEY;
import static org.apache.stormcrawler.urlfrontier.Constants.URLFRONTIER_HOST_KEY;
import static org.apache.stormcrawler.urlfrontier.Constants.URLFRONTIER_MAX_BUCKETS_KEY;
import static org.apache.stormcrawler.urlfrontier.Constants.URLFRONTIER_MAX_URLS_PER_BUCKET_DEFAULT;
import static org.apache.stormcrawler.urlfrontier.Constants.URLFRONTIER_MAX_URLS_PER_BUCKET_KEY;
import static org.apache.stormcrawler.urlfrontier.Constants.URLFRONTIER_PORT_KEY;

import crawlercommons.urlfrontier.URLFrontierGrpc;
import crawlercommons.urlfrontier.URLFrontierGrpc.URLFrontierStub;
import crawlercommons.urlfrontier.Urlfrontier.GetParams;
import crawlercommons.urlfrontier.Urlfrontier.URLInfo;
import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLongArray;
import org.apache.storm.Config;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.metrics.CrawlerMetrics;
import org.apache.stormcrawler.metrics.ScopedCounter;
import org.apache.stormcrawler.persistence.AbstractQueryingSpout;
import org.apache.stormcrawler.util.ConfUtils;
import org.apache.stormcrawler.util.MetadataTransfer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Spout extends AbstractQueryingSpout {

    private static final Logger LOG = LoggerFactory.getLogger(Spout.class);

    private ManagedChannel channel;

    private URLFrontierStub frontier;

    private int maxURLsPerBucket;

    private int maxBucketNum;

    private int delayRequestable;

    /** Number of URLs handed out by the frontier, per depth level. */
    private ScopedCounter depthCounter;

    /** Cumulative counters: scope X holds the number of URLs with a depth at most X. */
    private ScopedCounter depthLeCounter;

    /** Depths at or beyond this value are counted in a single "N+" scope. */
    private int depthMetricMax;

    /**
     * In-memory copy of the cumulative counts, indices 0 to depthMetricMax - 1 hold the number of
     * URLs with a depth at most the index, the last index the number of URLs with a valid depth.
     * Kept here because the metrics API is write-only.
     */
    private AtomicLongArray cumulativeDepthCounts;

    /** Same distribution restricted to the last urlfrontier.depth.window.secs. */
    private DepthWindow depthWindow;

    /** Globally set crawlID * */
    private String globalCrawlID = null;

    @Override
    public void open(
            Map<String, Object> stormConf,
            TopologyContext context,
            SpoutOutputCollector collector) {
        super.open(stormConf, context, collector);

        maxURLsPerBucket =
                ConfUtils.getInt(
                        stormConf,
                        URLFRONTIER_MAX_URLS_PER_BUCKET_KEY,
                        URLFRONTIER_MAX_URLS_PER_BUCKET_DEFAULT);

        maxBucketNum = ConfUtils.getInt(stormConf, URLFRONTIER_MAX_BUCKETS_KEY, 10);

        // initialise the delay requestable with the timeout for messages
        delayRequestable =
                ConfUtils.getInt(stormConf, Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS, delayRequestable);

        // then override with any user specified config
        delayRequestable =
                ConfUtils.getInt(stormConf, URLFRONTIER_DELAY_REQUESTABLE_KEY, delayRequestable);

        // host and port of URL Frontier(s)
        List<String> addresses = ConfUtils.loadListFromConf(URLFRONTIER_ADDRESS_KEY, stormConf);

        // Selected address
        String address;
        switch (addresses.size()) {
            case 0:
                LOG.debug("{} has no addresses.", URLFRONTIER_ADDRESS_KEY);
                address = null;
                break;
            case 1:
                LOG.debug(
                        "{} with a size of {} is used.", URLFRONTIER_ADDRESS_KEY, addresses.size());
                address = addresses.get(0);
                break;
            default:
                int totalTasks = context.getComponentTasks(context.getThisComponentId()).size();
                if (totalTasks != addresses.size()) {
                    String message =
                            "Not one task per frontier node. "
                                    + totalTasks
                                    + " vs "
                                    + addresses.size();
                    LOG.error(message);
                    throw new RuntimeException(message);
                }
                int nodeIndex = context.getThisTaskIndex();
                Collections.sort(addresses);
                address = addresses.get(nodeIndex);
        }

        if (address == null) {
            channel =
                    ManagedChannelUtil.createChannel(
                            ConfUtils.getString(
                                    stormConf, URLFRONTIER_HOST_KEY, URLFRONTIER_DEFAULT_HOST),
                            ConfUtils.getInt(
                                    stormConf, URLFRONTIER_PORT_KEY, URLFRONTIER_DEFAULT_PORT),
                            stormConf);
        } else {
            channel = ManagedChannelUtil.createChannel(address, stormConf);
        }

        frontier = URLFrontierGrpc.newStub(channel).withWaitForReady();
        LOG.debug("State of Channel: {}", channel.getState(false));

        globalCrawlID = ConfUtils.getString(stormConf, URLFRONTIER_CRAWL_ID_KEY);

        depthMetricMax =
                ConfUtils.getInt(
                        stormConf,
                        URLFRONTIER_DEPTH_METRIC_MAX_KEY,
                        URLFRONTIER_DEPTH_METRIC_MAX_DEFAULT);
        depthCounter = CrawlerMetrics.registerCounter(context, stormConf, DEPTH_METRIC_NAME, 10);
        depthLeCounter =
                CrawlerMetrics.registerCounter(context, stormConf, DEPTH_LE_METRIC_NAME, 10);
        cumulativeDepthCounts = new AtomicLongArray(depthMetricMax + 1);
        depthWindow =
                new DepthWindow(
                        depthMetricMax,
                        ConfUtils.getInt(
                                        stormConf,
                                        URLFRONTIER_DEPTH_WINDOW_KEY,
                                        URLFRONTIER_DEPTH_WINDOW_DEFAULT)
                                * 1000L,
                        System::currentTimeMillis);

        if (globalCrawlID != null) {
            LOG.info("Initialized URLFrontier Spout for crawlId {}", globalCrawlID);
        } else {
            LOG.info("Initialized URLFrontier Spout without crawlId");
        }
    }

    @Override
    protected void populateBuffer() {

        LOG.debug(
                "Populating buffer - max queues {} - max URLs per queues {}",
                maxBucketNum,
                maxURLsPerBucket);

        GetParams.Builder builder =
                GetParams.newBuilder()
                        .setMaxUrlsPerQueue(maxURLsPerBucket)
                        .setDelayRequestable(delayRequestable)
                        .setMaxQueues(maxBucketNum);

        if (globalCrawlID != null) {
            builder.setCrawlID(globalCrawlID);
        }

        GetParams request = builder.build();

        final AtomicInteger counter = new AtomicInteger();
        final long start = System.currentTimeMillis();

        StreamObserver<URLInfo> responseObserver =
                new StreamObserver<>() {

                    @Override
                    public void onNext(URLInfo item) {
                        Metadata m = new Metadata();
                        item.getMetadataMap()
                                .forEach(
                                        (k, v) -> {
                                            for (int index = 0;
                                                    index < v.getValuesCount();
                                                    index++) {
                                                m.addValue(k, v.getValues(index));
                                            }
                                        });
                        recordDepth(m);
                        buffer.add(item.getUrl(), m, item.getKey());
                        counter.addAndGet(1);
                    }

                    @Override
                    public void onError(Throwable t) {

                        if (t instanceof io.grpc.StatusRuntimeException) {
                            io.grpc.StatusRuntimeException e = (io.grpc.StatusRuntimeException) t;

                            StringBuilder sb =
                                    new StringBuilder("StatusRuntimeException: ")
                                            .append(e.getStatus().toString());

                            io.grpc.Metadata trailers = e.getTrailers();
                            if (trailers != null) {
                                sb.append(" ").append(trailers);
                            }
                            LOG.error(sb.toString(), e);
                        } else {
                            LOG.error("Exception caught: ", t);
                        }

                        markQueryReceivedNow();
                    }

                    @Override
                    public void onCompleted() {
                        final long end = System.currentTimeMillis();
                        LOG.debug(
                                "Got {} URLs from the frontier in {} msec",
                                counter.get(),
                                (end - start));
                        markQueryReceivedNow();
                    }
                };

        LOG.trace("isInquery set to true");
        isInQuery.set(true);

        frontier.getURLs(request, responseObserver);
    }

    /**
     * Counts a URL handed out by the frontier under the scope of its depth. URLs without a numeric,
     * non-negative depth are counted under {@link Constants#DEPTH_METRIC_UNKNOWN_SCOPE}; depths at
     * or beyond the configured maximum share the "N+" scope.
     */
    void recordDepth(Metadata metadata) {
        String value = metadata.getFirstValue(MetadataTransfer.depthKeyName);
        int depth = -1;
        if (value != null) {
            try {
                depth = Integer.parseInt(value);
            } catch (NumberFormatException e) {
                // counted as unknown
            }
        }
        if (depth < 0) {
            depthCounter.scope(DEPTH_METRIC_UNKNOWN_SCOPE).incr();
            return;
        }
        if (depth >= depthMetricMax) {
            depthCounter.scope(depthMetricMax + "+").incr();
        } else {
            depthCounter.scope(String.valueOf(depth)).incr();
        }
        // cumulative: every scope from the depth of the URL up to the cap, plus the total
        for (int i = depth; i < depthMetricMax; i++) {
            depthLeCounter.scope(String.valueOf(i)).incr();
            cumulativeDepthCounts.incrementAndGet(i);
        }
        depthLeCounter.scope(DEPTH_METRIC_INF_SCOPE).incr();
        cumulativeDepthCounts.incrementAndGet(depthMetricMax);
        depthWindow.record(depth);
    }

    /**
     * Same as {@link #probabilityDepthAtMost(int)} but restricted to the URLs handed out within the
     * last {@code urlfrontier.depth.window.secs}, so that it follows the current shape of the crawl
     * rather than its whole history.
     *
     * @return a value between 0 and 1, or {@link Double#NaN} when no URL with a valid depth was
     *     received within the window
     */
    public double windowProbabilityDepthAtMost(int depth) {
        return depthWindow.probabilityAtMost(depth);
    }

    /**
     * Probability that a URL handed out by the frontier since the spout was opened has a depth at
     * most {@code depth}, i.e. the cumulative distribution of the depth. URLs without a valid depth
     * are not taken into account. Constant time.
     *
     * @return a value between 0 and 1, or {@link Double#NaN} before any URL with a valid depth has
     *     been received
     */
    public double probabilityDepthAtMost(int depth) {
        long total = cumulativeDepthCounts.get(depthMetricMax);
        if (total == 0) {
            return Double.NaN;
        }
        if (depth >= depthMetricMax) {
            return 1.0;
        }
        if (depth < 0) {
            return 0.0;
        }
        return (double) cumulativeDepthCounts.get(depth) / total;
    }

    @Override
    public void ack(Object msgId) {
        LOG.debug("Ack for {}", msgId);
        super.ack(msgId);
    }

    @Override
    public void fail(Object msgId) {
        LOG.info("Fail for {}", msgId);
        super.fail(msgId);
    }

    @Override
    public void close() {
        super.close();

        if (!channel.isShutdown()) {
            LOG.info("Shutting down connection to URLFrontier service.");
            channel.shutdown();
        } else {
            LOG.warn(
                    "Tried to shutdown connection to URLFrontier service that was already shutdown.");
        }
    }
}
