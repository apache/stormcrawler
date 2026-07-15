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

import crawlercommons.urlfrontier.CrawlID;
import crawlercommons.urlfrontier.URLFrontierGrpc;
import crawlercommons.urlfrontier.URLFrontierGrpc.URLFrontierStub;
import crawlercommons.urlfrontier.Urlfrontier.BlockQueueParams;
import crawlercommons.urlfrontier.Urlfrontier.Empty;
import crawlercommons.urlfrontier.Urlfrontier.QueueDelayParams;
import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.util.ConfUtils;
import org.apache.stormcrawler.util.MetadataTransfer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Regulates a URLFrontier queue from the {@code queue} stream emitted by the status updater: blocks
 * it via {@code blockQueueUntil} when metadata reports a rate-limit response, and paces it via
 * {@code setDelay} when the fetcher reports a long robots.txt Crawl-delay. See issues #867, #784,
 * #1106 and #1979.
 *
 * <p>Two cases are handled, both gated on the status codes configured with {@code
 * urlfrontier.backoff.status.codes} (default 429 and 503):
 *
 * <ul>
 *   <li>the response carries a usable <a
 *       href="https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Retry-After">Retry-After</a>
 *       header: the requested delay is honoured, capped by {@code urlfrontier.max.retry.after} (in
 *       seconds, default 86400, -1 to disable the cap);
 *   <li>the response carries no usable header — the common case in the wild: the host is blocked
 *       for a growing duration, starting at {@code urlfrontier.backoff.base.secs} (default 60) and
 *       multiplied by {@code urlfrontier.backoff.factor} (default 2) on each new incident, capped
 *       at {@code urlfrontier.backoff.max.secs} (default 86400). A host that stays quiet for {@code
 *       urlfrontier.backoff.decay.secs} (default 1800) after its block has expired is forgiven and
 *       restarts from the base. Failures of URLs already in flight when a block was raised count as
 *       the same incident, not as new ones. With {@code urlfrontier.backoff.on.exceptions} (default
 *       false) fetch exceptions also count as incidents.
 * </ul>
 *
 * <p>A random fraction ({@code urlfrontier.backoff.jitter}, default 0.1) is added to every computed
 * block so that hosts blocked on the same schedule do not all retry at once, and a block is only
 * ever extended, never shrunk by a later, shorter signal.
 *
 * <p>Independently of rate limits, when robots pacing is explicitly enabled and the tuple's
 * metadata carries {@code robots.crawl.delay} (written by the fetcher bolts when a robots.txt
 * {@code Crawl-delay} exceeds {@code fetcher.max.crawl.delay}), the bolt forwards it to the
 * frontier via {@code setDelay}, capped by {@code urlfrontier.backoff.max.secs}. The value is
 * declarative: the last one seen wins, re-sent at most once per {@code
 * urlfrontier.backoff.decay.secs} window. If a site later removes its Crawl-delay, no signal
 * arrives and the frontier keeps the old pace: throughput loss on that host, never impoliteness;
 * {@code setDelay(key, 0)} is deliberately never called as it would erase a server-side default for
 * the queue.
 *
 * <p>Wire it with a fields grouping on {@code "key"} from the status updater's {@code queue}
 * stream: the per-host state lives in the bolt task, so all signals for a host must reach the same
 * task. The {@code "key"} is the frontier queue key derived from {@code partition.url.mode} (the
 * same setting the frontier uses to assign queues), so the block targets the matching queue.
 *
 * <p><b>The metadata on the queue stream is filtered by {@code metadata.persist}</b>: with the
 * default configuration neither {@code fetch.statusCode} nor the Retry-After header survive the
 * filter and this bolt never blocks anything. Both keys must be added to {@code metadata.persist}:
 *
 * <pre>
 *   metadata.persist:
 *    - fetch.statusCode
 *    - protocol.retry-after
 * </pre>
 *
 * (the second entry depends on {@code protocol.md.prefix}), and {@code fetch.exception} as well
 * when the back-off on exceptions is enabled. A warning is logged at startup when the configuration
 * would strip them.
 *
 * <p>Robots pacing is enabled by {@code urlfrontier.robots.crawl.delay.enabled=true} together with
 * {@code fetcher.max.crawl.delay.force=true}. It is safe only with {@code
 * partition.url.mode=byHost}, {@code urlfrontier.max.urls.per.bucket=1}, and {@code
 * robots.crawl.delay} in {@code metadata.persist} but <b>not</b> in {@code metadata.transfer}
 * (including wildcards such as {@code robots.*}). The bolt probes these conditions at startup and
 * fails fast rather than silently pacing a shared queue, transferring a host's delay to an outlink,
 * or handing out several URLs from that host in one request. A custom {@code
 * metadata.transfer.class} remains responsible for satisfying the same persist-only contract for
 * all URLs and values.
 *
 * <p>Neither blocking nor pacing recalls URLs already prefetched by the spout. For rate-limit
 * blocks, keep {@code urlfrontier.max.urls.per.bucket} and {@code topology.max.spout.pending} small
 * for the block to bite quickly. Robots pacing requires the stricter per-queue batch size of one;
 * this bounds each frontier hand-out but cannot recall earlier hand-outs or eliminate concurrent
 * requests during the transition to the new delay.
 *
 * <p>Connects via {@code urlfrontier.address}, falling back to {@code urlfrontier.host} / {@code
 * urlfrontier.port}. The block is issued with {@code local=false} and propagates to the whole
 * cluster, so with several frontier nodes a single connection to any one of them is enough.
 *
 * <p>The RPCs have a short deadline: a failed or expired call is logged but the tuple is acked
 * anyway. A missed block heals itself on further rate-limit signals. Robots delay calls are
 * serialized per host and coalesce to the latest value; an unchanged delay is re-sent after the
 * configured decay window.
 */
public class QueueRegulatorBolt extends BaseRichBolt {

    private static final Logger LOG = LoggerFactory.getLogger(QueueRegulatorBolt.class);

    /**
     * Deadline for the fire-and-forget blockQueueUntil call, so a frontier outage cannot pile up
     * pending RPCs and flush stale blocks on reconnect.
     */
    private static final long BLOCK_RPC_DEADLINE_SECS = 10;

    private static final StreamObserver<Empty> NOOP_OBSERVER =
            new StreamObserver<>() {
                @Override
                public void onNext(Empty value) {}

                @Override
                public void onError(Throwable t) {
                    LOG.warn("blockQueueUntil failed", t);
                }

                @Override
                public void onCompleted() {}
            };

    private OutputCollector collector;
    private ManagedChannel channel;
    private URLFrontierStub frontier;
    private String globalCrawlID;

    /** Per-host back-off state and decision logic. */
    private HostBackoff backoff;

    /** Per-host robots crawl-delay forwarding decision. */
    private CrawlDelayPolicy crawlDelayPolicy;

    /** Prevents terminal callbacks from chaining another RPC after cleanup starts. */
    private volatile boolean closed;

    @Override
    public void prepare(Map<String, Object> conf, TopologyContext context, OutputCollector c) {
        this.closed = false;
        this.collector = c;
        this.globalCrawlID =
                ConfUtils.getString(conf, Constants.URLFRONTIER_CRAWL_ID_KEY, CrawlID.DEFAULT);
        this.backoff = new HostBackoff(conf);
        validateRobotsPacingConfiguration(conf);
        this.crawlDelayPolicy = new CrawlDelayPolicy(conf);
        // the status updater emits the queue stream after the metadata.persist
        // filter: warn upfront when the keys this bolt relies on would be
        // stripped, as the bolt would otherwise silently never block anything
        Set<String> missing =
                missingQueueStreamKeys(conf, backoff.retryAfterKey(), backoff.onExceptions());
        if (!missing.isEmpty()) {
            LOG.warn(
                    "{} do(es) not survive the metadata.persist filter: the queue stream will"
                            + " never carry them and this bolt will never block a queue. Add the"
                            + " missing entries to metadata.persist.",
                    missing);
        }
        // a single connection is enough even with several frontier nodes, as
        // the block is issued with local=false and propagates cluster-wide
        List<String> addresses =
                ConfUtils.loadListFromConf(Constants.URLFRONTIER_ADDRESS_KEY, conf);
        String address;
        if (addresses.isEmpty()) {
            address =
                    ConfUtils.getString(
                                    conf,
                                    Constants.URLFRONTIER_HOST_KEY,
                                    Constants.URLFRONTIER_DEFAULT_HOST)
                            + ":"
                            + ConfUtils.getInt(
                                    conf,
                                    Constants.URLFRONTIER_PORT_KEY,
                                    Constants.URLFRONTIER_DEFAULT_PORT);
        } else {
            Collections.sort(addresses);
            address = addresses.get(context.getThisTaskIndex() % addresses.size());
        }
        this.channel = ManagedChannelUtil.createChannel(address);
        this.frontier = URLFrontierGrpc.newStub(channel).withWaitForReady();
    }

    @Override
    public void execute(Tuple t) {
        final String key = t.getStringByField("key");
        final Metadata metadata = (Metadata) t.getValueByField("metadata");
        final long blockUntil = backoff.blockUntilFor(key, metadata, System.currentTimeMillis());
        if (blockUntil > 0) {
            LOG.debug("Blocking queue {} until {}", key, blockUntil);
            BlockQueueParams params =
                    BlockQueueParams.newBuilder()
                            .setKey(key)
                            .setCrawlID(globalCrawlID)
                            .setTime(blockUntil)
                            .setLocal(false)
                            .build();
            frontier.withDeadlineAfter(BLOCK_RPC_DEADLINE_SECS, TimeUnit.SECONDS)
                    .blockQueueUntil(params, NOOP_OBSERVER);
        }
        crawlDelayPolicy.requestFor(key, metadata).ifPresent(this::setDelay);
        collector.ack(t);
    }

    private void setDelay(CrawlDelayPolicy.DelayRequest request) {
        if (closed) {
            return;
        }
        LOG.debug("Setting delay of {}s on queue {}", request.delaySecs(), request.key());
        QueueDelayParams delayParams =
                QueueDelayParams.newBuilder()
                        .setKey(request.key())
                        .setCrawlID(globalCrawlID)
                        .setDelayRequestable(request.delaySecs())
                        .setLocal(false)
                        .build();
        try {
            frontier.withDeadlineAfter(BLOCK_RPC_DEADLINE_SECS, TimeUnit.SECONDS)
                    .setDelay(delayParams, setDelayObserver(request));
        } catch (RuntimeException e) {
            LOG.warn("setDelay failed", e);
            crawlDelayPolicy.completed(request, false).ifPresent(this::setDelay);
        }
    }

    private StreamObserver<Empty> setDelayObserver(CrawlDelayPolicy.DelayRequest request) {
        return new StreamObserver<>() {
            @Override
            public void onNext(Empty value) {}

            @Override
            public void onError(Throwable t) {
                LOG.warn("setDelay failed", t);
                crawlDelayPolicy
                        .completed(request, false)
                        .ifPresent(QueueRegulatorBolt.this::setDelay);
            }

            @Override
            public void onCompleted() {
                crawlDelayPolicy
                        .completed(request, true)
                        .ifPresent(QueueRegulatorBolt.this::setDelay);
            }
        };
    }

    /**
     * Returns the queue-stream keys this bolt relies on that would not survive the {@code
     * metadata.persist} filter applied by the status updater before emitting. The {@code
     * fetch.exception} key only matters when the back-off on exceptions is enabled.
     */
    static Set<String> missingQueueStreamKeys(
            Map<String, Object> conf, String retryAfterKey, boolean onExceptions) {
        Metadata probe = new Metadata();
        probe.setValue(HostBackoff.STATUS_CODE_KEY, "429");
        probe.setValue(retryAfterKey, "1");
        probe.setValue(HostBackoff.EXCEPTION_KEY, "probe");
        Metadata filtered = MetadataTransfer.getInstance(conf).filter(probe);
        Set<String> missing = new LinkedHashSet<>();
        if (filtered.getFirstValue(HostBackoff.STATUS_CODE_KEY) == null) {
            missing.add(HostBackoff.STATUS_CODE_KEY);
        }
        if (filtered.getFirstValue(retryAfterKey) == null) {
            missing.add(retryAfterKey);
        }
        if (onExceptions && filtered.getFirstValue(HostBackoff.EXCEPTION_KEY) == null) {
            missing.add(HostBackoff.EXCEPTION_KEY);
        }
        return missing;
    }

    /**
     * Fails fast when robots pacing is enabled but the queue key, hand-out size, delay cap or
     * metadata routing could apply an unsafe pace or target the wrong host.
     */
    static void validateRobotsPacingConfiguration(Map<String, Object> conf) {
        if (!ConfUtils.getBoolean(
                conf,
                Constants.URLFRONTIER_ROBOTS_CRAWL_DELAY_ENABLED_KEY,
                Constants.URLFRONTIER_ROBOTS_CRAWL_DELAY_ENABLED_DEFAULT)) {
            return;
        }

        if (!ConfUtils.getBoolean(conf, "fetcher.max.crawl.delay.force", false)) {
            throw new IllegalArgumentException(
                    "robots crawl-delay pacing requires fetcher.max.crawl.delay.force=true");
        }

        int maxCrawlDelay = ConfUtils.getInt(conf, "fetcher.max.crawl.delay", 30);
        if (maxCrawlDelay < 0) {
            throw new IllegalArgumentException(
                    "robots crawl-delay pacing requires fetcher.max.crawl.delay >= 0");
        }

        String partitionMode =
                ConfUtils.getString(
                        conf,
                        org.apache.stormcrawler.Constants.PARTITION_MODEParamName,
                        org.apache.stormcrawler.Constants.PARTITION_MODE_HOST);
        if (!org.apache.stormcrawler.Constants.PARTITION_MODE_HOST.equals(partitionMode)) {
            throw new IllegalArgumentException(
                    "robots crawl-delay pacing requires partition.url.mode="
                            + org.apache.stormcrawler.Constants.PARTITION_MODE_HOST
                            + "; found "
                            + partitionMode);
        }

        int maxURLsPerBucket =
                ConfUtils.getInt(
                        conf,
                        Constants.URLFRONTIER_MAX_URLS_PER_BUCKET_KEY,
                        Constants.URLFRONTIER_MAX_URLS_PER_BUCKET_DEFAULT);
        if (maxURLsPerBucket != 1) {
            throw new IllegalArgumentException(
                    "robots crawl-delay pacing requires "
                            + Constants.URLFRONTIER_MAX_URLS_PER_BUCKET_KEY
                            + "=1; found "
                            + maxURLsPerBucket);
        }

        long maxDelaySecs =
                ConfUtils.getLong(
                        conf,
                        Constants.URLFRONTIER_BACKOFF_MAX_KEY,
                        Constants.URLFRONTIER_BACKOFF_MAX_DEFAULT);
        if (maxDelaySecs <= 0) {
            throw new IllegalArgumentException(
                    "robots crawl-delay pacing requires "
                            + Constants.URLFRONTIER_BACKOFF_MAX_KEY
                            + " > 0");
        }

        final String robotsDelayKey = org.apache.stormcrawler.Constants.ROBOTS_CRAWL_DELAY_KEY;
        MetadataTransfer transfer = MetadataTransfer.getInstance(conf);
        Metadata probe = new Metadata();
        probe.setValue(robotsDelayKey, "1");
        Metadata persisted = transfer.filter(probe);
        if (persisted == null || persisted.getFirstValue(robotsDelayKey) == null) {
            throw new IllegalArgumentException(
                    "robots.crawl.delay must be included in metadata.persist");
        }

        Metadata outlink =
                transfer.getMetaForOutlink(
                        "http://target.example/", "http://source.example/", probe);
        if (outlink == null || outlink.getFirstValue(robotsDelayKey) != null) {
            throw new IllegalArgumentException(
                    "robots.crawl.delay must not appear in metadata.transfer, including via a"
                            + " wildcard: it is a per-host control signal");
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // terminal bolt
    }

    @Override
    public void cleanup() {
        closed = true;
        if (crawlDelayPolicy != null) {
            crawlDelayPolicy.clear();
        }
        if (channel != null) {
            channel.shutdown();
        }
    }
}
