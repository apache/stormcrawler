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
 * it via {@code blockQueueUntil} whenever the tuple's metadata reports a rate-limit response. See
 * issues #867, #784 and #1106.
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
 * <p>Blocking stops new hand-outs; it does not recall URLs of the host already prefetched by the
 * spout into the topology, which still fail against the rate-limited server and reschedule via the
 * status stream. Keep {@code urlfrontier.max.urls.per.bucket} and {@code
 * topology.max.spout.pending} small for a block to bite quickly.
 *
 * <p>Connects via {@code urlfrontier.address}, falling back to {@code urlfrontier.host} / {@code
 * urlfrontier.port}. The block is issued with {@code local=false} and propagates to the whole
 * cluster, so with several frontier nodes a single connection to any one of them is enough.
 *
 * <p>The block is fire-and-forget with a short deadline: a failed or expired call is logged but the
 * tuple is acked anyway. A missed block heals itself: every further rate-limit signal for the host
 * re-asserts the stored block until the frontier stops serving it.
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

    @Override
    public void prepare(Map<String, Object> conf, TopologyContext context, OutputCollector c) {
        this.collector = c;
        this.globalCrawlID =
                ConfUtils.getString(conf, Constants.URLFRONTIER_CRAWL_ID_KEY, CrawlID.DEFAULT);
        this.backoff = new HostBackoff(conf);
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
        collector.ack(t);
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

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // terminal bolt
    }

    @Override
    public void cleanup() {
        if (channel != null) {
            channel.shutdown();
        }
    }
}
