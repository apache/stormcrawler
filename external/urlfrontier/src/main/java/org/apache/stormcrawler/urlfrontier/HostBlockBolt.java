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
import org.apache.stormcrawler.protocol.ProtocolResponse;
import org.apache.stormcrawler.util.ConfUtils;
import org.apache.stormcrawler.util.MetadataTransfer;
import org.apache.stormcrawler.util.RetryAfterParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Consumes the {@code queue} stream emitted by the status updater and blocks a queue in URLFrontier
 * via {@code blockQueueUntil} whenever the tuple's metadata reports a rate-limit response (HTTP 429
 * or 503) carrying a <a
 * href="https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Retry-After">Retry-After</a>
 * header. See issues #867 and #784.
 *
 * <p>Wire it with a fields grouping on {@code "key"} from the status updater's {@code queue}
 * stream. The {@code "key"} is the frontier queue key derived from {@code partition.url.mode} (the
 * same setting the frontier uses to assign queues), so the block targets the matching queue. The
 * honoured delay is capped by {@code urlfrontier.max.retry.after} (in seconds, default 86400, -1 to
 * disable the cap).
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
 * (the second entry depends on {@code protocol.md.prefix}). A warning is logged at startup when the
 * configuration would strip them.
 *
 * <p>Connects via {@code urlfrontier.address}, falling back to {@code urlfrontier.host} / {@code
 * urlfrontier.port}. The block is issued with {@code local=false} and propagates to the whole
 * cluster, so with several frontier nodes a single connection to any one of them is enough.
 *
 * <p>The block is fire-and-forget with a short deadline: a failed or expired call is logged but the
 * tuple is acked anyway. A missed block means the host is fetched once more and the next 429
 * re-emits the signal.
 */
public class HostBlockBolt extends BaseRichBolt {

    private static final Logger LOG = LoggerFactory.getLogger(HostBlockBolt.class);

    /** Metadata key set by the FetcherBolt with the HTTP status code of the fetch. */
    private static final String STATUS_CODE_KEY = "fetch.statusCode";

    /** Name of the Retry-After HTTP header, lower-cased as stored by the protocol layer. */
    private static final String RETRY_AFTER_HEADER = "retry-after";

    /**
     * Queue key used by the status updaters when no partition key can be derived from a URL. The
     * queue is shared by unrelated URLs, so it is never blocked.
     */
    private static final String DEFAULT_QUEUE_KEY = "_DEFAULT_";

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

    /** Metadata key holding the Retry-After header, including the protocol prefix. */
    private String retryAfterKey;

    /** Upper bound in ms for the honoured Retry-After delay; -1 means no cap. */
    private long maxRetryAfterMs;

    @Override
    public void prepare(Map<String, Object> conf, TopologyContext context, OutputCollector c) {
        this.collector = c;
        this.globalCrawlID =
                ConfUtils.getString(conf, Constants.URLFRONTIER_CRAWL_ID_KEY, CrawlID.DEFAULT);
        // the protocol layer stores response headers in the metadata with this
        // prefix; the FetcherBolt merges them into the status metadata
        this.retryAfterKey =
                ConfUtils.getString(conf, ProtocolResponse.PROTOCOL_MD_PREFIX_PARAM, "")
                        + RETRY_AFTER_HEADER;
        long maxRetryAfterSecs =
                ConfUtils.getLong(
                        conf,
                        Constants.URLFRONTIER_MAX_RETRY_AFTER_KEY,
                        Constants.URLFRONTIER_MAX_RETRY_AFTER_DEFAULT);
        this.maxRetryAfterMs = maxRetryAfterSecs < 0 ? -1L : maxRetryAfterSecs * 1000L;
        // the status updater emits the queue stream after the metadata.persist
        // filter: warn upfront when the keys this bolt relies on would be
        // stripped, as the bolt would otherwise silently never block anything
        Set<String> missing = missingQueueStreamKeys(conf, retryAfterKey);
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
        final long blockUntil =
                blockUntilFor(
                        key, metadata, retryAfterKey, maxRetryAfterMs, System.currentTimeMillis());
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
     * Decides whether a queue-stream tuple carries a server-requested back-off worth enforcing.
     * Only a rate-limit (429) or unavailable (503) response with a valid Retry-After header
     * qualifies; the requested delay is capped by {@code maxRetryAfterMs} unless negative. The
     * shared {@code _DEFAULT_} queue is never blocked.
     *
     * @return the absolute time to block the queue until, in epoch seconds, or {@code -1} if the
     *     tuple does not call for a block
     */
    static long blockUntilFor(
            String key, Metadata metadata, String retryAfterKey, long maxRetryAfterMs, long nowMs) {
        if (metadata == null || DEFAULT_QUEUE_KEY.equals(key)) {
            return -1L;
        }
        final String statusCode = metadata.getFirstValue(STATUS_CODE_KEY);
        // only on a rate-limit (429) or unavailable (503) response does
        // Retry-After signal a host back-off worth acting on
        if (!"429".equals(statusCode) && !"503".equals(statusCode)) {
            return -1L;
        }
        long retryAfterMs = RetryAfterParser.parseDelay(metadata.getFirstValue(retryAfterKey));
        if (retryAfterMs <= 0) {
            return -1L;
        }
        if (maxRetryAfterMs >= 0 && retryAfterMs > maxRetryAfterMs) {
            retryAfterMs = maxRetryAfterMs;
        }
        long blockUntilMs;
        try {
            blockUntilMs = Math.addExact(nowMs, retryAfterMs);
        } catch (ArithmeticException e) {
            // uncapped delay pushed the block time past Long.MAX_VALUE:
            // clamp instead of wrapping negative and skipping the block
            blockUntilMs = Long.MAX_VALUE;
        }
        return blockUntilMs / 1000L;
    }

    /**
     * Returns the queue-stream keys this bolt relies on that would not survive the {@code
     * metadata.persist} filter applied by the status updater before emitting.
     */
    static Set<String> missingQueueStreamKeys(Map<String, Object> conf, String retryAfterKey) {
        Metadata probe = new Metadata();
        probe.setValue(STATUS_CODE_KEY, "429");
        probe.setValue(retryAfterKey, "1");
        Metadata filtered = MetadataTransfer.getInstance(conf).filter(probe);
        Set<String> missing = new LinkedHashSet<>();
        if (filtered.getFirstValue(STATUS_CODE_KEY) == null) {
            missing.add(STATUS_CODE_KEY);
        }
        if (filtered.getFirstValue(retryAfterKey) == null) {
            missing.add(retryAfterKey);
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
