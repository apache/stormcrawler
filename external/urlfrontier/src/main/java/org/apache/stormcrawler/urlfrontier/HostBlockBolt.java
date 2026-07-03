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
import java.util.Map;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.protocol.ProtocolResponse;
import org.apache.stormcrawler.util.ConfUtils;
import org.apache.stormcrawler.util.RetryAfterParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Consumes the {@code queue} stream emitted by the status updater and blocks a queue in
 * URLFrontier via {@code blockQueueUntil} whenever the tuple's metadata reports a rate-limit
 * response (HTTP 429 or 503) carrying a <a
 * href="https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Retry-After">Retry-After</a>
 * header. See issues #867 and #784.
 *
 * <p>Wire it with a fields grouping on {@code "key"} from the status updater's {@code queue}
 * stream. The {@code "key"} is the frontier queue key derived from {@code partition.url.mode} (the
 * same setting the frontier uses to assign queues), so the block targets the matching queue. The
 * honoured delay is capped by {@code urlfrontier.max.retry.after} (in seconds, default 86400, -1
 * to disable the cap). Connects via {@code urlfrontier.host} / {@code urlfrontier.port};
 * multi-node address resolution is not supported.
 *
 * <p>The block is fire-and-forget: a failed call is logged but the tuple is acked anyway. A missed
 * block means the host is fetched once more and the next 429 re-emits the signal.
 */
public class HostBlockBolt extends BaseRichBolt {

    private static final Logger LOG = LoggerFactory.getLogger(HostBlockBolt.class);

    /** Metadata key set by the FetcherBolt with the HTTP status code of the fetch. */
    private static final String STATUS_CODE_KEY = "fetch.statusCode";

    /** Name of the Retry-After HTTP header, lower-cased as stored by the protocol layer. */
    private static final String RETRY_AFTER_HEADER = "retry-after";

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
        // build channel + stub as in Spout.prepare (host/port only)
        this.channel =
                ManagedChannelUtil.createChannel(
                        ConfUtils.getString(
                                conf,
                                Constants.URLFRONTIER_HOST_KEY,
                                Constants.URLFRONTIER_DEFAULT_HOST),
                        ConfUtils.getInt(
                                conf,
                                Constants.URLFRONTIER_PORT_KEY,
                                Constants.URLFRONTIER_DEFAULT_PORT));
        this.frontier = URLFrontierGrpc.newStub(channel).withWaitForReady();
    }

    @Override
    public void execute(Tuple t) {
        final String key = t.getStringByField("key");
        final Metadata metadata = (Metadata) t.getValueByField("metadata");
        final long blockUntil =
                blockUntilFor(metadata, retryAfterKey, maxRetryAfterMs, System.currentTimeMillis());
        if (blockUntil > 0) {
            LOG.debug("Blocking queue {} until {}", key, blockUntil);
            BlockQueueParams params =
                    BlockQueueParams.newBuilder()
                            .setKey(key)
                            .setCrawlID(globalCrawlID)
                            .setTime(blockUntil)
                            .setLocal(false)
                            .build();
            frontier.blockQueueUntil(params, NOOP_OBSERVER);
        }
        collector.ack(t);
    }

    /**
     * Decides whether a queue-stream tuple carries a server-requested back-off worth enforcing.
     * Only a rate-limit (429) or unavailable (503) response with a valid Retry-After header
     * qualifies; the requested delay is capped by {@code maxRetryAfterMs} unless negative.
     *
     * @return the absolute time to block the queue until, in epoch seconds, or {@code -1} if the
     *     tuple does not call for a block
     */
    static long blockUntilFor(Metadata metadata, String retryAfterKey, long maxRetryAfterMs, long nowMs) {
        if (metadata == null) {
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
        return (nowMs + retryAfterMs) / 1000L;
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
