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
import org.apache.stormcrawler.util.ConfUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Consumes host back-off signals emitted by the {@code FetcherBolt} on the {@code hostinfo} stream
 * and blocks the corresponding queue in URLFrontier until the requested time, via {@code
 * blockQueueUntil}. See issues #867 and #784. Wire it with a fields grouping on {@code "key"} from
 * the FetcherBolt's {@code hostinfo} stream. The {@code "key"} is the frontier queue key derived
 * from {@code partition.url.mode} (the same setting the frontier uses to assign queues), so the
 * block targets the matching queue. Connects via {@code urlfrontier.host} / {@code
 * urlfrontier.port}; multi-node address resolution is not supported.
 *
 * <p>The block is fire-and-forget: a failed call is logged but the tuple is acked anyway. A missed
 * block means the host is fetched once more and the next 429 re-emits the signal.
 */
public class HostBlockBolt extends BaseRichBolt {

    private static final Logger LOG = LoggerFactory.getLogger(HostBlockBolt.class);

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

    @Override
    public void prepare(Map<String, Object> conf, TopologyContext context, OutputCollector c) {
        this.collector = c;
        this.globalCrawlID =
                ConfUtils.getString(conf, Constants.URLFRONTIER_CRAWL_ID_KEY, CrawlID.DEFAULT);
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
        String key = t.getStringByField("key");
        long blockUntil = t.getLongByField("blockUntil");
        BlockQueueParams params =
                BlockQueueParams.newBuilder()
                        .setKey(key)
                        .setCrawlID(globalCrawlID)
                        .setTime(blockUntil)
                        .setLocal(false)
                        .build();
        frontier.blockQueueUntil(params, NOOP_OBSERVER);
        collector.ack(t);
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
