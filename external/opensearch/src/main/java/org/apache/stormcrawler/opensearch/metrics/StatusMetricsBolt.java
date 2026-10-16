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

package org.apache.stormcrawler.opensearch.metrics;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.storm.Config;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.TupleUtils;
import org.apache.stormcrawler.metrics.CrawlerMetrics;
import org.apache.stormcrawler.opensearch.Constants;
import org.apache.stormcrawler.opensearch.OpenSearchConnection;
import org.apache.stormcrawler.util.ConfUtils;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch._types.FieldValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Queries the status index periodically to get the count of URLs per status. This bolt can be
 * connected to the output of any other bolt and will not produce anything as output.
 */
public class StatusMetricsBolt extends BaseRichBolt {

    private static final Logger LOG = LoggerFactory.getLogger(StatusMetricsBolt.class);

    private static final String OSBoltType = "status";
    private static final String OSStatusIndexNameParamName =
            Constants.PARAMPREFIX + "status.index.name";

    private String indexName;

    private OpenSearchClient client;

    private Map<String, Long> latestStatusCounts = new ConcurrentHashMap<>(6);

    private int freqStats = 60;

    private OutputCollector _collector;

    private transient StatusCounter[] counters;

    private static final class StatusCounter {
        final String name;
        final AtomicBoolean ready = new AtomicBoolean(true);

        StatusCounter(String name) {
            this.name = name;
        }
    }

    @Override
    public void prepare(
            Map<String, Object> stormConf, TopologyContext context, OutputCollector collector) {
        _collector = collector;
        indexName = ConfUtils.getString(stormConf, OSStatusIndexNameParamName, "status");
        try {
            client = OpenSearchConnection.getClient(stormConf, OSBoltType);
        } catch (Exception e1) {
            LOG.error("Can't connect to OpenSearch", e1);
            throw new RuntimeException(e1);
        }

        CrawlerMetrics.registerGauge(
                context, stormConf, "status.count", () -> latestStatusCounts, freqStats);

        counters = new StatusCounter[6];

        counters[0] = new StatusCounter("DISCOVERED");
        counters[1] = new StatusCounter("FETCHED");
        counters[2] = new StatusCounter("FETCH_ERROR");
        counters[3] = new StatusCounter("REDIRECTION");
        counters[4] = new StatusCounter("ERROR");
        counters[5] = new StatusCounter("TOTAL");
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Config conf = new Config();
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, freqStats);
        return conf;
    }

    @Override
    public void execute(Tuple input) {
        _collector.ack(input);

        // this bolt can be connected to anything
        // we just want to trigger a new search when the input is a tick tuple
        if (!TupleUtils.isTick(input)) {
            return;
        }

        for (StatusCounter counter : counters) {
            // still waiting for results from previous request
            if (!counter.ready.compareAndSet(true, false)) {
                LOG.debug("Not ready to get counts for status {}", counter.name);
                continue;
            }
            final String statusName = counter.name;
            CompletableFuture.supplyAsync(
                            () -> {
                                try {
                                    if (statusName.equalsIgnoreCase("TOTAL")) {
                                        return client.count(c -> c.index(indexName));
                                    } else {
                                        return client.count(
                                                c ->
                                                        c.index(indexName)
                                                                .query(
                                                                        q ->
                                                                                q.term(
                                                                                        t ->
                                                                                                t.field(
                                                                                                                "status")
                                                                                                        .value(
                                                                                                                FieldValue
                                                                                                                        .of(
                                                                                                                                statusName)))));
                                    }
                                } catch (Exception e) {
                                    throw new CompletionException(e);
                                }
                            })
                    .thenAccept(
                            response -> {
                                counter.ready.set(true);
                                LOG.debug(
                                        "Got {} counts for status:{}",
                                        response.count(),
                                        statusName);
                                latestStatusCounts.put(statusName, response.count());
                            })
                    .exceptionally(
                            e -> {
                                counter.ready.set(true);
                                Throwable cause =
                                        e instanceof CompletionException ? e.getCause() : e;
                                LOG.error(
                                        "Failure when getting counts for status:{}",
                                        statusName,
                                        cause);
                                return null;
                            });
        }
    }

    @Override
    public void cleanup() {
        if (client != null) {
            try {
                client._transport().close();
            } catch (Exception e) {
                LOG.error("Exception closing client transport", e);
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // NONE - THIS BOLT DOES NOT GET CONNECTED TO ANY OTHERS
    }
}
