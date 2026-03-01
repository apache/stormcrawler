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

import java.util.HashMap;
import java.util.Map;
import org.apache.storm.Config;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.TupleUtils;
import org.apache.stormcrawler.opensearch.Constants;
import org.apache.stormcrawler.opensearch.OpenSearchConnection;
import org.apache.stormcrawler.util.ConfUtils;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.core.CountRequest;
import org.opensearch.client.core.CountResponse;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.builder.SearchSourceBuilder;
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

    private OpenSearchConnection connection;

    private Map<String, Long> latestStatusCounts = new HashMap<>(6);

    private int freqStats = 60;

    private OutputCollector _collector;

    private transient StatusActionListener[] listeners;

    private class StatusActionListener implements ActionListener<CountResponse> {

        private final String name;

        private boolean ready = true;

        public boolean isReady() {
            return ready;
        }

        public void busy() {
            this.ready = false;
        }

        StatusActionListener(String statusName) {
            name = statusName;
        }

        @Override
        public void onResponse(CountResponse response) {
            ready = true;
            LOG.debug("Got {} counts for status:{}", response.getCount(), name);
            latestStatusCounts.put(name, response.getCount());
        }

        @Override
        public void onFailure(Exception e) {
            ready = true;
            LOG.error("Failure when getting counts for status:{}", name, e);
        }
    }

    @Override
    public void prepare(
            Map<String, Object> stormConf, TopologyContext context, OutputCollector collector) {
        _collector = collector;
        indexName = ConfUtils.getString(stormConf, OSStatusIndexNameParamName, "status");
        try {
            connection = OpenSearchConnection.getConnection(stormConf, OSBoltType);
        } catch (Exception e1) {
            LOG.error("Can't connect to ElasticSearch", e1);
            throw new RuntimeException(e1);
        }

        context.registerMetric(
                "status.count",
                () -> {
                    return latestStatusCounts;
                },
                freqStats);

        listeners = new StatusActionListener[6];

        listeners[0] = new StatusActionListener("DISCOVERED");
        listeners[1] = new StatusActionListener("FETCHED");
        listeners[2] = new StatusActionListener("FETCH_ERROR");
        listeners[3] = new StatusActionListener("REDIRECTION");
        listeners[4] = new StatusActionListener("ERROR");
        listeners[5] = new StatusActionListener("TOTAL");
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

        for (StatusActionListener listener : listeners) {
            // still waiting for results from previous request
            if (!listener.isReady()) {
                LOG.debug("Not ready to get counts for status {}", listener.name);
                continue;
            }
            CountRequest request = new CountRequest(indexName);
            if (!listener.name.equalsIgnoreCase("TOTAL")) {
                SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
                sourceBuilder.query(QueryBuilders.termQuery("status", listener.name));
                request.source(sourceBuilder);
            }
            listener.busy();
            connection.getClient().countAsync(request, RequestOptions.DEFAULT, listener);
        }
    }

    @Override
    public void cleanup() {
        connection.close();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // NONE - THIS BOLT DOES NOT GET CONNECTED TO ANY OTHERS
    }
}
