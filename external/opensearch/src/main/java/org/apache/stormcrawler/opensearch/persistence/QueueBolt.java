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

package org.apache.stormcrawler.opensearch.persistence;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.stormcrawler.opensearch.IndexCreation;
import org.apache.stormcrawler.opensearch.OpenSearchConnection;
import org.apache.stormcrawler.util.ConfUtils;
import org.joda.time.Instant;
import org.opensearch.action.index.IndexRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Sends information about the queues into an OpenSearch index. This has to be connected to a status
 * updater bolt.
 */
public class QueueBolt extends BaseRichBolt {

    private static final Logger LOG = LoggerFactory.getLogger(QueueBolt.class);

    private static final String OSBoltType = "queues";

    static final String ESIndexNameParamName =
            org.apache.stormcrawler.opensearch.Constants.PARAMPREFIX + OSBoltType + ".index.name";

    private OutputCollector _collector;

    private String indexName;

    private OpenSearchConnection connection;

    private Cache<String, String> knownQueue;

    public QueueBolt() {}

    /** Sets the index name instead of taking it from the configuration. * */
    public QueueBolt(String indexName) {
        this.indexName = indexName;
    }

    @Override
    public void prepare(
            Map<String, Object> conf, TopologyContext context, OutputCollector collector) {
        _collector = collector;
        if (indexName == null) {
            indexName = ConfUtils.getString(conf, QueueBolt.ESIndexNameParamName, OSBoltType);
        }
        try {
            connection = OpenSearchConnection.getConnection(conf, OSBoltType);
        } catch (Exception e1) {
            LOG.error("Can't connect to OpenSearch", e1);
            throw new RuntimeException(e1);
        }
        try {
            IndexCreation.checkOrCreateIndex(connection.getClient(), indexName, OSBoltType, LOG);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        knownQueue = Caffeine.newBuilder().maximumSize(10000).build();
    }

    @Override
    public void cleanup() {
        if (connection != null) {
            connection.close();
        }
    }

    @Override
    public void execute(Tuple tuple) {

        final String key = tuple.getStringByField("key");

        // check whether this key is already known
        if (knownQueue.getIfPresent(key) != null) {
            _collector.ack(tuple);
            return;
        }

        final String docID = org.apache.commons.codec.digest.DigestUtils.sha256Hex(key);

        final HashMap<String, String> fields = new HashMap<>();
        fields.put("key", key);
        fields.put("lastUpdated", Instant.now().toString());

        final IndexRequest indexRequest =
                new IndexRequest(indexName).source(fields).id(docID).create(true);

        knownQueue.put(key, docID);

        connection.addToProcessor(indexRequest);

        // ack no matter what
        _collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // nothing to do here - this bolt is the last of a topology
    }
}
