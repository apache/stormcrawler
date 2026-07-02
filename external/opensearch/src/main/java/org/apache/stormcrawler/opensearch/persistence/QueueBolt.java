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
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.stormcrawler.opensearch.Constants;
import org.apache.stormcrawler.opensearch.IndexCreation;
import org.apache.stormcrawler.opensearch.OpenSearchConnection;
import org.apache.stormcrawler.util.ConfUtils;
import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.bulk.BulkItemResponse;
import org.opensearch.action.bulk.BulkProcessor;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.core.rest.RestStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Sends information about the queues into an OpenSearch index. This has to be connected to a status
 * updater bolt.
 */
public class QueueBolt extends BaseRichBolt implements BulkProcessor.Listener {

    private static final Logger LOG = LoggerFactory.getLogger(QueueBolt.class);

    private static final String OSBoltType = "queues";

    static final String OSQueuesIndexNameParamName =
            Constants.PARAMPREFIX + OSBoltType + ".index.name";

    /**
     * Parameter name to configure the cache of known queues. @see
     * https://github.com/ben-manes/caffeine/wiki/Specification Default value is
     * "maximumSize=10000".
     */
    static final String OSQueuesCacheSpecParamName =
            Constants.PARAMPREFIX + OSBoltType + ".cache.spec";

    private OutputCollector _collector;

    private String indexName;

    private OpenSearchConnection connection;

    /** Keys confirmed to be present in the index; populated on bulk success only */
    private Cache<String, Boolean> knownQueue;

    /** docID to key for the requests currently in the bulk processor */
    private final Map<String, String> inFlight = new ConcurrentHashMap<>();

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
            indexName = ConfUtils.getString(conf, QueueBolt.OSQueuesIndexNameParamName, OSBoltType);
        }
        try {
            connection = OpenSearchConnection.getConnection(conf, OSBoltType, this);
        } catch (Exception e1) {
            LOG.error("Can't connect to OpenSearch", e1);
            throw new RuntimeException(e1);
        }
        try {
            IndexCreation.checkOrCreateIndex(connection.getClient(), indexName, OSBoltType, LOG);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        final String cacheSpec =
                ConfUtils.getString(
                        conf, QueueBolt.OSQueuesCacheSpecParamName, "maximumSize=10000");
        knownQueue = Caffeine.from(cacheSpec).build();
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

        // ack no matter what - the queue info is best effort bookkeeping
        // and failed writes are retried when the key is next seen
        _collector.ack(tuple);

        // check whether this key is already known
        if (knownQueue.getIfPresent(key) != null) {
            return;
        }

        final String docID = org.apache.commons.codec.digest.DigestUtils.sha256Hex(key);

        // a request for this key is already in the bulk processor
        if (inFlight.putIfAbsent(docID, key) != null) {
            return;
        }

        final HashMap<String, String> fields = new HashMap<>();
        fields.put("key", key);
        fields.put("lastUpdated", Instant.now().toString());

        final IndexRequest indexRequest =
                new IndexRequest(indexName).source(fields).id(docID).create(true);

        connection.addToProcessor(indexRequest);
    }

    @Override
    public void beforeBulk(long executionId, BulkRequest request) {
        LOG.debug("beforeBulk {} with {} actions", executionId, request.numberOfActions());
    }

    @Override
    public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
        LOG.debug("afterBulk [{}] with {} responses", executionId, request.numberOfActions());
        for (BulkItemResponse item : response.getItems()) {
            final String key = inFlight.remove(item.getId());
            if (key == null) {
                continue;
            }
            if (!item.isFailed()) {
                knownQueue.put(key, Boolean.TRUE);
            } else if (RestStatus.CONFLICT.equals(item.getFailure().getStatus())) {
                // entry already exists in the index, e.g. written before a worker
                // restart or evicted from the cache - not an error
                knownQueue.put(key, Boolean.TRUE);
            } else {
                // do not cache the key so that the write is retried
                // when the key is next seen
                LOG.error(
                        "Failed to index queue entry {}: {}", key, item.getFailure().getMessage());
            }
        }
    }

    @Override
    public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
        LOG.error("Exception with bulk {} - failing the whole lot ", executionId, failure);
        for (DocWriteRequest<?> docWriteRequest : request.requests()) {
            // not cached - the writes are retried when the keys are next seen
            inFlight.remove(docWriteRequest.id());
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // nothing to do here - this bolt is the last of a topology
    }
}
