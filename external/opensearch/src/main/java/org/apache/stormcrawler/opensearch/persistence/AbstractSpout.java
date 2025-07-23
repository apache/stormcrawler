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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.opensearch.Constants;
import org.apache.stormcrawler.opensearch.IndexCreation;
import org.apache.stormcrawler.opensearch.OpenSearchConnection;
import org.apache.stormcrawler.persistence.AbstractQueryingSpout;
import org.apache.stormcrawler.util.ConfUtils;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.search.SearchHit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractSpout extends AbstractQueryingSpout {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractSpout.class);

    protected static final String OSBoltType = "status";
    protected static final String OSStatusIndexNameParamName =
            Constants.PARAMPREFIX + OSBoltType + ".index.name";

    /** Field name to use for aggregating * */
    protected static final String OSStatusBucketFieldParamName =
            Constants.PARAMPREFIX + OSBoltType + ".bucket.field";

    protected static final String OSStatusMaxBucketParamName =
            Constants.PARAMPREFIX + OSBoltType + ".max.buckets";
    protected static final String OSStatusMaxURLsParamName =
            Constants.PARAMPREFIX + OSBoltType + ".max.urls.per.bucket";

    /** Field name to use for sorting the URLs within a bucket, not used if empty or null. */
    protected static final String OSStatusBucketSortFieldParamName =
            Constants.PARAMPREFIX + OSBoltType + ".bucket.sort.field";

    /** Field name to use for sorting the buckets, not used if empty or null. */
    protected static final String OSStatusGlobalSortFieldParamName =
            Constants.PARAMPREFIX + OSBoltType + ".global.sort.field";

    protected static final String OSStatusFilterParamName =
            Constants.PARAMPREFIX + OSBoltType + ".filterQuery";

    protected static final String OSStatusQueryTimeoutParamName =
            Constants.PARAMPREFIX + OSBoltType + ".query.timeout";

    /** Query to use as a positive filter, set by es.status.filterQuery */
    protected List<String> filterQueries = null;

    protected String indexName;

    protected static RestHighLevelClient client;

    /**
     * when using multiple instances - each one is in charge of a specific shard useful when
     * sharding based on host or domain to guarantee a good mix of URLs
     */
    protected int shardID = -1;

    /** Used to distinguish between instances in the logs * */
    protected String logIdprefix = "";

    /** Field name used for field collapsing e.g. key * */
    protected String partitionField;

    protected int maxURLsPerBucket = 10;

    protected int maxBucketNum = 10;

    protected List<String> bucketSortField = new ArrayList<>();

    protected String totalSortField = "";

    protected Date queryDate;

    protected int queryTimeout = -1;

    @Override
    public void open(
            Map<String, Object> stormConf,
            TopologyContext context,
            SpoutOutputCollector collector) {

        super.open(stormConf, context, collector);

        indexName = ConfUtils.getString(stormConf, OSStatusIndexNameParamName, "status");

        // one OS client per JVM
        synchronized (AbstractSpout.class) {
            try {
                if (client == null) {
                    client = OpenSearchConnection.getClient(stormConf, OSBoltType);
                }
            } catch (Exception e1) {
                LOG.error("Can't connect to ElasticSearch", e1);
                throw new RuntimeException(e1);
            }

            // use the default status schema if none has been specified
            try {
                IndexCreation.checkOrCreateIndex(client, indexName, OSBoltType, LOG);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        // if more than one instance is used we expect their number to be the
        // same as the number of shards
        int totalTasks = context.getComponentTasks(context.getThisComponentId()).size();
        if (totalTasks > 1) {
            logIdprefix =
                    "[" + context.getThisComponentId() + " #" + context.getThisTaskIndex() + "] ";

            // determine the number of shards so that we can restrict the
            // search

            // TODO use the admin API when it gets available
            // TODO or the low level one with
            // https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-shards-stores.html
            // TODO identify local shards and use those if possible

            // ClusterSearchShardsRequest request = new
            // ClusterSearchShardsRequest(
            // indexName);
            // ClusterSearchShardsResponse shardresponse = client.admin()
            // .cluster().searchShards(request).actionGet();
            // ClusterSearchShardsGroup[] shardgroups =
            // shardresponse.getGroups();
            // if (totalTasks != shardgroups.length) {
            // throw new RuntimeException(
            // "Number of OS spout instances should be the same as number of
            // shards ("
            // + shardgroups.length + ") but is " + totalTasks);
            // }
            // shardID = shardgroups[context.getThisTaskIndex()].getShardId()
            // .getId();

            // TEMPORARY simply use the task index as shard index
            shardID = context.getThisTaskIndex();
            LOG.info("{} assigned shard ID {}", logIdprefix, shardID);
        }

        partitionField = ConfUtils.getString(stormConf, OSStatusBucketFieldParamName, "key");

        bucketSortField = ConfUtils.loadListFromConf(OSStatusBucketSortFieldParamName, stormConf);

        totalSortField = ConfUtils.getString(stormConf, OSStatusGlobalSortFieldParamName);

        maxURLsPerBucket = ConfUtils.getInt(stormConf, OSStatusMaxURLsParamName, 1);
        maxBucketNum = ConfUtils.getInt(stormConf, OSStatusMaxBucketParamName, 10);

        queryTimeout = ConfUtils.getInt(stormConf, OSStatusQueryTimeoutParamName, -1);

        filterQueries = ConfUtils.loadListFromConf(OSStatusFilterParamName, stormConf);
    }

    /** Builds a query and use it retrieve the results from OS * */
    protected abstract void populateBuffer();

    protected final boolean addHitToBuffer(SearchHit hit) {
        Map<String, Object> keyValues = hit.getSourceAsMap();
        String url = (String) keyValues.get("url");
        // is already being processed - skip it!
        if (beingProcessed.containsKey(url)) {
            return false;
        }
        return buffer.add(url, fromKeyValues(keyValues));
    }

    protected final Metadata fromKeyValues(Map<String, Object> keyValues) {
        Map<String, List<String>> mdAsMap = (Map<String, List<String>>) keyValues.get("metadata");
        Metadata metadata = new Metadata();
        if (mdAsMap != null) {
            for (Entry<String, List<String>> mdEntry : mdAsMap.entrySet()) {
                String key = mdEntry.getKey();
                // periods are not allowed - replace with %2E
                key = key.replaceAll("%2E", "\\.");
                Object mdValObj = mdEntry.getValue();
                // single value
                if (mdValObj instanceof String) {
                    metadata.addValue(key, (String) mdValObj);
                }
                // multi valued
                else {
                    metadata.addValues(key, (List<String>) mdValObj);
                }
            }
        }
        return metadata;
    }

    @Override
    public void ack(Object msgId) {
        LOG.debug("{}  Ack for {}", logIdprefix, msgId);
        super.ack(msgId);
    }

    @Override
    public void fail(Object msgId) {
        LOG.info("{}  Fail for {}", logIdprefix, msgId);
        super.fail(msgId);
    }

    @Override
    public void close() {
        if (client != null)
            try {
                client.close();
            } catch (IOException e) {
                LOG.error("Exception caught when closing client", e);
            }
    }
}
