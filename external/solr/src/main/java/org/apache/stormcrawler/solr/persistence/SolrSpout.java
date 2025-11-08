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
package org.apache.stormcrawler.solr.persistence;

import java.time.Instant;
import java.util.Collection;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.commons.lang3.StringUtils;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrQuery.ORDER;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.response.Group;
import org.apache.solr.client.solrj.response.GroupCommand;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.persistence.AbstractQueryingSpout;
import org.apache.stormcrawler.solr.Constants;
import org.apache.stormcrawler.solr.SolrConnection;
import org.apache.stormcrawler.util.ConfUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Spout which pulls URLs from a Solr index. The number of Spout instances should be the same as the
 * number of Solr shards (`solr.status.routing.shards`). Guarantees a good mix of URLs by
 * aggregating them by an arbitrary field e.g. key.
 */
@SuppressWarnings("serial")
public class SolrSpout extends AbstractQueryingSpout {

    private static final Logger LOG = LoggerFactory.getLogger(SolrSpout.class);

    private static final String BOLT_TYPE = "status";

    private static final String SolrDiversityFieldParam = "solr.status.bucket.field";
    private static final String SolrDiversityBucketParam = "solr.status.bucket.maxsize";
    private static final String SolrMetadataPrefix = "solr.status.metadata.prefix";
    private static final String SolrMaxResultsParam = "solr.status.max.results";

    private static final String SolrShardsParamName = Constants.PARAMPREFIX + "%s.routing.shards";

    private int solrShards;

    private int shardID = 1;

    private SolrConnection connection;

    private int maxNumResults = 10;

    private int lastStartOffset = 0;

    private Instant lastNextFetchDate = null;

    private String diversityField = null;

    private int diversityBucketSize = 0;

    private String mdPrefix;

    @Override
    public void open(
            Map<String, Object> stormConf,
            TopologyContext context,
            SpoutOutputCollector collector) {

        super.open(stormConf, context, collector);

        solrShards =
                ConfUtils.getInt(
                        stormConf,
                        String.format(Locale.ROOT, SolrSpout.SolrShardsParamName, BOLT_TYPE),
                        1);

        int totalTasks = context.getComponentTasks(context.getThisComponentId()).size();
        if (totalTasks != solrShards) {
            throw new RuntimeException(
                    "Number of SolrSpout instances should be the same as 'status' collection shards");
        } else {
            // Solr uses 1-based indexing in shard names (shard1, shard2, ...)
            shardID = context.getThisTaskIndex() + 1;
        }

        diversityField = ConfUtils.getString(stormConf, SolrDiversityFieldParam);
        diversityBucketSize = ConfUtils.getInt(stormConf, SolrDiversityBucketParam, 5);
        // the results have the first hit separate from the expansions
        diversityBucketSize--;

        mdPrefix = ConfUtils.getString(stormConf, SolrMetadataPrefix, "metadata");

        maxNumResults = ConfUtils.getInt(stormConf, SolrMaxResultsParam, 10);

        try {
            connection = SolrConnection.getConnection(stormConf, BOLT_TYPE);
        } catch (Exception e) {
            LOG.error("Can't connect to Solr: {}", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        if (connection != null) {
            try {
                connection.close();
            } catch (Exception e) {
                LOG.error("Can't close connection to Solr: {}", e);
            }
        }
    }

    protected void populateBuffer() {

        SolrQuery query = new SolrQuery();

        if (lastNextFetchDate == null) {
            lastNextFetchDate = Instant.now();
            lastStartOffset = 0;
            lastTimeResetToNow = Instant.now();
        }
        // reset the value for next fetch date if the previous one is too
        // old
        else if (resetFetchDateAfterNSecs != -1) {
            Instant changeNeededOn =
                    Instant.ofEpochMilli(
                            lastTimeResetToNow.toEpochMilli() + (resetFetchDateAfterNSecs * 1000));
            if (Instant.now().isAfter(changeNeededOn)) {
                LOG.info(
                        "lastDate reset based on resetFetchDateAfterNSecs {}",
                        resetFetchDateAfterNSecs);
                lastNextFetchDate = Instant.now();
                lastStartOffset = 0;
                lastTimeResetToNow = Instant.now();
            }
        }

        query.setQuery("*:*")
                .addFilterQuery("nextFetchDate:[* TO " + lastNextFetchDate + "]")
                .setSort("nextFetchDate", ORDER.asc);

        // add the shard parameter only when having multiple shards
        if (solrShards > 1) {
            query.setParam("shards", "shard" + shardID);
        }

        if (StringUtils.isNotBlank(diversityField) && diversityBucketSize > 0) {
            String[] diversityFields = diversityField.split(",");
            query.setStart(0)
                    .setRows(diversityBucketSize)
                    .set("indent", "true")
                    .set("group", "true")
                    .set("group.limit", this.maxNumResults)
                    .set("group.sort", "nextFetchDate asc")
                    .set("group.offset", lastStartOffset);
            for (String groupField : diversityFields) {
                query.set("group.field", groupField);
            }
        } else {
            query.setStart(lastStartOffset).setRows(this.maxNumResults);
        }

        LOG.debug("QUERY => {}", query);

        LOG.trace("isInQuery set to true");
        isInQuery.set(true);

        QueryRequest queryRequest = new QueryRequest(query);
        CompletableFuture<QueryResponse> future = connection.requestAsync(queryRequest);

        future.whenComplete(
                (futureResponse, throwable) -> {
                    if (throwable != null) {
                        LOG.error("Exception while querying Solr", throwable);
                    } else {
                        handleSuccess(futureResponse);
                    }
                });
    }

    protected void handleSuccess(QueryResponse response) {

        long timeTaken = System.currentTimeMillis() - getTimeLastQuerySent();

        markQueryReceivedNow();

        queryTimes.addMeasurement(timeTaken);

        SolrDocumentList docs = new SolrDocumentList();

        LOG.debug("Response : {}", response);

        // add the main results
        if (response.getResults() != null) {
            docs.addAll(response.getResults());
        }
        int groupsTotal = 0;
        // get groups
        if (response.getGroupResponse() != null) {
            for (GroupCommand groupCommand : response.getGroupResponse().getValues()) {
                for (Group group : groupCommand.getValues()) {
                    groupsTotal = Math.max(groupsTotal, group.getResult().size());
                    LOG.debug("Group : {}", group);
                    docs.addAll(group.getResult());
                }
            }
        }

        int numhits = (response.getResults() != null) ? response.getResults().size() : groupsTotal;

        // no more results?
        if (numhits == 0) {
            lastStartOffset = 0;
            lastNextFetchDate = null;
        } else {
            lastStartOffset += numhits;
        }

        String prefix = mdPrefix.concat(".");

        int alreadyProcessed = 0;
        int docReturned = 0;

        for (SolrDocument doc : docs) {
            String url = (String) doc.get("url");

            docReturned++;

            // is already being processed - skip it!
            if (beingProcessed.containsKey(url)) {
                alreadyProcessed++;
                continue;
            }

            Metadata metadata = new Metadata();

            for (String key : doc.getFieldNames()) {
                if (key.startsWith(prefix)) {
                    Collection<Object> values = doc.getFieldValues(key);

                    key = key.substring(prefix.length());
                    for (Object value : values) {
                        metadata.addValue(key, (String) value);
                    }
                }
            }

            buffer.add(url, metadata);
        }

        LOG.info(
                "SOLR returned {} results from {} buckets in {} msec including {} already being processed",
                docReturned,
                numhits,
                timeTaken,
                alreadyProcessed);
    }

    @Override
    public void ack(Object msgId) {
        LOG.debug("Ack for {}", msgId);
        super.ack(msgId);
    }

    @Override
    public void fail(Object msgId) {
        LOG.info("Fail for {}", msgId);
        super.fail(msgId);
    }
}
