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

import static org.opensearch.index.query.QueryBuilders.boolQuery;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Map;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.stormcrawler.opensearch.Constants;
import org.apache.stormcrawler.persistence.EmptyQueueListener;
import org.apache.stormcrawler.util.ConfUtils;
import org.joda.time.format.ISODateTimeFormat;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.SearchHit;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.sort.FieldSortBuilder;
import org.opensearch.search.sort.SortBuilders;
import org.opensearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Uses collapsing spouts to get an initial set of URLs and keys to query for and gets emptyQueue
 * notifications from the URLBuffer to query OpenSearch for a specific key.
 *
 * @since 1.15
 */
public class HybridSpout extends AggregationSpout implements EmptyQueueListener {

    private static final Logger LOG = LoggerFactory.getLogger(HybridSpout.class);

    protected static final String RELOADPARAMNAME =
            Constants.PARAMPREFIX + "status.max.urls.per.reload";

    private int bufferReloadSize = 10;

    private Cache<String, Object[]> searchAfterCache;

    private HostResultListener hrl;

    @Override
    public void open(
            Map<String, Object> stormConf,
            TopologyContext context,
            SpoutOutputCollector collector) {
        super.open(stormConf, context, collector);
        bufferReloadSize = ConfUtils.getInt(stormConf, RELOADPARAMNAME, maxURLsPerBucket);
        buffer.setEmptyQueueListener(this);
        searchAfterCache = Caffeine.newBuilder().build();
        hrl = new HostResultListener();
    }

    @Override
    public void emptyQueue(String queueName) {

        LOG.info("{} Emptied buffer queue for {}", logIdprefix, queueName);

        if (!currentBuckets.contains(queueName)) {
            // not interested in this one any more
            return;
        }

        // reloading the aggregs - searching now
        // would just overload OpenSearch and yield
        // mainly duplicates
        if (isInQuery.get()) {
            LOG.trace("{} isInquery true", logIdprefix, queueName);
            return;
        }

        LOG.info("{} Querying for more docs for {}", logIdprefix, queueName);

        if (queryDate == null) {
            queryDate = new Date();
            lastTimeResetToNow = Instant.now();
        }

        String formattedQueryDate = ISODateTimeFormat.dateTimeNoMillis().print(queryDate.getTime());

        BoolQueryBuilder queryBuilder =
                boolQuery()
                        .filter(QueryBuilders.rangeQuery("nextFetchDate").lte(formattedQueryDate));

        queryBuilder.filter(QueryBuilders.termQuery(partitionField, queueName));

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(queryBuilder);
        sourceBuilder.from(0);
        sourceBuilder.size(bufferReloadSize);
        sourceBuilder.explain(false);
        sourceBuilder.trackTotalHits(false);

        // sort within a bucket
        for (String bsf : bucketSortField) {
            FieldSortBuilder sorter = SortBuilders.fieldSort(bsf).order(SortOrder.ASC);
            sourceBuilder.sort(sorter);
        }

        // do we have a search after for this one?
        Object[] searchAfterValues = searchAfterCache.getIfPresent(queueName);
        if (searchAfterValues != null) {
            sourceBuilder.searchAfter(searchAfterValues);
        }

        SearchRequest request = new SearchRequest(indexName);

        request.source(sourceBuilder);

        // https://www.elastic.co/guide/en/opensearch/reference/current/search-request-preference.html
        // _shards:2,3
        // specific shard but ideally a local copy of it
        if (shardID != -1) {
            request.preference("_shards:" + shardID + "|_local");
        }

        // dump query to log
        LOG.debug("{} OpenSearch query {} - {}", logIdprefix, queueName, request.toString());

        client.searchAsync(request, RequestOptions.DEFAULT, hrl);
    }

    @Override
    /** Overrides the handling of responses for aggregations */
    public void onResponse(SearchResponse response) {
        // delete all entries from the searchAfterCache when
        // we get the results from the aggregation spouts
        searchAfterCache.invalidateAll();
        super.onResponse(response);
    }

    @Override
    /** The aggregation kindly told us where to start from * */
    protected void sortValuesForKey(String key, Object[] sortValues) {
        if (sortValues != null && sortValues.length > 0) this.searchAfterCache.put(key, sortValues);
    }

    /** Handling of results for a specific queue * */
    class HostResultListener implements ActionListener<SearchResponse> {

        @Override
        public void onResponse(SearchResponse response) {

            int alreadyprocessed = 0;
            int numDocs = 0;

            SearchHit[] hits = response.getHits().getHits();

            Object[] sortValues = null;

            // retrieve the key for these results
            String key = null;

            for (SearchHit hit : hits) {
                numDocs++;
                String pfield = partitionField;
                Map<String, Object> sourceAsMap = hit.getSourceAsMap();
                if (pfield.startsWith("metadata.")) {
                    sourceAsMap = (Map<String, Object>) sourceAsMap.get("metadata");
                    pfield = pfield.substring(9);
                }
                Object key_as_object = sourceAsMap.get(pfield);
                if (key_as_object instanceof List) {
                    if (((List<String>) (key_as_object)).size() == 1)
                        key = ((List<String>) key_as_object).get(0);
                } else {
                    key = key_as_object.toString();
                }

                sortValues = hit.getSortValues();
                if (!addHitToBuffer(hit)) {
                    alreadyprocessed++;
                }
            }

            // no key if no results have been found
            if (key != null) {
                searchAfterCache.put(key, sortValues);
            }

            eventCounter.scope("OpenSearch_queries_host").incrBy(1);
            eventCounter.scope("OpenSearch_docs_host").incrBy(numDocs);
            eventCounter.scope("already_being_processed_host").incrBy(alreadyprocessed);

            LOG.info(
                    "{} OpenSearch term query returned {} hits  in {} msec with {} already being processed for {}",
                    logIdprefix,
                    numDocs,
                    response.getTook().getMillis(),
                    alreadyprocessed,
                    key);
        }

        @Override
        public void onFailure(Exception e) {
            LOG.error("Exception with OpenSearch query", e);
        }
    }
}
