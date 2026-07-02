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
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.stormcrawler.opensearch.Constants;
import org.apache.stormcrawler.persistence.EmptyQueueListener;
import org.apache.stormcrawler.util.ConfUtils;
import org.opensearch.client.json.JsonData;
import org.opensearch.client.opensearch._types.FieldValue;
import org.opensearch.client.opensearch._types.SortOrder;
import org.opensearch.client.opensearch.core.SearchRequest;
import org.opensearch.client.opensearch.core.SearchResponse;
import org.opensearch.client.opensearch.core.search.Hit;
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
            LOG.trace("{} isInquery true for {}", logIdprefix, queueName);
            return;
        }

        LOG.info("{} Querying for more docs for {}", logIdprefix, queueName);

        if (queryDate == null) {
            queryDate = new Date();
            lastTimeResetToNow = Instant.now();
        }

        String formattedQueryDate =
                Instant.ofEpochMilli(queryDate.getTime())
                        .atOffset(ZoneOffset.UTC)
                        .format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);

        SearchRequest.Builder requestBuilder =
                new SearchRequest.Builder()
                        .index(indexName)
                        .size(bufferReloadSize)
                        .trackTotalHits(t -> t.enabled(false))
                        .query(
                                q ->
                                        q.bool(
                                                b ->
                                                        b.filter(
                                                                        f ->
                                                                                f.range(
                                                                                        r ->
                                                                                                r.field(
                                                                                                                "nextFetchDate")
                                                                                                        .lte(
                                                                                                                JsonData
                                                                                                                        .of(
                                                                                                                                formattedQueryDate))))
                                                                .filter(
                                                                        f ->
                                                                                f.term(
                                                                                        t ->
                                                                                                t.field(
                                                                                                                partitionField)
                                                                                                        .value(
                                                                                                                FieldValue
                                                                                                                        .of(
                                                                                                                                queueName))))));

        // sort within a bucket
        for (String bsf : bucketSortField) {
            requestBuilder.sort(s -> s.field(fs -> fs.field(bsf).order(SortOrder.Asc)));
        }

        // do we have a search after for this one?
        Object[] searchAfterValues = searchAfterCache.getIfPresent(queueName);
        if (searchAfterValues != null) {
            for (Object sav : searchAfterValues) {
                requestBuilder.searchAfter(FieldValue.of(sav.toString()));
            }
        }

        // shard preference for routing
        if (shardID != -1) {
            requestBuilder.preference("_shards:" + shardID + "|_local");
        }

        SearchRequest request = requestBuilder.build();

        // dump query to log
        LOG.debug("{} OpenSearch query {} - {}", logIdprefix, queueName, request);

        CompletableFuture.supplyAsync(
                        () -> {
                            try {
                                return client.search(request, JsonData.class);
                            } catch (IOException e) {
                                throw new CompletionException(e);
                            }
                        },
                        queryExecutor)
                .thenAccept(hrl::handleResponse)
                .exceptionally(
                        e -> {
                            Throwable cause = e instanceof CompletionException ? e.getCause() : e;
                            LOG.error("Exception with OpenSearch query", cause);
                            return null;
                        });
    }

    /** Overrides the handling of responses for aggregations. */
    @Override
    protected void handleResponse(SearchResponse<JsonData> response) {
        // delete all entries from the searchAfterCache when
        // we get the results from the aggregation spouts
        searchAfterCache.invalidateAll();
        super.handleResponse(response);
    }

    /** The aggregation kindly told us where to start from. */
    @Override
    protected void sortValuesForKey(String key, Object[] sortValues) {
        if (sortValues != null && sortValues.length > 0) {
            this.searchAfterCache.put(key, sortValues);
        }
    }

    /** Handling of results for a specific queue. */
    class HostResultListener {

        /**
         * Handles the search response for a host-specific query, extracting hits and adding them to
         * the buffer.
         *
         * @param response the search response containing document hits
         */
        void handleResponse(SearchResponse<JsonData> response) {

            int alreadyprocessed = 0;
            int numDocs = 0;

            List<Hit<JsonData>> hits = response.hits().hits();

            Object[] sortValues = null;

            // retrieve the key for these results
            String key = null;

            for (Hit<JsonData> hit : hits) {
                numDocs++;

                Map<String, Object> source = sourceAsMap(hit.source());

                String pfield = partitionField;
                Map<String, Object> fieldSource = source;
                if (pfield.startsWith("metadata.")) {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> metadataMap = (Map<String, Object>) source.get("metadata");
                    fieldSource = metadataMap;
                    pfield = pfield.substring(9);
                }
                Object key_as_object = fieldSource.get(pfield);
                if (key_as_object instanceof List) {
                    @SuppressWarnings("unchecked")
                    List<String> keyList = (List<String>) key_as_object;
                    if (keyList.size() == 1) {
                        key = keyList.get(0);
                    }
                } else {
                    key = key_as_object.toString();
                }

                sortValues = hit.sort().toArray();
                if (!addHitToBuffer(source)) {
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
                    response.took(),
                    alreadyprocessed,
                    key);
        }
    }
}
