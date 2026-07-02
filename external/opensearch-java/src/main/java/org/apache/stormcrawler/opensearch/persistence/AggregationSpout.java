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
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import org.apache.commons.lang3.StringUtils;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.opensearch.Constants;
import org.apache.stormcrawler.util.ConfUtils;
import org.opensearch.client.json.JsonData;
import org.opensearch.client.opensearch._types.FieldValue;
import org.opensearch.client.opensearch._types.SortOrder;
import org.opensearch.client.opensearch._types.aggregations.Aggregate;
import org.opensearch.client.opensearch._types.aggregations.Aggregation;
import org.opensearch.client.opensearch._types.aggregations.StringTermsBucket;
import org.opensearch.client.opensearch._types.aggregations.TopHitsAggregate;
import org.opensearch.client.opensearch.core.SearchRequest;
import org.opensearch.client.opensearch.core.SearchResponse;
import org.opensearch.client.opensearch.core.search.Hit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Spout which pulls URL from an OpenSearch index. Use a single instance unless you use
 * 'opensearch.status.routing' with the StatusUpdaterBolt, in which case you need to have exactly
 * the same number of spout instances as OpenSearch shards. Guarantees a good mix of URLs by
 * aggregating them by an arbitrary field e.g. key.
 */
public class AggregationSpout extends AbstractSpout {

    private static final Logger LOG = LoggerFactory.getLogger(AggregationSpout.class);

    private static final String StatusSampleParamName = Constants.PARAMPREFIX + "status.sample";
    private static final String MostRecentDateIncreaseParamName =
            Constants.PARAMPREFIX + "status.recentDate.increase";
    private static final String MostRecentDateMinGapParamName =
            Constants.PARAMPREFIX + "status.recentDate.min.gap";

    private boolean sample = false;

    private int recentDateIncrease = -1;
    private int recentDateMinGap = -1;

    protected Set<String> currentBuckets;

    @Override
    public void open(
            Map<String, Object> stormConf,
            TopologyContext context,
            SpoutOutputCollector collector) {
        sample = ConfUtils.getBoolean(stormConf, StatusSampleParamName, sample);
        recentDateIncrease =
                ConfUtils.getInt(stormConf, MostRecentDateIncreaseParamName, recentDateIncrease);
        recentDateMinGap =
                ConfUtils.getInt(stormConf, MostRecentDateMinGapParamName, recentDateMinGap);
        super.open(stormConf, context, collector);
        currentBuckets = new HashSet<>();
    }

    @Override
    protected void populateBuffer() {

        if (queryDate == null) {
            queryDate = new Date();
            lastTimeResetToNow = Instant.now();
        }

        String formattedQueryDate =
                Instant.ofEpochMilli(queryDate.getTime())
                        .atOffset(ZoneOffset.UTC)
                        .format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);

        LOG.info("{} Populating buffer with nextFetchDate <= {}", logIdprefix, formattedQueryDate);

        // Build the top_hits sub-aggregation
        Aggregation topHitsAgg =
                Aggregation.of(
                        a ->
                                a.topHits(
                                        th -> {
                                            th.size(maxURLsPerBucket).explain(false);
                                            for (String bsf : bucketSortField) {
                                                th.sort(
                                                        s ->
                                                                s.field(
                                                                        fs ->
                                                                                fs.field(bsf)
                                                                                        .order(
                                                                                                SortOrder
                                                                                                        .Asc)));
                                            }
                                            return th;
                                        }));

        // Build the terms (partition) aggregation with top_hits sub-agg
        Aggregation.Builder.ContainerBuilder partitionAggBuilder =
                new Aggregation.Builder()
                        .terms(
                                t -> {
                                    t.field(partitionField).size(maxBucketNum);
                                    // sort between buckets by the min sub-aggregation
                                    if (StringUtils.isNotBlank(totalSortField)) {
                                        t.order(
                                                Collections.singletonList(
                                                        Collections.singletonMap(
                                                                "top_hit", SortOrder.Asc)));
                                    }
                                    return t;
                                })
                        .aggregations("docs", topHitsAgg);

        // add the min sub-aggregation used for sorting between buckets
        if (StringUtils.isNotBlank(totalSortField)) {
            partitionAggBuilder.aggregations(
                    "top_hit", Aggregation.of(minAgg -> minAgg.min(m -> m.field(totalSortField))));
        }

        Aggregation partitionAgg = partitionAggBuilder.build();

        // Build the search request
        SearchRequest.Builder requestBuilder =
                new SearchRequest.Builder()
                        .index(indexName)
                        .size(0)
                        .trackTotalHits(t -> t.enabled(false))
                        .query(
                                q ->
                                        q.bool(
                                                b -> {
                                                    b.filter(
                                                            f ->
                                                                    f.range(
                                                                            r ->
                                                                                    r.field(
                                                                                                    "nextFetchDate")
                                                                                            .lte(
                                                                                                    JsonData
                                                                                                            .of(
                                                                                                                    formattedQueryDate))));
                                                    if (filterQueries != null) {
                                                        for (String fq : filterQueries) {
                                                            b.filter(
                                                                    f ->
                                                                            f.queryString(
                                                                                    qs ->
                                                                                            qs
                                                                                                    .query(
                                                                                                            fq)));
                                                        }
                                                    }
                                                    return b;
                                                }));

        if (queryTimeout != -1) {
            requestBuilder.timeout(queryTimeout + "s");
        }

        if (sample) {
            // Wrap in a diversified sampler aggregation
            requestBuilder.aggregations(
                    "sample",
                    Aggregation.of(
                            a ->
                                    a.diversifiedSampler(
                                                    ds ->
                                                            ds.field(partitionField)
                                                                    .maxDocsPerValue(
                                                                            maxURLsPerBucket)
                                                                    .shardSize(
                                                                            maxURLsPerBucket
                                                                                    * maxBucketNum))
                                            .aggregations("partition", partitionAgg)));
        } else {
            requestBuilder.aggregations("partition", partitionAgg);
        }

        // shard preference for routing
        if (shardID != -1) {
            requestBuilder.preference("_shards:" + shardID + "|_local");
        }

        SearchRequest request = requestBuilder.build();

        // dump query to log
        LOG.debug("{} OpenSearch query {}", logIdprefix, request);

        LOG.trace("{} isInQuery set to true", logIdprefix);
        isInQuery.set(true);

        CompletableFuture.supplyAsync(
                        () -> {
                            try {
                                return client.search(request, JsonData.class);
                            } catch (IOException e) {
                                throw new CompletionException(e);
                            }
                        },
                        queryExecutor)
                .thenAccept(this::handleResponse)
                .exceptionally(
                        e -> {
                            Throwable cause = e instanceof CompletionException ? e.getCause() : e;
                            LOG.error("{} Exception with OpenSearch query", logIdprefix, cause);
                            markQueryReceivedNow();
                            return null;
                        });
    }

    /**
     * Handles the search response from an asynchronous aggregation query, extracting URLs from term
     * buckets and adding them to the buffer.
     *
     * @param response the search response containing aggregation results
     */
    protected void handleResponse(SearchResponse<JsonData> response) {
        long timeTaken = System.currentTimeMillis() - getTimeLastQuerySent();

        Map<String, Aggregate> aggregs = response.aggregations();

        if (aggregs == null || aggregs.isEmpty()) {
            markQueryReceivedNow();
            return;
        }

        // Unwrap the sample aggregation if present
        Aggregate sampleAgg = aggregs.get("sample");
        if (sampleAgg != null) {
            aggregs = sampleAgg.sampler().aggregations();
        }

        Aggregate partitionAgg = aggregs.get("partition");
        List<StringTermsBucket> buckets = partitionAgg.sterms().buckets().array();

        int numhits = 0;
        int numBuckets = 0;
        int alreadyprocessed = 0;

        Instant mostRecentDateFound = null;

        currentBuckets.clear();

        // For each entry
        Iterator<StringTermsBucket> iterator = buckets.iterator();
        while (iterator.hasNext()) {
            StringTermsBucket entry = iterator.next();
            String key = entry.key(); // bucket key

            currentBuckets.add(key);

            long docCount = entry.docCount(); // Doc count

            int hitsForThisBucket = 0;

            List<FieldValue> lastSortValues = null;
            // filter results so that we don't include URLs we are already
            // being processed
            TopHitsAggregate topHits = entry.aggregations().get("docs").topHits();
            for (Hit<JsonData> hit : topHits.hits().hits()) {

                Map<String, Object> keyValues = sourceAsMap(hit.source());

                LOG.debug("{} -> id [{}], _source [{}]", logIdprefix, hit.id(), keyValues);

                hitsForThisBucket++;

                lastSortValues = hit.sort();

                String url = (String) keyValues.get("url");

                // consider only the first document of the last bucket
                // for optimising the nextFetchDate
                if (hitsForThisBucket == 1 && !iterator.hasNext()) {
                    String strDate = (String) keyValues.get("nextFetchDate");
                    try {
                        mostRecentDateFound = Instant.parse(strDate);
                    } catch (Exception e) {
                        throw new RuntimeException("can't parse date :" + strDate);
                    }
                }

                // is already being processed or in buffer - skip it!
                if (beingProcessed.containsKey(url)) {
                    LOG.debug("{} -> already processed: {}", logIdprefix, url);
                    alreadyprocessed++;
                    continue;
                }

                Metadata metadata = fromKeyValues(keyValues);
                boolean added = buffer.add(url, metadata);
                if (!added) {
                    LOG.debug("{} -> already in buffer: {}", logIdprefix, url);
                    alreadyprocessed++;
                    continue;
                }
                LOG.debug("{} -> added to buffer : {}", logIdprefix, url);
            }

            if (lastSortValues != null && !lastSortValues.isEmpty()) {
                sortValuesForKey(key, lastSortValues.toArray());
            }

            if (hitsForThisBucket > 0) {
                numBuckets++;
            }

            numhits += hitsForThisBucket;

            LOG.debug(
                    "{} key [{}], hits[{}], doc_count [{}], already_processed [{}]",
                    logIdprefix,
                    key,
                    hitsForThisBucket,
                    docCount,
                    alreadyprocessed);
        }

        LOG.info(
                "{} OpenSearch query returned {} hits from {} buckets in {} msec with {} already being processed. Took {} msec per doc on average.",
                logIdprefix,
                numhits,
                numBuckets,
                timeTaken,
                alreadyprocessed,
                ((float) timeTaken / numhits));

        queryTimes.accept(timeTaken);
        eventCounter.scope("already_being_processed").incrBy(alreadyprocessed);
        eventCounter.scope("OpenSearch_queries").incrBy(1);
        eventCounter.scope("OpenSearch_docs").incrBy(numhits);

        // optimise the nextFetchDate by getting the most recent value
        // returned in the query and add to it, unless the previous value is
        // within n mins in which case we'll keep it
        if (mostRecentDateFound != null && recentDateIncrease >= 0) {
            Calendar potentialNewDate =
                    Calendar.getInstance(TimeZone.getTimeZone("GMT"), Locale.ROOT);
            potentialNewDate.setTimeInMillis(mostRecentDateFound.toEpochMilli());
            potentialNewDate.add(Calendar.MINUTE, recentDateIncrease);
            Date oldDate = null;
            // check boundaries
            if (this.recentDateMinGap > 0) {
                Calendar low = Calendar.getInstance(TimeZone.getTimeZone("GMT"), Locale.ROOT);
                low.setTime(queryDate);
                low.add(Calendar.MINUTE, -recentDateMinGap);
                Calendar high = Calendar.getInstance(TimeZone.getTimeZone("GMT"), Locale.ROOT);
                high.setTime(queryDate);
                high.add(Calendar.MINUTE, recentDateMinGap);
                if (high.before(potentialNewDate) || low.after(potentialNewDate)) {
                    oldDate = queryDate;
                }
            } else {
                oldDate = queryDate;
            }
            if (oldDate != null) {
                queryDate = potentialNewDate.getTime();
                LOG.info(
                        "{} queryDate changed from {} to {} based on mostRecentDateFound {}",
                        logIdprefix,
                        oldDate,
                        queryDate,
                        mostRecentDateFound);
            } else {
                LOG.info(
                        "{} queryDate kept at {} based on mostRecentDateFound {}",
                        logIdprefix,
                        queryDate,
                        mostRecentDateFound);
            }
        }

        // reset the value for next fetch date if the previous one is too old
        if (resetFetchDateAfterNSecs != -1) {
            Instant changeNeededOn =
                    Instant.ofEpochMilli(
                            lastTimeResetToNow.toEpochMilli() + (resetFetchDateAfterNSecs * 1000L));
            if (Instant.now().isAfter(changeNeededOn)) {
                LOG.info(
                        "{} queryDate set to null based on resetFetchDateAfterNSecs {}",
                        logIdprefix,
                        resetFetchDateAfterNSecs);
                queryDate = null;
            }
        }

        // change the date if we don't get any results at all
        if (numBuckets == 0) {
            queryDate = null;
        }

        // remove lock
        markQueryReceivedNow();
    }

    protected void sortValuesForKey(String key, Object[] sortValues) {}
}
