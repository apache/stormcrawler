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

import java.time.Instant;
import java.util.Calendar;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.opensearch.Constants;
import org.apache.stormcrawler.util.ConfUtils;
import org.joda.time.format.ISODateTimeFormat;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.SearchHit;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.aggregations.Aggregations;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.bucket.SingleBucketAggregation;
import org.opensearch.search.aggregations.bucket.sampler.DiversifiedAggregationBuilder;
import org.opensearch.search.aggregations.bucket.terms.Terms;
import org.opensearch.search.aggregations.bucket.terms.Terms.Bucket;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.opensearch.search.aggregations.metrics.TopHits;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.sort.FieldSortBuilder;
import org.opensearch.search.sort.SortBuilders;
import org.opensearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Spout which pulls URL from an OpenSearch index. Use a single instance unless you use
 * 'opensearch.status.routing' with the StatusUpdaterBolt, in which case you need to have exactly
 * the same number of spout instances as OpenSearch shards. Guarantees a good mix of URLs by
 * aggregating them by an arbitrary field e.g. key.
 */
public class AggregationSpout extends AbstractSpout implements ActionListener<SearchResponse> {

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

        String formattedQueryDate = ISODateTimeFormat.dateTimeNoMillis().print(queryDate.getTime());

        LOG.info("{} Populating buffer with nextFetchDate <= {}", logIdprefix, formattedQueryDate);

        BoolQueryBuilder queryBuilder =
                boolQuery()
                        .filter(QueryBuilders.rangeQuery("nextFetchDate").lte(formattedQueryDate));

        if (filterQueries != null) {
            for (String filterQuery : filterQueries) {
                queryBuilder.filter(QueryBuilders.queryStringQuery(filterQuery));
            }
        }

        SearchRequest request = new SearchRequest(indexName);

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(queryBuilder);
        sourceBuilder.from(0);
        sourceBuilder.size(0);
        sourceBuilder.explain(false);
        sourceBuilder.trackTotalHits(false);

        if (queryTimeout != -1) {
            sourceBuilder.timeout(
                    new org.opensearch.common.unit.TimeValue(queryTimeout, TimeUnit.SECONDS));
        }

        TermsAggregationBuilder aggregations =
                AggregationBuilders.terms("partition").field(partitionField).size(maxBucketNum);

        org.opensearch.search.aggregations.metrics.TopHitsAggregationBuilder tophits =
                AggregationBuilders.topHits("docs").size(maxURLsPerBucket).explain(false);

        // sort within a bucket
        for (String bsf : bucketSortField) {
            FieldSortBuilder sorter = SortBuilders.fieldSort(bsf).order(SortOrder.ASC);
            tophits.sort(sorter);
        }

        aggregations.subAggregation(tophits);

        // sort between buckets
        if (StringUtils.isNotBlank(totalSortField)) {
            org.opensearch.search.aggregations.metrics.MinAggregationBuilder minBuilder =
                    AggregationBuilders.min("top_hit").field(totalSortField);
            aggregations.subAggregation(minBuilder);
            aggregations.order(BucketOrder.aggregation("top_hit", true));
        }

        if (sample) {
            DiversifiedAggregationBuilder sab = new DiversifiedAggregationBuilder("sample");
            sab.field(partitionField).maxDocsPerValue(maxURLsPerBucket);
            sab.shardSize(maxURLsPerBucket * maxBucketNum);
            sab.subAggregation(aggregations);
            sourceBuilder.aggregation(sab);
        } else {
            sourceBuilder.aggregation(aggregations);
        }

        request.source(sourceBuilder);

        // https://www.elastic.co/guide/en/elasticsearch/reference/current/search-request-preference.html
        // _shards:2,3
        // specific shard but ideally a local copy of it
        if (shardID != -1) {
            request.preference("_shards:" + shardID + "|_local");
        }

        // dump query to log
        LOG.debug("{} OpenSearch query {}", logIdprefix, request);

        LOG.trace("{} isInquery set to true");
        isInQuery.set(true);
        client.searchAsync(request, RequestOptions.DEFAULT, this);
    }

    @Override
    public void onFailure(Exception arg0) {
        LOG.error("{} Exception with OpenSearch query", logIdprefix, arg0);
        markQueryReceivedNow();
    }

    @Override
    public void onResponse(SearchResponse response) {
        long timeTaken = System.currentTimeMillis() - getTimeLastQuerySent();

        Aggregations aggregs = response.getAggregations();

        if (aggregs == null) {
            markQueryReceivedNow();
            return;
        }

        SingleBucketAggregation sample = aggregs.get("sample");
        if (sample != null) {
            aggregs = sample.getAggregations();
        }

        Terms agg = aggregs.get("partition");

        int numhits = 0;
        int numBuckets = 0;
        int alreadyprocessed = 0;

        Instant mostRecentDateFound = null;

        currentBuckets.clear();

        // For each entry
        Iterator<Terms.Bucket> iterator = (Iterator<Bucket>) agg.getBuckets().iterator();
        while (iterator.hasNext()) {
            Terms.Bucket entry = iterator.next();
            String key = (String) entry.getKey(); // bucket key

            currentBuckets.add(key);

            long docCount = entry.getDocCount(); // Doc count

            int hitsForThisBucket = 0;

            SearchHit lastHit = null;

            // filter results so that we don't include URLs we are already
            // being processed
            TopHits topHits = entry.getAggregations().get("docs");
            for (SearchHit hit : topHits.getHits().getHits()) {

                LOG.debug(
                        "{} -> id [{}], _source [{}]",
                        logIdprefix,
                        hit.getId(),
                        hit.getSourceAsString());

                hitsForThisBucket++;

                lastHit = hit;

                Map<String, Object> keyValues = hit.getSourceAsMap();
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

            if (lastHit != null) {
                sortValuesForKey(key, lastHit.getSortValues());
            }

            if (hitsForThisBucket > 0) numBuckets++;

            numhits += hitsForThisBucket;

            LOG.debug(
                    "{} key [{}], hits[{}], doc_count [{}]",
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

        queryTimes.addMeasurement(timeTaken);
        eventCounter.scope("already_being_processed").incrBy(alreadyprocessed);
        eventCounter.scope("ES_queries").incrBy(1);
        eventCounter.scope("ES_docs").incrBy(numhits);

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
