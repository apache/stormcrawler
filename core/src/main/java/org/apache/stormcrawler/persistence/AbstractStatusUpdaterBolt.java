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

package org.apache.stormcrawler.persistence;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.storm.metric.api.IMetric;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.stormcrawler.Constants;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.util.ConfUtils;
import org.apache.stormcrawler.util.MetadataTransfer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract bolt used to store the status of URLs. Uses the DefaultScheduler and MetadataTransfer.
 */
public abstract class AbstractStatusUpdaterBolt extends BaseRichBolt {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractStatusUpdaterBolt.class);

    /**
     * Parameter name to indicate whether the internal cache should be used for discovered URLs. The
     * value of the parameter is a boolean - true by default.
     */
    public static String useCacheParamName = "status.updater.use.cache";

    /** Number of successive FETCH_ERROR before status changes to ERROR. */
    public static String maxFetchErrorsParamName = "max.fetch.errors";

    /**
     * Parameter name to configure the cache. @see http://docs.guava-libraries.googlecode
     * .com/git/javadoc/com/google/common/cache/CacheBuilderSpec.html Default value is
     * "maximumSize=10000,expireAfterAccess=1h".
     */
    public static String cacheConfigParamName = "status.updater.cache.spec";

    /**
     * Used for rounding nextFetchDates. Values are hour, minute or second, the latter is the
     * default value.
     */
    public static String roundDateParamName = "status.updater.unit.round.date";

    /**
     * Key used to pass a preset Date to use as nextFetchDate. The value must represent a valid
     * instant in UTC and be parsable using {@link DateTimeFormatter#ISO_INSTANT}. This also
     * indicates that the storage can be done directly on the metadata as-is.
     */
    public static final String AS_IS_NEXTFETCHDATE_METADATA =
            "status.store.as.is.with.nextfetchdate";

    protected OutputCollector collector;

    private Scheduler scheduler;
    private MetadataTransfer mdTransfer;

    private Cache<Object, Object> cache;
    private boolean useCache = true;

    private int maxFetchErrors = 3;

    private long cacheHits = 0;
    private long cacheMisses = 0;

    private int roundDateUnit = Calendar.SECOND;

    @Override
    public void prepare(
            Map<String, Object> stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;

        scheduler = Scheduler.getInstance(stormConf);

        mdTransfer = MetadataTransfer.getInstance(stormConf);

        useCache = ConfUtils.getBoolean(stormConf, useCacheParamName, true);

        if (useCache) {
            String spec = ConfUtils.getString(stormConf, cacheConfigParamName);
            cache = Caffeine.from(spec).build();

            context.registerMetric(
                    "cache",
                    new IMetric() {
                        @Override
                        public Object getValueAndReset() {
                            Map<String, Long> statsMap = new HashMap<>();
                            statsMap.put("hits", cacheHits);
                            statsMap.put("misses", cacheMisses);
                            statsMap.put("size", cache.estimatedSize());
                            cacheHits = 0;
                            cacheMisses = 0;
                            return statsMap;
                        }
                    },
                    30);
        }

        maxFetchErrors = ConfUtils.getInt(stormConf, maxFetchErrorsParamName, 3);

        String tmpdateround = ConfUtils.getString(stormConf, roundDateParamName, "SECOND");
        if (tmpdateround.equalsIgnoreCase("MINUTE")) {
            roundDateUnit = Calendar.MINUTE;
        } else if (tmpdateround.equalsIgnoreCase("HOUR")) {
            roundDateUnit = Calendar.HOUR;
        }
    }

    @Override
    public void execute(Tuple tuple) {

        String url = tuple.getStringByField("url");
        Status status = (Status) tuple.getValueByField("status");

        boolean potentiallyNew = status.equals(Status.DISCOVERED);

        // if the URL is a freshly discovered one
        // check whether it is already known in the cache
        // if so we've already seen it and don't need to
        // store it again
        if (potentiallyNew && useCache) {
            if (cache.getIfPresent(url) != null) {
                // no need to add it to the queue
                LOG.debug("URL {} already in cache", url);
                cacheHits++;
                collector.ack(tuple);
                return;
            } else {
                LOG.debug("URL {} not in cache", url);
                cacheMisses++;
            }
        }

        Metadata metadata = (Metadata) tuple.getValueByField("metadata");

        // store directly with the date specified in the metadata without
        // changing the status or scheduling.
        String dateInMetadata = metadata.getFirstValue(AS_IS_NEXTFETCHDATE_METADATA);
        if (dateInMetadata != null) {
            Date nextFetch = Date.from(Instant.parse(dateInMetadata));
            try {
                store(url, status, mdTransfer.filter(metadata), Optional.of(nextFetch), tuple);
                return;
            } catch (Exception e) {
                LOG.error("Exception caught when storing", e);
                collector.fail(tuple);
                return;
            }
        }

        // store last processed or discovery date in UTC
        final String nowAsString = Instant.now().toString();
        if (status.equals(Status.DISCOVERED)) {
            metadata.setValue("discoveryDate", nowAsString);
        } else {
            metadata.setValue("lastProcessedDate", nowAsString);
        }

        // too many fetch errors?
        if (status.equals(Status.FETCH_ERROR)) {
            String errorCount = metadata.getFirstValue(Constants.fetchErrorCountParamName);
            int count = 0;
            try {
                count = Integer.parseInt(errorCount);
            } catch (NumberFormatException e) {
            }
            count++;
            if (count >= maxFetchErrors) {
                status = Status.ERROR;
                metadata.setValue(Constants.STATUS_ERROR_CAUSE, "maxFetchErrors");
            } else {
                metadata.setValue(Constants.fetchErrorCountParamName, Integer.toString(count));
            }
        }

        // delete any existing error count metadata
        // e.g. status changed
        if (!status.equals(Status.FETCH_ERROR)) {
            metadata.remove(Constants.fetchErrorCountParamName);
        }
        // https://github.com/apache/stormcrawler/issues/415
        // remove error related key values in case of success
        if (status.equals(Status.FETCHED) || status.equals(Status.REDIRECTION)) {
            metadata.remove(Constants.STATUS_ERROR_CAUSE);
            metadata.remove(Constants.STATUS_ERROR_MESSAGE);
            metadata.remove(Constants.STATUS_ERROR_SOURCE);
        } else if (status == Status.ERROR) {
            // gone? notify any deleters. Doesn't need to be anchored
            collector.emit(Constants.DELETION_STREAM_NAME, new Values(url, metadata));
        }

        // determine the value of the next fetch based on the status
        Optional<Date> nextFetch = scheduler.schedule(status, metadata);

        // filter metadata just before storing it, so that non-persisted
        // metadata is available to fetch schedulers
        metadata = mdTransfer.filter(metadata);

        // round next fetch date - unless it is never
        if (nextFetch.isPresent()) {
            nextFetch = Optional.of(DateUtils.round(nextFetch.get(), this.roundDateUnit));
        }

        // extensions of this class will handle the storage
        // on a per document basis
        try {
            store(url, status, metadata, nextFetch, tuple);
        } catch (Exception e) {
            LOG.error("Exception caught when storing", e);
            collector.fail(tuple);
        }
    }

    /**
     * Get the document id.
     *
     * @param metadata The {@link Metadata}.
     * @param url The normalised url.
     * @return Return the normalised url SHA-256 digest as String.
     */
    protected String getDocumentID(Metadata metadata, String url) {
        return org.apache.commons.codec.digest.DigestUtils.sha256Hex(url);
    }

    /** Must be called by extending classes to store and collect in one go. */
    protected final void ack(Tuple t, String url) {
        // keep the URL in the cache
        if (useCache) {
            cache.put(url, "");
        }

        collector.ack(t);
    }

    protected abstract void store(
            String url, Status status, Metadata metadata, Optional<Date> nextFetch, Tuple t)
            throws Exception;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(Constants.DELETION_STREAM_NAME, new Fields("url", "metadata"));
    }
}
