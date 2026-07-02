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

import java.time.Duration;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.TimeZone;
import org.apache.http.HttpHeaders;
import org.apache.stormcrawler.Constants;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.parse.filter.MD5SignatureParseFilter;
import org.apache.stormcrawler.util.ConfUtils;
import org.slf4j.LoggerFactory;

/**
 * Adaptive fetch scheduler, checks by signature comparison whether a re-fetched page has changed.
 *
 * <ul>
 *   <li>if yes, shrink the fetch interval up to a minimum fetch interval
 *   <li>if not, increase the fetch interval up to a maximum
 * </ul>
 *
 * <p>The rate how the fetch interval is incremented or decremented is configurable.
 *
 * <p>Note, that this scheduler requires the following metadata:
 *
 * <dl>
 *   <dt>signature
 *   <dd>page signature, filled by {@link MD5SignatureParseFilter}
 *   <dt>signatureOld
 *   <dd>(temporary) copy of the previous signature, optionally copied by {@link
 *       MD5SignatureParseFilter}
 *   <dt>fetch.statusCode
 *   <dt>
 *   <dd>HTTP response status code, required to handle &quot;HTTP 304 Not Modified&quot; responses
 * </dl>
 *
 * and writes the following metadata fields:
 *
 * <dl>
 *   <dt>fetchInterval
 *   <dd>current fetch interval
 *   <dt>signatureChangeDate
 *   <dd>date when the signature has changed (ISO-8601 date time format)
 *   <dt>last-modified
 *   <dd>last-modified time used to send If-Modified-Since HTTP requests, only written if <code>
 *       scheduler.adaptive.setLastModified</code> is true. Same date string as set in
 *       &quot;signatureChangeDate&quot;. Note that it is assumed that the metadata field
 *       `last-modified` is written only by the scheduler, in detail, the property
 *       `protocol.md.prefix` should not be empty to avoid that `last-modified` is filled with an
 *       incorrect or ill-formed date from the HTTP header.
 * </dl>
 *
 * <h2>Configuration</h2>
 *
 * <p>The following lines show how to configure the adaptive scheduler in the configuration file
 * (crawler-conf.yaml):
 *
 * <pre>
 * scheduler.class: "org.apache.stormcrawler.persistence.AdaptiveScheduler"
 * # set last-modified time ({@value HttpHeaders#LAST_MODIFIED})
 * # used in HTTP If-Modified-Since request header field
 * scheduler.adaptive.setLastModified: true
 * # min. interval in minutes (default: 1h)
 * scheduler.adaptive.fetchInterval.min: 60
 * # max. interval in minutes (default: 2 weeks)
 * scheduler.adaptive.fetchInterval.max: 20160
 * # increment and decrement rates (0.0 &lt; rate &lt;= 1.0)
 * scheduler.adaptive.fetchInterval.rate.incr: .5
 * scheduler.adaptive.fetchInterval.rate.decr: .5
 *
 * # required persisted metadata (in addition to other persisted metadata):
 * metadata.persist:
 *  - ...
 *  - signature
 *  - fetch.statusCode
 *  - fetchInterval
 *  - last-modified
 * # - signatureOld
 * # - signatureChangeDate
 * # Note: "signatureOld" and "signatureChangeDate" are optional, the adaptive
 * # scheduler will also work if both are temporarily passed and not persisted.
 * </pre>
 *
 * <p>To generate the signature and keep a copy of the last signature, the parse filters should be
 * configured accordingly:
 *
 * <pre>
 * "org.apache.stormcrawler.parse.ParseFilters": [
 *   ...,
 *   {
 *     "class": "org.apache.stormcrawler.parse.filter.MD5SignatureParseFilter",
 *     "name": "MD5Digest",
 *     "params": {
 *       "useText": "false",
 *       "keyName": "signature",
 *       "keyNameCopy": "signatureOld"
 *     }
 *   }
 * </pre>
 *
 * <p>The order is mandatory: first copy the old signature, than generate the current one.
 */
public class AdaptiveScheduler extends DefaultScheduler {

    /**
     * Configuration property (boolean) whether or not to set the &quot;last-modified&quot; metadata
     * field when a page change was detected by signature comparison.
     */
    public static final String SET_LAST_MODIFIED = "scheduler.adaptive.setLastModified";

    /** Configuration property (int) to set the minimum fetch interval in minutes. */
    public static final String INTERVAL_MIN = "scheduler.adaptive.fetchInterval.min";

    /** Configuration property (int) to set the maximum fetch interval in minutes. */
    public static final String INTERVAL_MAX = "scheduler.adaptive.fetchInterval.max";

    /**
     * Configuration property (float) to set the increment rate. If a page hasn't changed when
     * refetched, the fetch interval is multiplied by (1.0 + incr_rate) until the max. fetch
     * interval is reached.
     */
    public static final String INTERVAL_INC_RATE = "scheduler.adaptive.fetchInterval.rate.incr";

    /**
     * Configuration property (float) to set the decrement rate. If a page has changed when
     * refetched, the fetch interval is multiplied by (1.0 - decr_rate). If the fetch interval comes
     * closer to the minimum interval, the decrementing is slowed down.
     */
    public static final String INTERVAL_DEC_RATE = "scheduler.adaptive.fetchInterval.rate.decr";

    /**
     * Name of the signature key in metadata, must be defined as &quot;keyName&quot; in the
     * configuration of {@link org.apache.stormcrawler.parse.filter.MD5SignatureParseFilter} . This
     * key must be listed in &quot;metadata.persist&quot;.
     */
    public static final String SIGNATURE_KEY = "signature";

    /**
     * Name of key to hold previous signature: a copy, not overwritten by {@link
     * MD5SignatureParseFilter}. This key is a temporary copy, not necessarily persisted in
     * metadata.
     */
    public static final String SIGNATURE_OLD_KEY = "signatureOld";

    /**
     * Key to store the current fetch interval value, must be listed in
     * &quot;metadata.persist&quot;.
     */
    public static final String FETCH_INTERVAL_KEY = "fetchInterval";

    /**
     * Key to store the date when the signature has been changed, must be listed in
     * &quot;metadata.persist&quot;.
     */
    public static final String SIGNATURE_MODIFIED_KEY = "signatureChangeDate";

    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(AdaptiveScheduler.class);

    protected int defaultfetchInterval;
    protected int minFetchInterval = 60;
    protected int maxFetchInterval = 60 * 24 * 14;
    protected float fetchIntervalDecRate = .5f;
    protected float fetchIntervalIncRate = .5f;

    protected boolean setLastModified = false;
    protected boolean overwriteLastModified = false;

    @Override
    public void init(Map<String, Object> stormConf) {
        defaultfetchInterval =
                ConfUtils.getInt(stormConf, Constants.defaultFetchIntervalParamName, 1440);
        setLastModified = ConfUtils.getBoolean(stormConf, SET_LAST_MODIFIED, false);
        minFetchInterval = ConfUtils.getInt(stormConf, INTERVAL_MIN, minFetchInterval);
        maxFetchInterval = ConfUtils.getInt(stormConf, INTERVAL_MAX, maxFetchInterval);
        fetchIntervalDecRate =
                ConfUtils.getFloat(stormConf, INTERVAL_DEC_RATE, fetchIntervalDecRate);
        fetchIntervalIncRate =
                ConfUtils.getFloat(stormConf, INTERVAL_INC_RATE, fetchIntervalIncRate);
        super.init(stormConf);
    }

    @Override
    public Optional<Date> schedule(Status status, Metadata metadata) {
        LOG.debug("Scheduling status: {}, metadata: {}", status, metadata);

        String signature = metadata.getFirstValue(SIGNATURE_KEY);
        String oldSignature = metadata.getFirstValue(SIGNATURE_OLD_KEY);

        if (status != Status.FETCHED) {

            // reset all metadata
            metadata.remove(SIGNATURE_MODIFIED_KEY);
            metadata.remove(FETCH_INTERVAL_KEY);
            metadata.remove(SIGNATURE_KEY);
            metadata.remove(SIGNATURE_OLD_KEY);

            if (status == Status.ERROR) {
                /*
                 * remove last-modified for permanent errors so that no
                 * if-modified-since request is sent: the content is needed
                 * again to be parsed and index
                 */
                metadata.remove(HttpHeaders.LAST_MODIFIED);
            }

            // fall-back to DefaultScheduler
            return super.schedule(status, metadata);
        }

        Calendar now = Calendar.getInstance(TimeZone.getTimeZone("GMT"), Locale.ROOT);

        String signatureModified = metadata.getFirstValue(SIGNATURE_MODIFIED_KEY);

        boolean changed = false;

        final String modifiedTimeString = now.toInstant().toString();

        if (metadata.getFirstValue("fetch.statusCode").equals("304")) {
            // HTTP 304 Not Modified
            // - no new signature calculated because no content fetched
            // - do not compare persisted signatures
            // - leave last-modified time unchanged
        } else if (signature == null || oldSignature == null) {
            // no decision possible by signature comparison if
            // - document not parsed (intentionally or not) or
            // - signature not generated or
            // - old signature not copied
            // fall-back to DefaultScheduler
            LOG.debug("No signature for FETCHED page: {}", metadata);
            if (setLastModified && signature != null) {
                // set last-modified time for first fetch
                metadata.setValue(HttpHeaders.LAST_MODIFIED, modifiedTimeString);
            }
            Optional<Date> nextFetch = super.schedule(status, metadata);
            if (nextFetch.isPresent()) {
                long fetchIntervalMinutes =
                        Duration.between(now.toInstant(), nextFetch.get().toInstant()).toMinutes();
                metadata.setValue(FETCH_INTERVAL_KEY, Long.toString(fetchIntervalMinutes));
            }
            return nextFetch;
        } else if (signature.equals(oldSignature)) {
            // unchanged
        } else {
            // change detected by signature comparison
            changed = true;
            signatureModified = modifiedTimeString;
            if (setLastModified) {
                metadata.setValue(HttpHeaders.LAST_MODIFIED, modifiedTimeString);
            }
        }

        String fetchInterval = metadata.getFirstValue(FETCH_INTERVAL_KEY);
        int interval;
        if (fetchInterval != null) {
            interval = Integer.parseInt(fetchInterval);
        } else {
            // initialize from DefaultScheduler
            Optional<Integer> customInterval = super.checkCustomInterval(metadata, status);
            if (customInterval.isPresent()) {
                interval = customInterval.get();
            } else {
                interval = defaultfetchInterval;
            }
            fetchInterval = Integer.toString(interval);
        }

        if (changed) {
            // shrink fetch interval (slow down decrementing if already close to
            // the minimum interval)
            interval =
                    (int)
                            ((1.0f - fetchIntervalDecRate) * interval
                                    + fetchIntervalDecRate * minFetchInterval);
            LOG.debug(
                    "Signature has changed, fetchInterval decreased from {} to {}",
                    fetchInterval,
                    interval);

        } else {
            // no change or not modified, increase fetch interval
            interval = (int) (interval * (1.0f + fetchIntervalIncRate));
            if (interval > maxFetchInterval) {
                interval = maxFetchInterval;
            }
            LOG.debug("Unchanged, fetchInterval increased from {} to {}", fetchInterval, interval);
            // remove old signature (do not keep same signature twice)
            metadata.remove(SIGNATURE_OLD_KEY);
            if (signatureModified == null) {
                signatureModified = modifiedTimeString;
            }
        }

        metadata.setValue(FETCH_INTERVAL_KEY, Integer.toString(interval));
        metadata.setValue(SIGNATURE_MODIFIED_KEY, signatureModified);

        now.add(Calendar.MINUTE, interval);

        return Optional.of(now.getTime());
    }
}
