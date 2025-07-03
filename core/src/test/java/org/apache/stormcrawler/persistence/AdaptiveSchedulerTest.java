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

import java.net.MalformedURLException;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.TimeZone;
import org.apache.commons.lang.time.DateUtils;
import org.apache.http.HttpHeaders;
import org.apache.stormcrawler.Metadata;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class AdaptiveSchedulerTest {

    private static String md5sumEmptyContent = "d41d8cd98f00b204e9800998ecf8427e";

    private static String md5sumSpaceContent = "7215ee9c7d9dc229d2921a40e899ec5f";

    private static Map<String, Object> getConf() {
        Map<String, Object> stormConf = new HashMap<>();
        stormConf.put("fetchInterval.FETCHED.testKey=someValue", 6);
        stormConf.put("fetchInterval.testKey=someValue", 8);
        stormConf.put("scheduler.adaptive.setLastModified", true);
        stormConf.put("scheduler.adaptive.fetchInterval.min", 2);
        stormConf.put("fetchInterval.default", 5);
        stormConf.put("scheduler.adaptive.fetchInterval.max", 10);
        stormConf.put("protocol.md.prefix", "protocol.");
        return stormConf;
    }

    /**
     * Verify setting the initial fetch interval by metadata and fetch status implemented in
     * DefaultScheduler
     */
    @Test
    void testSchedulerInitialInterval() throws MalformedURLException {
        Scheduler scheduler = new AdaptiveScheduler();
        scheduler.init(getConf());
        Metadata metadata = new Metadata();
        metadata.addValue("testKey", "someValue");
        metadata.addValue("fetch.statusCode", "200");
        Optional<Date> nextFetch = scheduler.schedule(Status.FETCHED, metadata);
        Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("GMT"), Locale.ROOT);
        cal.add(Calendar.MINUTE, 6);
        Assertions.assertEquals(
                DateUtils.round(cal.getTime(), Calendar.SECOND),
                DateUtils.round(nextFetch.get(), Calendar.SECOND));
        nextFetch = scheduler.schedule(Status.ERROR, metadata);
        cal = Calendar.getInstance(TimeZone.getTimeZone("GMT"), Locale.ROOT);
        cal.add(Calendar.MINUTE, 8);
        Assertions.assertEquals(
                DateUtils.round(cal.getTime(), Calendar.SECOND),
                DateUtils.round(nextFetch.get(), Calendar.SECOND));
    }

    @Test
    void testSchedule() throws MalformedURLException {
        Scheduler scheduler = new AdaptiveScheduler();
        scheduler.init(getConf());
        Metadata metadata = new Metadata();
        metadata.addValue("fetch.statusCode", "200");
        metadata.addValue(AdaptiveScheduler.SIGNATURE_KEY, md5sumEmptyContent);
        scheduler.schedule(Status.FETCHED, metadata);
        Instant firstFetch =
                DateUtils.round(
                                Calendar.getInstance(TimeZone.getTimeZone("GMT"), Locale.ROOT)
                                        .getTime(),
                                Calendar.SECOND)
                        .toInstant();
        /* verify initial fetch interval and last-modified time */
        String lastModified = metadata.getFirstValue(HttpHeaders.LAST_MODIFIED);
        Assertions.assertNotNull(lastModified);
        Instant lastModifiedTime =
                DateUtils.round(
                                GregorianCalendar.from(
                                        DateTimeFormatter.ISO_OFFSET_DATE_TIME.parse(
                                                lastModified, ZonedDateTime::from)),
                                Calendar.SECOND)
                        .toInstant();
        Assertions.assertTrue(firstFetch.until(lastModifiedTime, ChronoUnit.SECONDS) <= 1);
        String fetchInterval = metadata.getFirstValue(AdaptiveScheduler.FETCH_INTERVAL_KEY);
        Assertions.assertNotNull(fetchInterval);
        /* initial interval is the default interval */
        Assertions.assertEquals(5, Integer.parseInt(fetchInterval));
        /* test with signature not modified */
        metadata.addValue(AdaptiveScheduler.SIGNATURE_OLD_KEY, md5sumEmptyContent);
        scheduler.schedule(Status.FETCHED, metadata);
        fetchInterval = metadata.getFirstValue(AdaptiveScheduler.FETCH_INTERVAL_KEY);
        Assertions.assertNotNull(fetchInterval);
        /* interval should be bigger than initial interval */
        int fi1 = Integer.parseInt(fetchInterval);
        Assertions.assertTrue(5 < fi1);
        /* last-modified time should be unchanged */
        Assertions.assertEquals(lastModified, metadata.getFirstValue(HttpHeaders.LAST_MODIFIED));
        /* test with HTTP 304 "not modified" */
        metadata.setValue("fetch.statusCode", "304");
        scheduler.schedule(Status.FETCHED, metadata);
        fetchInterval = metadata.getFirstValue(AdaptiveScheduler.FETCH_INTERVAL_KEY);
        Assertions.assertNotNull(fetchInterval);
        /* interval should be bigger than initial interval and interval from last step */
        int fi2 = Integer.parseInt(fetchInterval);
        Assertions.assertTrue(5 < fi2);
        Assertions.assertTrue(fi1 < fi2);
        /* last-modified time should be unchanged */
        Assertions.assertEquals(lastModified, metadata.getFirstValue(HttpHeaders.LAST_MODIFIED));
        /* test with a changed signature */
        metadata.setValue("fetch.statusCode", "200");
        metadata.addValue(AdaptiveScheduler.SIGNATURE_KEY, md5sumSpaceContent);
        scheduler.schedule(Status.FETCHED, metadata);
        Instant lastFetch =
                DateUtils.round(
                                Calendar.getInstance(TimeZone.getTimeZone("GMT"), Locale.ROOT)
                                        .getTime(),
                                Calendar.SECOND)
                        .toInstant();
        fetchInterval = metadata.getFirstValue(AdaptiveScheduler.FETCH_INTERVAL_KEY);
        Assertions.assertNotNull(fetchInterval);
        /* interval should now shrink */
        int fi3 = Integer.parseInt(fetchInterval);
        Assertions.assertTrue(fi2 > fi3);
        /* last-modified time should fetch time of last fetch */
        lastModified = metadata.getFirstValue(HttpHeaders.LAST_MODIFIED);
        Assertions.assertNotNull(lastModified);
        lastModifiedTime =
                DateUtils.round(
                                GregorianCalendar.from(
                                        DateTimeFormatter.ISO_OFFSET_DATE_TIME.parse(
                                                lastModified, ZonedDateTime::from)),
                                Calendar.SECOND)
                        .toInstant();
        Assertions.assertTrue(lastFetch.until(lastModifiedTime, ChronoUnit.SECONDS) <= 1);
    }
}
