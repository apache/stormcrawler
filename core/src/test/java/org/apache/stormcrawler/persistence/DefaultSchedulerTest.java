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
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.TimeZone;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.stormcrawler.Metadata;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class DefaultSchedulerTest {

    @Test
    void testScheduler() throws MalformedURLException {
        Map<String, Object> stormConf = new HashMap<>();
        stormConf.put("fetchInterval.FETCHED.testKey=someValue", 360);
        stormConf.put("fetchInterval.testKey=someValue", 3600);
        DefaultScheduler scheduler = new DefaultScheduler();
        scheduler.init(stormConf);
        Metadata metadata = new Metadata();
        metadata.addValue("testKey", "someValue");
        Optional<Date> nextFetch = scheduler.schedule(Status.FETCHED, metadata);
        Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("GMT"), Locale.ROOT);
        cal.add(Calendar.MINUTE, 360);
        Assertions.assertEquals(
                DateUtils.round(cal.getTime(), Calendar.SECOND),
                DateUtils.round(nextFetch.get(), Calendar.SECOND));
        nextFetch = scheduler.schedule(Status.ERROR, metadata);
        cal = Calendar.getInstance(TimeZone.getTimeZone("GMT"), Locale.ROOT);
        cal.add(Calendar.MINUTE, 3600);
        Assertions.assertEquals(
                DateUtils.round(cal.getTime(), Calendar.SECOND),
                DateUtils.round(nextFetch.get(), Calendar.SECOND));
    }

    @Test
    void testCustomWithDot() throws MalformedURLException {
        Map<String, Object> stormConf = new HashMap<>();
        stormConf.put("fetchInterval.FETCHED.testKey.key2=someValue", 360);
        DefaultScheduler scheduler = new DefaultScheduler();
        scheduler.init(stormConf);
        Metadata metadata = new Metadata();
        metadata.addValue("testKey.key2", "someValue");
        Optional<Date> nextFetch = scheduler.schedule(Status.FETCHED, metadata);
        Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("GMT"), Locale.ROOT);
        cal.add(Calendar.MINUTE, 360);
        Assertions.assertEquals(
                DateUtils.round(cal.getTime(), Calendar.SECOND),
                DateUtils.round(nextFetch.get(), Calendar.SECOND));
    }

    @Test
    void testBadConfig() throws MalformedURLException {
        Map<String, Object> stormConf = new HashMap<>();
        stormConf.put("fetchInterval.DODGYSTATUS.testKey=someValue", 360);
        DefaultScheduler scheduler = new DefaultScheduler();
        boolean exception = false;
        try {
            scheduler.init(stormConf);
        } catch (IllegalArgumentException e) {
            exception = true;
        }
        Assertions.assertTrue(exception);
    }

    @Test
    void testNever() throws MalformedURLException {
        Map<String, Object> stormConf = new HashMap<>();
        stormConf.put("fetchInterval.error", -1);
        DefaultScheduler scheduler = new DefaultScheduler();
        scheduler.init(stormConf);
        Metadata metadata = new Metadata();
        Optional<Date> nextFetch = scheduler.schedule(Status.ERROR, metadata);
        Assertions.assertFalse(nextFetch.isPresent());
    }

    @Test
    void testSpecificNever() throws MalformedURLException {
        Map<String, Object> stormConf = new HashMap<>();
        stormConf.put("fetchInterval.FETCHED.isSpam=true", -1);
        DefaultScheduler scheduler = new DefaultScheduler();
        scheduler.init(stormConf);
        Metadata metadata = new Metadata();
        metadata.setValue("isSpam", "true");
        Optional<Date> nextFetch = scheduler.schedule(Status.FETCHED, metadata);
        Assertions.assertFalse(nextFetch.isPresent());
    }
}
