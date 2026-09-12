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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Tuple;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.TestUtil;
import org.apache.stormcrawler.util.MetadataTransfer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The date passed in {@link AbstractStatusUpdaterBolt#AS_IS_NEXTFETCHDATE_METADATA} comes from the
 * metadata and can be anything, so it must be parsed defensively.
 */
class AbstractStatusUpdaterBoltTest {

    private static final String URL = "http://example.com/";

    /** Records what the bolt asked to store. */
    private static class RecordingStatusUpdaterBolt extends AbstractStatusUpdaterBolt {

        Optional<Date> nextFetch;
        String storedUrl;
        int stored = 0;

        @Override
        public void store(
                String url, Status status, Metadata metadata, Optional<Date> nextFetch, Tuple t) {
            this.nextFetch = nextFetch;
            this.storedUrl = url;
            this.stored++;
            ack(t, url);
        }
    }

    private RecordingStatusUpdaterBolt bolt;

    private OutputCollector collector;

    @BeforeEach
    void setUp() {
        collector = mock(OutputCollector.class);
        bolt = new RecordingStatusUpdaterBolt();
        Map<String, Object> conf = new HashMap<>();
        conf.put(AbstractStatusUpdaterBolt.useCacheParamName, Boolean.FALSE);
        conf.put(Scheduler.schedulerClassParamName, DefaultScheduler.class.getName());
        conf.put(MetadataTransfer.metadataTransferClassParamName, MetadataTransfer.class.getName());
        bolt.prepare(conf, TestUtil.getMockedTopologyContext(), collector);
    }

    private static Tuple statusTuple(Metadata metadata) {
        Tuple tuple = mock(Tuple.class);
        when(tuple.getStringByField("url")).thenReturn(URL);
        when(tuple.getValueByField("metadata")).thenReturn(metadata);
        when(tuple.getValueByField("status")).thenReturn(Status.FETCHED);
        return tuple;
    }

    @Test
    void validNextFetchDateIsUsedAsIs() {
        Metadata metadata = new Metadata();
        metadata.setValue(
                AbstractStatusUpdaterBolt.AS_IS_NEXTFETCHDATE_METADATA, "2026-01-02T03:04:05Z");

        bolt.execute(statusTuple(metadata));

        assertEquals(1, bolt.stored);
        assertNotNull(bolt.nextFetch);
        assertTrue(bolt.nextFetch.isPresent());
        assertEquals("2026-01-02T03:04:05Z", bolt.nextFetch.get().toInstant().toString());
    }

    @Test
    void invalidNextFetchDateIsIgnoredAndTheUrlIsScheduled() {
        Metadata metadata = new Metadata();
        metadata.setValue(AbstractStatusUpdaterBolt.AS_IS_NEXTFETCHDATE_METADATA, "NOT-A-DATE");

        bolt.execute(statusTuple(metadata));

        assertEquals(1, bolt.stored, "the URL must still be stored");
        assertNotNull(bolt.nextFetch);
    }

    @Test
    void outOfRangeNextFetchDateIsIgnoredAndTheUrlIsScheduled() {
        Metadata metadata = new Metadata();
        // parses as an instant but does not fit a java.util.Date
        metadata.setValue(
                AbstractStatusUpdaterBolt.AS_IS_NEXTFETCHDATE_METADATA,
                "+1000000000-12-31T23:59:59Z");

        bolt.execute(statusTuple(metadata));

        assertEquals(1, bolt.stored, "the URL must still be stored");
        assertNotNull(bolt.nextFetch);
    }

    /** A bolt prepared with the given normalise-hosts and cache settings. */
    private static RecordingStatusUpdaterBolt newBolt(boolean normaliseHosts, boolean useCache) {
        RecordingStatusUpdaterBolt b = new RecordingStatusUpdaterBolt();
        Map<String, Object> conf = new HashMap<>();
        conf.put(AbstractStatusUpdaterBolt.useCacheParamName, useCache);
        conf.put(AbstractStatusUpdaterBolt.normaliseHostsParamName, normaliseHosts);
        conf.put(
                AbstractStatusUpdaterBolt.cacheConfigParamName,
                "maximumSize=100,expireAfterAccess=1h");
        conf.put(Scheduler.schedulerClassParamName, DefaultScheduler.class.getName());
        conf.put(MetadataTransfer.metadataTransferClassParamName, MetadataTransfer.class.getName());
        b.prepare(conf, TestUtil.getMockedTopologyContext(), mock(OutputCollector.class));
        return b;
    }

    private static Tuple discoveredTuple(String url) {
        Tuple tuple = mock(Tuple.class);
        when(tuple.getStringByField("url")).thenReturn(url);
        when(tuple.getValueByField("metadata")).thenReturn(new Metadata());
        when(tuple.getValueByField("status")).thenReturn(Status.DISCOVERED);
        return tuple;
    }

    @Test
    void discoveredHostIsNormalisedWhenEnabled() {
        RecordingStatusUpdaterBolt b = newBolt(true, false);
        b.execute(discoveredTuple("http://exampl%65.org./a"));
        assertEquals("http://example.org/a", b.storedUrl, "the aliased host is stored normalised");
    }

    /** The dedup cache is keyed on the normalised URL, so aliases collapse. */
    @Test
    void normalisedAliasesHitTheDedupCache() {
        RecordingStatusUpdaterBolt b = newBolt(true, true);
        b.execute(discoveredTuple("http://example.org/a"));
        b.execute(discoveredTuple("http://exampl%65.org/a"));
        assertEquals(1, b.stored, "the second spelling is deduped against the first");
    }

    @Test
    void discoveredHostIsLeftRawWhenDisabled() {
        RecordingStatusUpdaterBolt b = newBolt(false, false);
        b.execute(discoveredTuple("http://exampl%65.org./a"));
        assertEquals(
                "http://exampl%65.org./a", b.storedUrl, "the default keeps the URL byte for byte");
    }

    /** Only DISCOVERED is normalised; updates of stored URLs are not rewritten. */
    @Test
    void onlyDiscoveredStatusIsNormalised() {
        RecordingStatusUpdaterBolt b = newBolt(true, false);
        Tuple tuple = mock(Tuple.class);
        when(tuple.getStringByField("url")).thenReturn("http://exampl%65.org/a");
        when(tuple.getValueByField("metadata")).thenReturn(new Metadata());
        when(tuple.getValueByField("status")).thenReturn(Status.FETCHED);
        b.execute(tuple);
        assertEquals(
                "http://exampl%65.org/a",
                b.storedUrl,
                "a FETCHED update carries a URL read back from the store and is not rewritten");
    }
}
