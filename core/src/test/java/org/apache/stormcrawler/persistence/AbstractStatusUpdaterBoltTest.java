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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Tuple;
import org.apache.stormcrawler.Constants;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.TestOutputCollector;
import org.apache.stormcrawler.TestUtil;
import org.junit.jupiter.api.Test;

class AbstractStatusUpdaterBoltTest {

    @Test
    void testPermanentRedirect301IsEmittedToDeletionStream() {
        TestOutputCollector output = new TestOutputCollector();
        TestStatusUpdaterBolt bolt = new TestStatusUpdaterBolt();

        Map<String, Object> config = createConfig();
        config.put(AbstractStatusUpdaterBolt.deleteRedirectionsParamName, true);

        bolt.prepare(config, TestUtil.getMockedTopologyContext(), new OutputCollector(output));

        String url = "http://example.com/old-page";
        Metadata metadata = new Metadata();
        metadata.setValue("fetch.statusCode", "301");

        Tuple tuple = createTuple(url, Status.REDIRECTION, metadata);

        bolt.execute(tuple);

        List<List<Object>> deletions =
                output.getEmitted(Constants.DELETION_STREAM_NAME);

        assertEquals(1, deletions.size());
        assertEquals(url, deletions.get(0).get(0));

        Metadata emittedMetadata = (Metadata) deletions.get(0).get(1);
        assertEquals("301", emittedMetadata.getFirstValue("fetch.statusCode"));
    }

    @Test
    void testPermanentRedirect308IsEmittedToDeletionStream() {
        TestOutputCollector output = new TestOutputCollector();
        TestStatusUpdaterBolt bolt = new TestStatusUpdaterBolt();

        Map<String, Object> config = createConfig();
        config.put(AbstractStatusUpdaterBolt.deleteRedirectionsParamName, true);

        bolt.prepare(config, TestUtil.getMockedTopologyContext(), new OutputCollector(output));

        String url = "http://example.com/old-page";
        Metadata metadata = new Metadata();
        metadata.setValue("fetch.statusCode", "308");

        Tuple tuple = createTuple(url, Status.REDIRECTION, metadata);

        bolt.execute(tuple);

        List<List<Object>> deletions =
                output.getEmitted(Constants.DELETION_STREAM_NAME);

        assertEquals(1, deletions.size());
        assertEquals(url, deletions.get(0).get(0));

        Metadata emittedMetadata = (Metadata) deletions.get(0).get(1);
        assertEquals("308", emittedMetadata.getFirstValue("fetch.statusCode"));
    }

    @Test
    void testTemporaryRedirect302IsNotEmittedToDeletionStream() {
        TestOutputCollector output = new TestOutputCollector();
        TestStatusUpdaterBolt bolt = new TestStatusUpdaterBolt();

        Map<String, Object> config = createConfig();
        config.put(AbstractStatusUpdaterBolt.deleteRedirectionsParamName, true);

        bolt.prepare(config, TestUtil.getMockedTopologyContext(), new OutputCollector(output));

        String url = "http://example.com/old-page";
        Metadata metadata = new Metadata();
        metadata.setValue("fetch.statusCode", "302");

        Tuple tuple = createTuple(url, Status.REDIRECTION, metadata);

        bolt.execute(tuple);

        List<List<Object>> deletions =
                output.getEmitted(Constants.DELETION_STREAM_NAME);

        assertEquals(0, deletions.size());
    }

    @Test
    void testMetaRefreshRedirectIsNotEmittedToDeletionStream() {
        TestOutputCollector output = new TestOutputCollector();
        TestStatusUpdaterBolt bolt = new TestStatusUpdaterBolt();

        Map<String, Object> config = createConfig();
        config.put(AbstractStatusUpdaterBolt.deleteRedirectionsParamName, true);

        bolt.prepare(config, TestUtil.getMockedTopologyContext(), new OutputCollector(output));

        String url = "http://example.com/old-page";
        Metadata metadata = new Metadata();
        metadata.setValue("fetch.statusCode", "200");
        metadata.setValue("_redirTo", "http://example.com/new-page");

        Tuple tuple = createTuple(url, Status.REDIRECTION, metadata);

        bolt.execute(tuple);

        List<List<Object>> deletions =
                output.getEmitted(Constants.DELETION_STREAM_NAME);

        assertEquals(0, deletions.size());
    }

    @Test
    void testRedirectionWithoutStatusCodeIsNotEmittedToDeletionStream() {
        TestOutputCollector output = new TestOutputCollector();
        TestStatusUpdaterBolt bolt = new TestStatusUpdaterBolt();

        Map<String, Object> config = createConfig();
        config.put(AbstractStatusUpdaterBolt.deleteRedirectionsParamName, true);

        bolt.prepare(config, TestUtil.getMockedTopologyContext(), new OutputCollector(output));

        String url = "http://example.com/old-page";
        Metadata metadata = new Metadata();

        Tuple tuple = createTuple(url, Status.REDIRECTION, metadata);

        bolt.execute(tuple);

        List<List<Object>> deletions =
                output.getEmitted(Constants.DELETION_STREAM_NAME);

        assertEquals(0, deletions.size());
    }

    @Test
    void testPermanentRedirectIsNotDeletedByDefault() {
        TestOutputCollector output = new TestOutputCollector();
        TestStatusUpdaterBolt bolt = new TestStatusUpdaterBolt();

        bolt.prepare(
                createConfig(), TestUtil.getMockedTopologyContext(), new OutputCollector(output));

        String url = "http://example.com/old-page";
        Metadata metadata = new Metadata();
        metadata.setValue("fetch.statusCode", "301");

        Tuple tuple = createTuple(url, Status.REDIRECTION, metadata);

        bolt.execute(tuple);

        List<List<Object>> deletions =
                output.getEmitted(Constants.DELETION_STREAM_NAME);

        assertEquals(0, deletions.size());
    }

    @Test
    void testFetchedUrlIsNotEmittedToDeletionStream() {
        TestOutputCollector output = new TestOutputCollector();
        TestStatusUpdaterBolt bolt = new TestStatusUpdaterBolt();

        bolt.prepare(
                createConfig(), TestUtil.getMockedTopologyContext(), new OutputCollector(output));

        String url = "http://example.com/page";
        Metadata metadata = new Metadata();
        metadata.setValue("fetch.statusCode", "200");

        Tuple tuple = createTuple(url, Status.FETCHED, metadata);

        bolt.execute(tuple);

        List<List<Object>> deletions =
                output.getEmitted(Constants.DELETION_STREAM_NAME);

        assertEquals(0, deletions.size());
    }

    @Test
    void testErrorIsEmittedToDeletionStream() {
        TestOutputCollector output = new TestOutputCollector();
        TestStatusUpdaterBolt bolt = new TestStatusUpdaterBolt();

        bolt.prepare(
                createConfig(), TestUtil.getMockedTopologyContext(), new OutputCollector(output));

        String url = "http://example.com/error";
        Metadata metadata = new Metadata();

        Tuple tuple = createTuple(url, Status.ERROR, metadata);

        bolt.execute(tuple);

        List<List<Object>> deletions =
                output.getEmitted(Constants.DELETION_STREAM_NAME);

        assertEquals(1, deletions.size());
        assertEquals(url, deletions.get(0).get(0));
    }

    private static Map<String, Object> createConfig() {
        Map<String, Object> config = new HashMap<>();
        config.put(AbstractStatusUpdaterBolt.useCacheParamName, false);
        config.put("scheduler.class", "org.apache.stormcrawler.persistence.DefaultScheduler");
        return config;
    }

    private static Tuple createTuple(String url, Status status, Metadata metadata) {
        Map<String, Object> tupleValues = new HashMap<>();
        tupleValues.put("url", url);
        tupleValues.put("status", status);
        tupleValues.put("metadata", metadata);

        return TestUtil.getMockedTestTuple(tupleValues);
    }

    private static class TestStatusUpdaterBolt extends AbstractStatusUpdaterBolt {

        @Override
        protected void store(
                String url,
                Status status,
                Metadata metadata,
                java.util.Optional<java.util.Date> nextFetch,
                Tuple tuple) {
            collector.ack(tuple);
        }
    }
}