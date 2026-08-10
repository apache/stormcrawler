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
    void testRedirectedUrlIsEmittedToDeletionStream() {
        TestOutputCollector output = new TestOutputCollector();
        TestStatusUpdaterBolt bolt = new TestStatusUpdaterBolt();

        Map<String, Object> config = new HashMap<>();
        config.put(AbstractStatusUpdaterBolt.useCacheParamName, false);
        config.put(
                "scheduler.class",
                "org.apache.stormcrawler.persistence.DefaultScheduler");

        bolt.prepare(
                config,
                TestUtil.getMockedTopologyContext(),
                new OutputCollector(output));

        String url = "http://example.com/old-page";
        Metadata metadata = new Metadata();

        Map<String, Object> tupleValues = new HashMap<>();
        tupleValues.put("url", url);
        tupleValues.put("status", Status.REDIRECTION);
        tupleValues.put("metadata", metadata);

        Tuple tuple = TestUtil.getMockedTestTuple(tupleValues);

        bolt.execute(tuple);

        List<List<Object>> deletions =
                output.getEmitted(Constants.DELETION_STREAM_NAME);

        assertEquals(1, deletions.size());
        assertEquals(url, deletions.get(0).get(0));
        assertEquals(metadata, deletions.get(0).get(1));
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