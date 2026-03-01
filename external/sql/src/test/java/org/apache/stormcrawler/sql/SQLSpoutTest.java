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

package org.apache.stormcrawler.sql;

import static org.apache.stormcrawler.TestUtil.getMockedTopologyContextWithBucket;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.stormcrawler.TestOutputCollector;
import org.apache.stormcrawler.TestUtil;
import org.apache.stormcrawler.persistence.urlbuffer.URLBuffer;
import org.junit.jupiter.api.Test;

class SQLSpoutTest extends AbstractSQLTest {

    @Override
    protected void setupTestTables() throws Exception {
        execute("DROP TABLE IF EXISTS urls");
        execute(
                """
                CREATE TABLE IF NOT EXISTS urls (
                    url VARCHAR(255),
                    status VARCHAR(16) DEFAULT 'DISCOVERED',
                    nextfetchdate TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    metadata TEXT,
                    bucket SMALLINT DEFAULT 0,
                    host VARCHAR(128),
                    PRIMARY KEY(url)
                )
                """);
    }

    @Test
    void bufferIsPopulatedAndEmitted() throws Exception {
        // Insert base test data with past nextfetchdate to ensure they're eligible for fetching
        Instant pastTime = Instant.now().minus(1, ChronoUnit.HOURS);
        insertTestURL("http://example.com/page1", 0, "example.com", pastTime);
        insertTestURL("http://example.com/page2", 0, "example.com", pastTime);
        insertTestURL("http://test.com/page1", 0, "test.com", pastTime);

        TestOutputCollector testCollector = new TestOutputCollector();
        SQLSpout spout = createSpout(testCollector, TestUtil.getMockedTopologyContext());

        // First call to nextTuple() populates the buffer
        spout.nextTuple();

        final List<String> expectedURLs =
                Arrays.asList(
                        "http://example.com/page1",
                        "http://example.com/page2",
                        "http://test.com/page1");
        assertURLsEmitted(spout, testCollector, 3, expectedURLs);
        spout.close();
    }

    @Test
    void testMaxDocsPerBucket() throws Exception {
        // Add more URLs from example.com to test the maxDocsPerBucket limit
        // Insert base test data with past nextfetchdate to ensure they're eligible for fetching
        Instant pastTime = Instant.now().minus(1, ChronoUnit.HOURS);
        insertTestURL("http://example.com/page1", 0, "example.com", pastTime);
        insertTestURL("http://example.com/page2", 0, "example.com", pastTime);
        insertTestURL("http://test.com/page1", 0, "test.com", pastTime);

        TestOutputCollector testCollector = new TestOutputCollector();
        SQLSpout spout = createSpout(testCollector, TestUtil.getMockedTopologyContext());

        pastTime = Instant.now().minus(1, ChronoUnit.HOURS);
        for (int i = 4; i <= 10; i++) {
            insertTestURL("http://example.com/page" + i, 0, "example.com", pastTime);
        }

        spout.nextTuple();

        URLBuffer buffer = getBufferFromSpout(spout);

        // With maxDocsPerBucket=5, we should get at most 5 URLs from example.com
        // plus 1 from test.com = 6 total
        assertEquals(6, buffer.size());
        spout.close();
    }

    @Test
    void testSingleInstanceNoBucketFiltering() throws Exception {

        Instant pastTime = Instant.now().minus(1, ChronoUnit.HOURS);

        // Insert URLs into different buckets
        insertTestURL("http://site1.com/page1", 0, "site1.com", pastTime);
        insertTestURL("http://site2.com/page1", 1, "site2.com", pastTime);
        insertTestURL("http://site3.com/page1", 2, "site3.com", pastTime);
        insertTestURL("http://site4.com/page1", 3, "site4.com", pastTime);

        // Create a single spout instance (totalTasks = 1)
        TestOutputCollector collector = new TestOutputCollector();
        TopologyContext context = getMockedTopologyContextWithBucket(0, 1, "sqlSpout");
        SQLSpout singleSpout = createSpout(collector, context);

        // Populate buffer
        singleSpout.nextTuple();

        final List<String> expectedURLs =
                Arrays.asList(
                        "http://site1.com/page1",
                        "http://site2.com/page1",
                        "http://site3.com/page1",
                        "http://site4.com/page1");
        assertURLsEmitted(singleSpout, collector, 4, expectedURLs);
        singleSpout.close();
    }

    @Test
    void testBucketPartitioningTwoInstances() throws Exception {

        // Insert URLs into different buckets
        Instant pastTime = Instant.now().minus(1, ChronoUnit.HOURS);

        // Bucket 0 URLs
        insertTestURL("http://bucket0-site1.com/page1", 0, "bucket0-site1.com", pastTime);
        insertTestURL("http://bucket0-site2.com/page1", 0, "bucket0-site2.com", pastTime);
        insertTestURL("http://bucket0-site3.com/page1", 0, "bucket0-site3.com", pastTime);

        // Bucket 1 URLs
        insertTestURL("http://bucket1-site1.com/page1", 1, "bucket1-site1.com", pastTime);
        insertTestURL("http://bucket1-site2.com/page1", 1, "bucket1-site2.com", pastTime);
        insertTestURL("http://bucket1-site3.com/page1", 1, "bucket1-site3.com", pastTime);

        // Create two spout instances with different bucket assignments
        SQLSpout[] spouts = new SQLSpout[2];
        for (int i = 0; i < 2; i++) {
            TestOutputCollector collector = new TestOutputCollector();
            TopologyContext context = getMockedTopologyContextWithBucket(i, 2, "sqlSpout");
            spouts[i] = createSpout(collector, context);
            spouts[i].nextTuple();
            assertURLsEmitted(
                    spouts[i],
                    collector,
                    3,
                    Arrays.asList(
                            "http://bucket" + i + "-site1.com/page1",
                            "http://bucket" + i + "-site2.com/page1",
                            "http://bucket" + i + "-site3.com/page1"));
            spouts[i].close();
        }
    }

    private void insertTestURL(String url, int bucket, String host, Instant time) throws Exception {
        String sql =
                """
                INSERT INTO urls (url, status, nextfetchdate, metadata, bucket, host)
                VALUES (?, ?, ?, ?, ?, ?)
                """;

        try (PreparedStatement ps = testConnection.prepareStatement(sql)) {
            ps.setString(1, url);
            ps.setString(2, "DISCOVERED");
            ps.setTimestamp(3, Timestamp.from(time));
            ps.setString(4, "\tkey=value\tdepth=0");
            ps.setInt(5, bucket);
            ps.setString(6, host);
            ps.executeUpdate();
        }
    }

    private Map<String, Object> createTestConfig() {
        Map<String, Object> conf = new HashMap<>();
        conf.put("sql.connection", createSqlConnectionConfig());
        conf.put("sql.status.table", "urls");
        conf.put("sql.max.urls.per.bucket", 5);
        conf.put("sql.spout.max.results", 100);
        conf.put(
                "urlbuffer.class", "org.apache.stormcrawler.persistence.urlbuffer.SimpleURLBuffer");
        return conf;
    }

    private URLBuffer getBufferFromSpout(SQLSpout spoutInstance) throws Exception {
        Field bufferField = spoutInstance.getClass().getSuperclass().getDeclaredField("buffer");
        bufferField.setAccessible(true);
        return (URLBuffer) bufferField.get(spoutInstance);
    }

    private void assertURLsEmitted(
            SQLSpout spout,
            TestOutputCollector collector,
            int numTuples,
            List<String> expectedURLs) {
        assertEquals(0, collector.getEmitted().size());

        // Emit all URLs
        Set<String> urls = new HashSet<>();
        for (int i = 0; i < numTuples; i++) {
            spout.nextTuple();
        }
        for (List<Object> tuple : collector.getEmitted()) {
            urls.add((String) tuple.get(0));
        }

        for (String url : expectedURLs) {
            assertTrue(urls.contains(url));
        }
    }

    private SQLSpout createSpout(TestOutputCollector collector, TopologyContext context) {
        SQLSpout singleSpout = new SQLSpout();
        Map<String, Object> conf = createTestConfig();
        singleSpout.open(conf, context, new SpoutOutputCollector(collector));
        singleSpout.activate();
        return singleSpout;
    }
}
