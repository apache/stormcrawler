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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Tuple;
import org.apache.stormcrawler.Constants;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.TestOutputCollector;
import org.apache.stormcrawler.TestUtil;
import org.apache.stormcrawler.indexing.AbstractIndexerBolt;
import org.apache.stormcrawler.persistence.Status;
import org.apache.stormcrawler.util.RobotsTags;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class IndexerBoltTest extends AbstractSQLTest {

    private TestOutputCollector output;
    private final String tableName = "content";

    @Override
    protected void setupTestTables() throws Exception {
        execute(
                """
                DROP TABLE IF EXISTS content
                """);
        execute(
                """
                CREATE TABLE IF NOT EXISTS content (
                    url VARCHAR(255) PRIMARY KEY,
                    title VARCHAR(255),
                    description TEXT,
                    keywords VARCHAR(255)
                )
                """);
    }

    @BeforeEach
    void setup() {
        output = new TestOutputCollector();
    }

    @Test
    void testBasicIndexing() throws Exception {
        IndexerBolt bolt = createBolt(createBasicConfig());
        String url = "http://example.com/page1";

        executeTuple(bolt, url, "This is the page content", getMetadata());

        // Verify URL was stored in database
        try (Statement stmt = testConnection.createStatement();
                ResultSet rs =
                        stmt.executeQuery(
                                "SELECT * FROM " + tableName + " WHERE url = '" + url + "'")) {
            assertTrue(rs.next(), "URL should be stored in database");
            assertEquals(url, rs.getString("url"));
            assertEquals("Test Page Title", rs.getString("title"));
            assertEquals("Test page description", rs.getString("description"));
            assertEquals("test, page, keywords", rs.getString("keywords"));
        }

        // Verify tuple was acked and status emitted
        assertEquals(1, output.getAckedTuples().size());
        assertEquals(1, output.getEmitted(Constants.StatusStreamName).size());

        // Verify emitted status is FETCHED
        List<Object> emitted = output.getEmitted(Constants.StatusStreamName).get(0);
        assertEquals(url, emitted.get(0));
        assertEquals(Status.FETCHED, emitted.get(2));
        bolt.cleanup();
    }

    @Test
    void testDuplicateHandling() throws Exception {
        IndexerBolt bolt = createBolt(createBasicConfig());

        String url = "http://example.com/page2";
        executeTuple(bolt, url, "Original content", getMetadata());

        // Second indexing with updated content (same URL)
        Metadata metadata = new Metadata();
        metadata.addValue("title", "Updated Title");
        metadata.addValue("description", "Updated description");

        executeTuple(bolt, url, "Updated content", metadata);

        try (Statement stmt = testConnection.createStatement();
                ResultSet rs =
                        stmt.executeQuery(
                                "SELECT * FROM " + tableName + " WHERE url = '" + url + "'")) {
            assertTrue(rs.next());
            assertEquals("Updated Title", rs.getString("title"));
            assertEquals("Updated description", rs.getString("description"));
            assertEquals("test, page, keywords", rs.getString("keywords"));
        }

        assertEquals(2, output.getAckedTuples().size());
        bolt.cleanup();
    }

    @Test
    void testFilteringByRobotsNoIndex() throws Exception {
        IndexerBolt bolt = createBolt(createBasicConfig());

        String url = "http://example.com/noindex-page";
        Metadata metadata = new Metadata();
        metadata.addValue("title", "Should Not Be Indexed");
        metadata.addValue(RobotsTags.ROBOTS_NO_INDEX, "true");

        executeTuple(bolt, url, "Content", metadata);

        // Verify URL was NOT stored in database
        try (Statement stmt = testConnection.createStatement();
                ResultSet rs =
                        stmt.executeQuery(
                                "SELECT * FROM " + tableName + " WHERE url = '" + url + "'")) {
            assertFalse(rs.next(), "URL with noindex should not be stored in database");
        }

        // But tuple should still be acked and FETCHED status emitted
        assertEquals(1, output.getAckedTuples().size());
        assertEquals(1, output.getEmitted(Constants.StatusStreamName).size());

        List<Object> emitted = output.getEmitted(Constants.StatusStreamName).get(0);
        assertEquals(Status.FETCHED, emitted.get(2));
        bolt.cleanup();
    }

    @Test
    void testFilteringByMetadataFilter() throws Exception {
        // Configure filter to only index documents with indexable=yes
        Map<String, Object> conf = createBasicConfig();
        conf.put(AbstractIndexerBolt.metadataFilterParamName, "indexable=yes");

        IndexerBolt bolt = createBolt(conf);

        // Document that should be filtered out (no indexable metadata)
        String url1 = "http://example.com/filtered-page";
        Metadata metadata1 = new Metadata();
        metadata1.addValue("title", "Filtered Page");

        executeTuple(bolt, url1, "Content", metadata1);

        // Verify filtered document was NOT stored
        try (Statement stmt = testConnection.createStatement();
                ResultSet rs =
                        stmt.executeQuery(
                                "SELECT * FROM " + tableName + " WHERE url = '" + url1 + "'")) {
            assertFalse(rs.next(), "Filtered document should not be stored");
        }

        // Document that should be indexed (has indexable=yes)
        String url2 = "http://example.com/indexed-page";
        Metadata metadata2 = new Metadata();
        metadata2.addValue("title", "Indexed Page");
        metadata2.addValue("indexable", "yes");

        Tuple tuple2 = createTuple(url2, "Content", metadata2);
        bolt.execute(tuple2);

        // Verify indexed document WAS stored
        try (Statement stmt = testConnection.createStatement();
                ResultSet rs =
                        stmt.executeQuery(
                                "SELECT * FROM " + tableName + " WHERE url = '" + url2 + "'")) {
            assertTrue(rs.next(), "Document with indexable=yes should be stored");
            assertEquals("Indexed Page", rs.getString("title"));
        }

        // Both tuples should be acked with FETCHED status
        assertEquals(2, output.getAckedTuples().size());
        assertEquals(2, output.getEmitted(Constants.StatusStreamName).size());
        bolt.cleanup();
    }

    @Test
    void testMetadataExtraction() throws Exception {
        // Configure to only extract specific metadata fields
        Map<String, Object> conf = createBasicConfig();
        // Only map title and description, not keywords
        List<String> mdMapping = new ArrayList<>();
        mdMapping.add("title");
        mdMapping.add("description");
        conf.put(AbstractIndexerBolt.metadata2fieldParamName, mdMapping);

        IndexerBolt bolt = createBolt(conf);

        String url = "http://example.com/metadata-test";
        Metadata metadata = new Metadata();
        metadata.addValue("title", "Extracted Title");
        metadata.addValue("description", "Extracted Description");
        metadata.addValue("keywords", "these,should,not,be,stored");
        metadata.addValue("author", "Should Not Be Stored");

        executeTuple(bolt, url, "Content", metadata);

        // Verify only configured metadata was stored
        try (Statement stmt = testConnection.createStatement();
                ResultSet rs =
                        stmt.executeQuery(
                                "SELECT * FROM " + tableName + " WHERE url = '" + url + "'")) {
            assertTrue(rs.next());
            assertEquals("Extracted Title", rs.getString("title"));
            assertEquals("Extracted Description", rs.getString("description"));
            // keywords column should be null since it wasn't in the mapping
            assertNull(rs.getString("keywords"));
        }

        assertEquals(1, output.getAckedTuples().size());
        bolt.cleanup();
    }

    @Test
    void testMetadataAliasMapping() throws Exception {
        // Configure metadata mapping with aliases
        Map<String, Object> conf = createBasicConfig();
        List<String> mdMapping = new ArrayList<>();
        mdMapping.add("parse.title=title"); // map parse.title to title column
        mdMapping.add("parse.description=description");
        conf.put(AbstractIndexerBolt.metadata2fieldParamName, mdMapping);

        IndexerBolt bolt = createBolt(conf);

        String url = "http://example.com/alias-test";
        Metadata metadata = new Metadata();
        metadata.addValue("parse.title", "Title from Parser");
        metadata.addValue("parse.description", "Description from Parser");

        executeTuple(bolt, url, "Content", metadata);

        // Verify aliased metadata was stored correctly
        try (Statement stmt = testConnection.createStatement();
                ResultSet rs =
                        stmt.executeQuery(
                                "SELECT * FROM " + tableName + " WHERE url = '" + url + "'")) {
            assertTrue(rs.next());
            assertEquals("Title from Parser", rs.getString("title"));
            assertEquals("Description from Parser", rs.getString("description"));
        }
        bolt.cleanup();
    }

    private Tuple createTuple(String url, String text, Metadata metadata) {
        Tuple tuple = mock(Tuple.class);
        when(tuple.getStringByField("url")).thenReturn(url);
        when(tuple.getStringByField("text")).thenReturn(text);
        when(tuple.getValueByField("metadata")).thenReturn(metadata);
        return tuple;
    }

    private void executeTuple(IndexerBolt bolt, String url, String text, Metadata metadata) {
        Tuple tuple = createTuple(url, text, metadata);
        bolt.execute(tuple);
    }

    private Map<String, Object> createBasicConfig() {
        Map<String, Object> conf = new HashMap<>();
        conf.put("sql.connection", createSqlConnectionConfig());
        conf.put(IndexerBolt.SQL_INDEX_TABLE_PARAM_NAME, tableName);
        conf.put(AbstractIndexerBolt.urlFieldParamName, "url");
        // Default metadata mapping
        List<String> mdMapping = new ArrayList<>();
        mdMapping.add("title");
        mdMapping.add("description");
        mdMapping.add("keywords");
        conf.put(AbstractIndexerBolt.metadata2fieldParamName, mdMapping);
        return conf;
    }

    private IndexerBolt createBolt(Map<String, Object> conf) {
        IndexerBolt bolt = new IndexerBolt();
        bolt.prepare(conf, TestUtil.getMockedTopologyContext(), new OutputCollector(output));
        return bolt;
    }

    private Metadata getMetadata() {
        Metadata metadata = new Metadata();
        metadata.addValue("title", "Test Page Title");
        metadata.addValue("description", "Test page description");
        metadata.addValue("keywords", "test, page, keywords");
        return metadata;
    }
}
