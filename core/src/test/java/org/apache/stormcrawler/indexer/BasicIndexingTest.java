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
package org.apache.stormcrawler.indexer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.indexing.AbstractIndexerBolt;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class BasicIndexingTest extends IndexerTester {

    private static final String URL = "https://stormcrawler.apache.org";

    @BeforeEach
    void setupIndexerBolt() {
        bolt = new DummyIndexer();
        setupIndexerBolt(bolt);
    }

    @Test
    void testEmptyCanonicalURL() throws Exception {
        Map<String, Object> config = new HashMap<>();
        config.put(AbstractIndexerBolt.urlFieldParamName, "url");
        config.put(AbstractIndexerBolt.canonicalMetadataParamName, "canonical");
        prepareIndexerBolt(config);
        index(URL, new Metadata());
        Map<String, String> fields = ((DummyIndexer) bolt).returnFields();
        Assertions.assertEquals(
                URL, fields.get("url"), "The URL should be used if no canonical URL is found");
    }

    @Test
    void testCanonicalURL() throws Exception {
        Map<String, Object> config = new HashMap<>();
        config.put(AbstractIndexerBolt.urlFieldParamName, "url");
        config.put(AbstractIndexerBolt.canonicalMetadataParamName, "canonical");
        Metadata metadata = new Metadata();
        metadata.setValue("canonical", "https://stormcrawler.apache.org/");
        prepareIndexerBolt(config);
        index(URL, metadata);
        Map<String, String> fields = ((DummyIndexer) bolt).returnFields();
        Assertions.assertEquals(
                "https://stormcrawler.apache.org/",
                fields.get("url"),
                "Use the canonical URL if found");
    }

    @Test
    void testRelativeCanonicalURL() throws Exception {
        Map<String, Object> config = new HashMap<>();
        config.put(AbstractIndexerBolt.urlFieldParamName, "url");
        config.put(AbstractIndexerBolt.canonicalMetadataParamName, "canonical");
        Metadata metadata = new Metadata();
        metadata.setValue("canonical", "/home");
        prepareIndexerBolt(config);
        index(URL, metadata);
        Map<String, String> fields = ((DummyIndexer) bolt).returnFields();
        Assertions.assertEquals(
                "https://stormcrawler.apache.org/home",
                fields.get("url"),
                "Use the canonical URL if found");
    }

    @Test
    void testBadCanonicalURL() throws Exception {
        Map<String, Object> config = new HashMap<>();
        config.put(AbstractIndexerBolt.urlFieldParamName, "url");
        config.put(AbstractIndexerBolt.canonicalMetadataParamName, "canonical");
        Metadata metadata = new Metadata();
        metadata.setValue("canonical", "htp://stormcrawler.apache.org/");
        prepareIndexerBolt(config);
        index(URL, metadata);
        Map<String, String> fields = ((DummyIndexer) bolt).returnFields();
        Assertions.assertEquals(
                "https://stormcrawler.apache.org",
                fields.get("url"),
                "Use the default URL if a bad canonical URL is found");
    }

    @Test
    void testOtherHostCanonicalURL() throws Exception {
        Map<String, Object> config = new HashMap<>();
        config.put(AbstractIndexerBolt.urlFieldParamName, "url");
        config.put(AbstractIndexerBolt.canonicalMetadataParamName, "canonical");
        Metadata metadata = new Metadata();
        metadata.setValue("canonical", "http://www.google.com/");
        prepareIndexerBolt(config);
        index(URL, metadata);
        Map<String, String> fields = ((DummyIndexer) bolt).returnFields();
        Assertions.assertEquals(
                "https://stormcrawler.apache.org",
                fields.get("url"),
                "Ignore if the canonical URL references other host");
    }

    @Test
    void testMissingCanonicalParamConfiguration() throws Exception {
        Map<String, Object> config = new HashMap<>();
        config.put(AbstractIndexerBolt.urlFieldParamName, "url");
        Metadata metadata = new Metadata();
        metadata.setValue("canonical", "https://stormcrawler.apache.org/");
        prepareIndexerBolt(config);
        index(URL, metadata);
        Map<String, String> fields = ((DummyIndexer) bolt).returnFields();
        Assertions.assertEquals(
                "https://stormcrawler.apache.org",
                fields.get("url"),
                "Use the canonical URL if found");
    }

    @Test
    void testFilterDocumentWithMetadata() throws Exception {
        Map<String, Object> config = new HashMap<>();
        config.put(AbstractIndexerBolt.urlFieldParamName, "url");
        config.put(AbstractIndexerBolt.metadataFilterParamName, "key1=value1");
        Metadata metadata = new Metadata();
        metadata.setValue("key1", "value1");
        prepareIndexerBolt(config);
        index(URL, metadata);
        Map<String, String> fields = ((DummyIndexer) bolt).returnFields();
        Assertions.assertEquals(
                "https://stormcrawler.apache.org",
                fields.get("url"),
                "The document must pass if the key/value is found in the metadata");
    }

    @Test
    void testFilterDocumentWithoutMetadata() throws Exception {
        Map<String, Object> config = new HashMap<>();
        config.put(AbstractIndexerBolt.urlFieldParamName, "url");
        config.put(AbstractIndexerBolt.metadataFilterParamName, "key1=value1");
        prepareIndexerBolt(config);
        index(URL, new Metadata());
        Map<String, String> fields = ((DummyIndexer) bolt).returnFields();
        Assertions.assertEquals(
                0,
                fields.size(),
                "The document must not pass if the key/value is not found in the metadata");
    }

    @Test
    void testFilterMetadata() throws Exception {
        Map<String, Object> config = new HashMap<>();
        config.put(AbstractIndexerBolt.urlFieldParamName, "url");
        final List<String> vector = new ArrayList<>();
        vector.add("parse.title=title");
        vector.add("parse.keywords=keywords");
        config.put(AbstractIndexerBolt.metadata2fieldParamName, vector);
        prepareIndexerBolt(config);
        Metadata metadata = new Metadata();
        metadata.setValue("parse.title", "This is the title");
        metadata.setValue("parse.keywords", "keyword1, keyword2, keyword3");
        metadata.setValue("parse.description", "This is the description");
        index(URL, metadata);
        Map<String, String> fields = ((DummyIndexer) bolt).returnFields();
        Assertions.assertArrayEquals(
                new String[] {"keywords", "title", "url"},
                new TreeSet<>(fields.keySet()).toArray(),
                "Only the mapped metadata attributes should be indexed");
    }

    @Test
    void testEmptyFilterMetadata() throws Exception {
        Map<String, Object> config = new HashMap<>();
        config.put(AbstractIndexerBolt.urlFieldParamName, "url");
        prepareIndexerBolt(config);
        Metadata metadata = new Metadata();
        metadata.setValue("parse.title", "This is the title");
        metadata.setValue("parse.keywords", "keyword1, keyword2, keyword3");
        metadata.setValue("parse.description", "This is the description");
        index(URL, metadata);
        Map<String, String> fields = ((DummyIndexer) bolt).returnFields();
        Assertions.assertArrayEquals(
                new String[] {"url"},
                fields.keySet().toArray(),
                "Index only the URL if no mapping is provided");
    }

    @Test
    void testGlobFilterMetadata() throws Exception {
        Map<String, Object> config = new HashMap<>();
        config.put(AbstractIndexerBolt.urlFieldParamName, "url");
        List<String> listKV = new ArrayList<>();
        listKV.add("parse.*");
        config.put(AbstractIndexerBolt.metadata2fieldParamName, listKV);
        prepareIndexerBolt(config);
        Metadata metadata = new Metadata();
        metadata.setValue("parse.title", "This is the title");
        metadata.setValue("parse.keywords", "keyword1, keyword2, keyword3");
        metadata.setValue("parse.description", "This is the description");
        index(URL, metadata);
        Map<String, String> fields = ((DummyIndexer) bolt).returnFields();
        Assertions.assertEquals(4, fields.keySet().size(), "Incorrect number of fields");
    }
}
