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

package org.apache.stormcrawler.jsoup;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.storm.task.OutputCollector;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.TestUtil;
import org.apache.stormcrawler.bolt.JSoupParserBolt;
import org.apache.stormcrawler.parse.ParsingTester;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Test the JSoup filters * */
class JSoupFiltersTest extends ParsingTester {

    @BeforeEach
    void setupParserBolt() {
        bolt = new JSoupParserBolt();
        setupParserBolt(bolt);
    }

    protected void prepareParserBolt(String configFile, Map<String, Object> parserConfig) {
        parserConfig.put("jsoup.filters.config.file", configFile);
        bolt.prepare(
                parserConfig, TestUtil.getMockedTopologyContext(), new OutputCollector(output));
    }

    @Test
    void testBasicExtraction() throws IOException {
        prepareParserBolt("test.jsoupfilters.json");
        parse("https://stormcrawler.apache.org", "stormcrawler.apache.org.html");
        Assertions.assertEquals(1, output.getEmitted().size());
        List<Object> parsedTuple = output.getEmitted().get(0);
        Metadata metadata = (Metadata) parsedTuple.get(2);
        Assertions.assertNotNull(metadata);
        String concept = metadata.getFirstValue("concept");
        Assertions.assertNotNull(concept);
        concept = metadata.getFirstValue("concept2");
        Assertions.assertNotNull(concept);
    }

    @Test
    // https://github.com/apache/stormcrawler/issues/219
    void testScriptExtraction() throws IOException {
        prepareParserBolt("test.jsoupfilters.json");
        parse("https://stormcrawler.apache.org", "stormcrawler.apache.org.html");
        Assertions.assertEquals(1, output.getEmitted().size());
        List<Object> parsedTuple = output.getEmitted().get(0);
        Metadata metadata = (Metadata) parsedTuple.get(2);
        Assertions.assertNotNull(metadata);
        String[] scripts = metadata.getValues("js");
        Assertions.assertNotNull(scripts);
        // should be 2 of them
        Assertions.assertEquals(2, scripts.length);
        Assertions.assertEquals("", scripts[0].trim());
        Assertions.assertTrue(scripts[1].contains("_paq"));
    }

    @Test
    void testLDJsonExtraction() throws IOException {
        prepareParserBolt("test.jsoupfilters.json");
        parse("https://stormcrawler.apache.org", "stormcrawler.apache.org.html");
        Assertions.assertEquals(1, output.getEmitted().size());
        List<Object> parsedTuple = output.getEmitted().get(0);
        Metadata metadata = (Metadata) parsedTuple.get(2);
        Assertions.assertNotNull(metadata);
        String[] scripts = metadata.getValues("streetAddress");
        Assertions.assertNotNull(scripts);
    }

    @Test
    void testExtraLink() throws IOException {
        prepareParserBolt("test.jsoupfilters.json");
        parse("https://stormcrawler.apache.org", "stormcrawler.apache.org.html");
        Assertions.assertEquals(31, output.getEmitted("status").size());
    }

    private Metadata parseXPathTestPage() throws IOException {
        prepareParserBolt("test.xpath.jsoupfilters.json", new HashMap<>());
        parse("https://example.com", "xpath-test.html");
        Assertions.assertEquals(1, output.getEmitted().size());
        List<Object> parsedTuple = output.getEmitted().get(0);
        Metadata metadata = (Metadata) parsedTuple.get(2);
        Assertions.assertNotNull(metadata);
        return metadata;
    }

    @Test
    void testAttributeExtraction() throws IOException {
        Metadata metadata = parseXPathTestPage();
        Assertions.assertEquals("storm, crawler, xpath", metadata.getFirstValue("attr_content"));
        Assertions.assertEquals("Test Author", metadata.getFirstValue("attr_author"));
    }

    @Test
    void testTidyTextFunction() throws IOException {
        Metadata metadata = parseXPathTestPage();
        String tidyText = metadata.getFirstValue("tidy_text");
        Assertions.assertNotNull(tidyText);
        Assertions.assertEquals("Hello world, this is a test.", tidyText.strip());
    }

    @Test
    void testAllTextFunction() throws IOException {
        Metadata metadata = parseXPathTestPage();
        String allText = metadata.getFirstValue("all_text");
        Assertions.assertNotNull(allText);
        Assertions.assertEquals("Hello world, this is a test.", allText.strip());
    }

    @Test
    void testHtmlFunction() throws IOException {
        Metadata metadata = parseXPathTestPage();
        String html = metadata.getFirstValue("html_content");
        Assertions.assertNotNull(html);
        Assertions.assertTrue(html.contains("<strong>world</strong>"));
        Assertions.assertTrue(html.contains("Hello"));
    }

    @Test
    void testUppercaseElementNames() throws IOException {
        Metadata metadata = parseXPathTestPage();
        String[] concepts = metadata.getValues("concept_upper");
        Assertions.assertNotNull(concepts);
        Assertions.assertEquals(2, concepts.length);
        Assertions.assertTrue(concepts[0].contains("First concept"));
        Assertions.assertTrue(concepts[1].contains("Second concept"));
        Assertions.assertTrue(metadata.getFirstValue("h1_text").contains("Main Title"));
    }

    @Test
    void testFallbackExpressions() throws IOException {
        Metadata metadata = parseXPathTestPage();
        String fallback = metadata.getFirstValue("fallback");
        Assertions.assertNotNull(fallback, "Fallback expression should have matched");
        Assertions.assertTrue(fallback.contains("Main Title"));
    }
}
