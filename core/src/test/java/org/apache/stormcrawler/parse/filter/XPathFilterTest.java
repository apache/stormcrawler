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

package org.apache.stormcrawler.parse.filter;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.bolt.JSoupParserBolt;
import org.apache.stormcrawler.parse.DocumentFragmentBuilder;
import org.apache.stormcrawler.parse.ParseResult;
import org.apache.stormcrawler.parse.ParsingTester;
import org.jsoup.Jsoup;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class XPathFilterTest extends ParsingTester {

    @BeforeEach
    void setupParserBolt() {
        bolt = new JSoupParserBolt();
        setupParserBolt(bolt);
    }

    @Test
    void testBasicExtraction() throws IOException {
        prepareParserBolt("test.parsefilters.json");
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
        prepareParserBolt("test.parsefilters.json");
        parse("https://stormcrawler.apache.org", "stormcrawler.apache.org.html");
        Assertions.assertEquals(1, output.getEmitted().size());
        List<Object> parsedTuple = output.getEmitted().get(0);
        Metadata metadata = (Metadata) parsedTuple.get(2);
        Assertions.assertNotNull(metadata);
        String[] scripts = metadata.getValues("js");
        Assertions.assertNotNull(scripts);
        // the first script is external and has no content, its blank value is skipped
        Assertions.assertEquals(1, scripts.length);
        Assertions.assertTrue(scripts[0].contains("_paq"));
    }

    // element names are in upper case in the DOM built from the JSoup document
    private static final String TITLE_PARAMS =
            """
            {"title": ["//TITLE", "//META[@name='title']/@content"]}
            """;

    private static Metadata filterTitle(String html) throws IOException {
        XPathFilter filter = new XPathFilter();
        filter.configure(new HashMap<>(), new ObjectMapper().readTree(TITLE_PARAMS));
        String url = "https://example.com";
        ParseResult parse = new ParseResult();
        org.jsoup.nodes.Document doc = Jsoup.parse(html, url);
        filter.filter(
                url,
                html.getBytes(StandardCharsets.UTF_8),
                DocumentFragmentBuilder.fromJsoup(doc),
                parse);
        return parse.getOrCreate(url).getMetadata();
    }

    @Test
    void testBlankValueFallsBackToNextExpression() throws IOException {
        Metadata metadata =
                filterTitle(
                        "<html><head><title> </title>"
                                + "<meta name=\"title\" content=\"Fallback title\"></head>"
                                + "<body></body></html>");
        Assertions.assertArrayEquals(new String[] {"Fallback title"}, metadata.getValues("title"));
    }

    @Test
    void testBlankValuesAreNotStored() throws IOException {
        Metadata metadata =
                filterTitle(
                        "<html><head><title></title>"
                                + "<meta name=\"title\" content=\"  \"></head>"
                                + "<body></body></html>");
        Assertions.assertNull(metadata.getValues("title"));
    }
}
