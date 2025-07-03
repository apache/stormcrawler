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
package org.apache.stormcrawler.bolt;

import static org.junit.jupiter.api.Assertions.assertThrows;

import crawlercommons.sitemaps.extension.Extension;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.http.HttpHeaders;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Values;
import org.apache.stormcrawler.Constants;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.TestUtil;
import org.apache.stormcrawler.parse.ParsingTester;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SiteMapParserBoltTest extends ParsingTester {

    @BeforeEach
    void setupParserBolt() {
        bolt = new SiteMapParserBolt();
        setupParserBolt(bolt);
    }

    @Test
    void testSitemapParsing() throws IOException {
        prepareParserBolt("test.parsefilters.json");
        Metadata metadata = new Metadata();
        // specify that it is a sitemap file
        metadata.setValue(SiteMapParserBolt.isSitemapKey, "true");
        // and its mime-type
        metadata.setValue(HttpHeaders.CONTENT_TYPE, "application/xml");
        parse("https://stormcrawler.apache.org/sitemap.xml", "stormcrawler.sitemap.xml", metadata);
        Assertions.assertEquals(7, output.getEmitted(Constants.StatusStreamName).size());
        // TODO test that the new links have the right metadata
        List<Object> fields = output.getEmitted(Constants.StatusStreamName).get(0);
        Assertions.assertEquals(3, fields.size());
    }

    @Test
    void testSitemapIndexParsing() throws IOException {
        prepareParserBolt("test.parsefilters.json");
        Metadata metadata = new Metadata();
        // specify that it is a sitemap file
        metadata.setValue(SiteMapParserBolt.isSitemapKey, "true");
        // and its mime-type
        metadata.setValue(HttpHeaders.CONTENT_TYPE, "application/xml");
        parse(
                "https://stormcrawler.apache.org/sitemap-index.xml",
                "stormcrawler.sitemap.index.xml",
                metadata);
        for (List<Object> fields : output.getEmitted(Constants.StatusStreamName)) {
            Metadata parsedMetadata = (Metadata) fields.get(1);
            Assertions.assertEquals(
                    "true", parsedMetadata.getFirstValue(SiteMapParserBolt.isSitemapKey));
        }
        Assertions.assertEquals(5, output.getEmitted(Constants.StatusStreamName).size());
    }

    @Test
    void testGzipSitemapParsing() throws IOException {
        prepareParserBolt("test.parsefilters.json");
        Metadata metadata = new Metadata();
        // specify that it is a sitemap file
        metadata.setValue(SiteMapParserBolt.isSitemapKey, "true");
        parse(
                "https://stormcrawler.apache.org/sitemap.xml.gz",
                "stormcrawler.sitemap.xml.gz",
                metadata);
        Assertions.assertEquals(7, output.getEmitted(Constants.StatusStreamName).size());
    }

    @Test
    void testSitemapParsingWithImageExtensions() throws IOException {
        Map<String, Object> parserConfig = new HashMap<>();
        parserConfig.put("sitemap.extensions", Collections.singletonList(Extension.IMAGE.name()));
        prepareParserBolt("test.parsefilters.json", parserConfig);
        Metadata metadata = new Metadata();
        // specify that it is a sitemap file
        metadata.setValue(SiteMapParserBolt.isSitemapKey, "true");
        // and its mime-type
        metadata.setValue(HttpHeaders.CONTENT_TYPE, "application/xml");
        parse(
                "https://stormcrawler.apache.org/sitemap.xml",
                "stormcrawler.sitemap.extensions.image.xml",
                metadata);
        Values values = (Values) output.getEmitted(Constants.StatusStreamName).get(0);
        Metadata parsedMetadata = (Metadata) values.get(1);
        assertImageAttributes(parsedMetadata);
    }

    @Test
    void testSitemapParsingWithMobileExtensions() throws IOException {
        Map<String, Object> parserConfig = new HashMap<>();
        parserConfig.put("sitemap.extensions", Collections.singletonList(Extension.MOBILE.name()));
        prepareParserBolt("test.parsefilters.json", parserConfig);
        Metadata metadata = new Metadata();
        // specify that it is a sitemap file
        metadata.setValue(SiteMapParserBolt.isSitemapKey, "true");
        // and its mime-type
        metadata.setValue(HttpHeaders.CONTENT_TYPE, "application/xml");
        parse(
                "https://stormcrawler.apache.org/sitemap.xml",
                "stormcrawler.sitemap.extensions.mobile.xml",
                metadata);
        Values values = (Values) output.getEmitted(Constants.StatusStreamName).get(0);
        Metadata parsedMetadata = (Metadata) values.get(1);
        assertMobileAttributes(parsedMetadata);
    }

    @Test
    void testSitemapParsingWithLinkExtensions() throws IOException {
        Map<String, Object> parserConfig = new HashMap<>();
        parserConfig.put("sitemap.extensions", Collections.singletonList(Extension.LINKS.name()));
        prepareParserBolt("test.parsefilters.json", parserConfig);
        Metadata metadata = new Metadata();
        // specify that it is a sitemap file
        metadata.setValue(SiteMapParserBolt.isSitemapKey, "true");
        // and its mime-type
        metadata.setValue(HttpHeaders.CONTENT_TYPE, "application/xml");
        parse(
                "https://stormcrawler.apache.org/sitemap.xml",
                "stormcrawler.sitemap.extensions.links.xml",
                metadata);
        Values values = (Values) output.getEmitted(Constants.StatusStreamName).get(0);
        Metadata parsedMetadata = (Metadata) values.get(1);
        assertLinksAttributes(parsedMetadata);
    }

    @Test
    void testSitemapParsingWithNewsExtensions() throws IOException {
        Map<String, Object> parserConfig = new HashMap<>();
        parserConfig.put("sitemap.extensions", Collections.singletonList(Extension.NEWS.name()));
        prepareParserBolt("test.parsefilters.json", parserConfig);
        Metadata metadata = new Metadata();
        // specify that it is a sitemap file
        metadata.setValue(SiteMapParserBolt.isSitemapKey, "true");
        // and its mime-type
        metadata.setValue(HttpHeaders.CONTENT_TYPE, "application/xml");
        parse(
                "https://stormcrawler.apache.org/sitemap.xml",
                "stormcrawler.sitemap.extensions.news.xml",
                metadata);
        Values values = (Values) output.getEmitted(Constants.StatusStreamName).get(0);
        Metadata parsedMetadata = (Metadata) values.get(1);
        assertNewsAttributes(parsedMetadata);
    }

    @Test
    void testSitemapParsingWithVideoExtensions() throws IOException {
        Map<String, Object> parserConfig = new HashMap<>();
        parserConfig.put("sitemap.extensions", Collections.singletonList(Extension.VIDEO.name()));
        prepareParserBolt("test.parsefilters.json", parserConfig);
        Metadata metadata = new Metadata();
        // specify that it is a sitemap file
        metadata.setValue(SiteMapParserBolt.isSitemapKey, "true");
        // and its mime-type
        metadata.setValue(HttpHeaders.CONTENT_TYPE, "application/xml");
        parse(
                "https://stormcrawler.apache.org/sitemap.xml",
                "stormcrawler.sitemap.extensions.video.xml",
                metadata);
        Values values = (Values) output.getEmitted(Constants.StatusStreamName).get(0);
        Metadata parsedMetadata = (Metadata) values.get(1);
        assertVideoAttributes(parsedMetadata);
    }

    @Test
    void testSitemapParsingWithAllExtensions() throws IOException {
        Map<String, Object> parserConfig = new HashMap<>();
        parserConfig.put(
                "sitemap.extensions",
                Arrays.asList(
                        Extension.MOBILE.name(),
                        Extension.NEWS.name(),
                        Extension.LINKS.name(),
                        Extension.VIDEO.name(),
                        Extension.IMAGE.name()));
        prepareParserBolt("test.parsefilters.json", parserConfig);
        Metadata metadata = new Metadata();
        // specify that it is a sitemap file
        metadata.setValue(SiteMapParserBolt.isSitemapKey, "true");
        // and its mime-type
        metadata.setValue(HttpHeaders.CONTENT_TYPE, "application/xml");
        parse(
                "https://stormcrawler.apache.org/sitemap.xml",
                "stormcrawler.sitemap.extensions.all.xml",
                metadata);
        Values values = (Values) output.getEmitted(Constants.StatusStreamName).get(0);
        Metadata parsedMetadata = (Metadata) values.get(1);
        assertImageAttributes(parsedMetadata);
        assertNewsAttributes(parsedMetadata);
        assertVideoAttributes(parsedMetadata);
        assertLinksAttributes(parsedMetadata);
        assertMobileAttributes(parsedMetadata);
    }

    @Test
    void testSitemapParsingWithIllegalExtensionConfigured() throws IOException {
        assertThrows(
                IllegalArgumentException.class,
                () -> {
                    Map<String, Object> parserConfig = new HashMap<>();
                    parserConfig.put("sitemap.extensions", Arrays.asList("AUDIONEWSLINKS"));
                    prepareParserBolt("test.parsefilters.json", parserConfig);
                });
    }

    @Test
    void testSitemapParsingNoMT() throws IOException {
        Map<String, Object> parserConfig = new HashMap<>();
        parserConfig.put("sitemap.sniffContent", true);
        // sniff beyond ASLv2 license header
        parserConfig.put("sitemap.offset.guess", 1024);
        parserConfig.put("parsefilters.config.file", "test.parsefilters.json");
        bolt.prepare(
                parserConfig, TestUtil.getMockedTopologyContext(), new OutputCollector(output));
        Metadata metadata = new Metadata();
        // do not specify that it is a sitemap file
        // do not set the mimetype
        parse("https://stormcrawler.apache.org/sitemap.xml", "stormcrawler.sitemap.xml", metadata);
        Assertions.assertEquals(7, output.getEmitted(Constants.StatusStreamName).size());
        // TODO test that the new links have the right metadata
        List<Object> fields = output.getEmitted(Constants.StatusStreamName).get(0);
        Assertions.assertEquals(3, fields.size());
    }

    @Test
    void testNonSitemapParsing() throws IOException {
        prepareParserBolt("test.parsefilters.json");
        // do not specify that it is a sitemap file
        parse("https://stormcrawler.apache.org", "stormcrawler.apache.org.html", new Metadata());
        Assertions.assertEquals(1, output.getEmitted().size());
    }

    private void assertNewsAttributes(Metadata metadata) {
        long numAttributes = metadata.keySet(Extension.NEWS.name() + ".").size();
        Assertions.assertEquals(7, numAttributes);
        Assertions.assertEquals(
                "The Example Times", metadata.getFirstValue(Extension.NEWS.name() + "." + "name"));
        Assertions.assertEquals(
                "en", metadata.getFirstValue(Extension.NEWS.name() + "." + "language"));
        Assertions.assertArrayEquals(
                new String[] {"PressRelease", "Blog"},
                metadata.getValues(Extension.NEWS.name() + "." + "genres"));
        Assertions.assertEquals(
                "2008-12-23T00:00Z",
                metadata.getFirstValue(Extension.NEWS.name() + "." + "publication_date"));
        Assertions.assertEquals(
                "Companies A, B in Merger Talks",
                metadata.getFirstValue(Extension.NEWS.name() + "." + "title"));
        Assertions.assertArrayEquals(
                new String[] {"business", "merger", "acquisition", "A", "B"},
                metadata.getValues(Extension.NEWS.name() + "." + "keywords"));
        Assertions.assertArrayEquals(
                new String[] {"NASDAQ:A", "NASDAQ:B"},
                metadata.getValues(Extension.NEWS.name() + "." + "stock_tickers"));
    }

    private void assertImageAttributes(Metadata metadata) {
        long numAttributes = metadata.keySet(Extension.IMAGE.name() + ".").size();
        Assertions.assertEquals(5, numAttributes);
        Assertions.assertEquals(
                "This is the caption.",
                metadata.getFirstValue(Extension.IMAGE.name() + "." + "caption"));
        Assertions.assertEquals(
                "http://example.com/photo.jpg",
                metadata.getFirstValue(Extension.IMAGE.name() + "." + "loc"));
        Assertions.assertEquals(
                "Example photo shot in Limerick, Ireland",
                metadata.getFirstValue(Extension.IMAGE.name() + "." + "title"));
        Assertions.assertEquals(
                "https://creativecommons.org/licenses/by/4.0/legalcode",
                metadata.getFirstValue(Extension.IMAGE.name() + "." + "license"));
        Assertions.assertEquals(
                "Limerick, Ireland",
                metadata.getFirstValue(Extension.IMAGE.name() + "." + "geo_location"));
    }

    private void assertLinksAttributes(Metadata metadata) {
        long numAttributes = metadata.keySet(Extension.LINKS.name() + ".").size();
        Assertions.assertEquals(3, numAttributes);
        Assertions.assertEquals(
                "alternate", metadata.getFirstValue(Extension.LINKS.name() + "." + "params.rel"));
        Assertions.assertEquals(
                "http://www.example.com/english/",
                metadata.getFirstValue(Extension.LINKS.name() + "." + "href"));
        Assertions.assertEquals(
                "en", metadata.getFirstValue(Extension.LINKS.name() + "." + "params.hreflang"));
    }

    private void assertVideoAttributes(Metadata metadata) {
        long numAttributes = metadata.keySet(Extension.VIDEO.name() + ".").size();
        Assertions.assertEquals(20, numAttributes);
        Assertions.assertEquals(
                "http://www.example.com/thumbs/123.jpg",
                metadata.getFirstValue(Extension.VIDEO.name() + "." + "thumbnail_loc"));
        Assertions.assertEquals(
                "Grilling steaks for summer",
                metadata.getFirstValue(Extension.VIDEO.name() + "." + "title"));
        Assertions.assertEquals(
                "Alkis shows you how to get perfectly done steaks every time",
                metadata.getFirstValue(Extension.VIDEO.name() + "." + "description"));
        Assertions.assertEquals(
                "http://www.example.com/video123.flv",
                metadata.getFirstValue(Extension.VIDEO.name() + "." + "content_loc"));
        Assertions.assertEquals(
                "http://www.example.com/videoplayer.swf?video=123",
                metadata.getFirstValue(Extension.VIDEO.name() + "." + "player_loc"));
        // Assert.assertEquals("600", metadata.getFirstValue(Extension.VIDEO.name() +
        // "." + "duration"));
        Assertions.assertEquals(
                "2009-11-05T19:20:30+08:00",
                metadata.getFirstValue(Extension.VIDEO.name() + "." + "expiration_date"));
        Assertions.assertEquals(
                "4.2", metadata.getFirstValue(Extension.VIDEO.name() + "." + "rating"));
        Assertions.assertEquals(
                "12345", metadata.getFirstValue(Extension.VIDEO.name() + "." + "view_count"));
        Assertions.assertEquals(
                "2007-11-05T19:20:30+08:00",
                metadata.getFirstValue(Extension.VIDEO.name() + "." + "publication_date"));
        Assertions.assertEquals(
                "true", metadata.getFirstValue(Extension.VIDEO.name() + "." + "family_friendly"));
        Assertions.assertArrayEquals(
                new String[] {"sample_tag1", "sample_tag2"},
                metadata.getValues(Extension.VIDEO.name() + "." + "tags"));
        Assertions.assertArrayEquals(
                new String[] {"IE", "GB", "US", "CA"},
                metadata.getValues(Extension.VIDEO.name() + "." + "allowed_countries"));
        Assertions.assertEquals(
                "http://cooking.example.com",
                metadata.getFirstValue(Extension.VIDEO.name() + "." + "gallery_loc"));
        Assertions.assertEquals(
                "value: 1.99, currency: EUR, type: own, resolution: null",
                metadata.getFirstValue(Extension.VIDEO.name() + "." + "prices"));
        Assertions.assertEquals(
                "true",
                metadata.getFirstValue(Extension.VIDEO.name() + "." + "requires_subscription"));
        Assertions.assertEquals(
                "GrillyMcGrillerson",
                metadata.getFirstValue(Extension.VIDEO.name() + "." + "uploader"));
        Assertions.assertEquals(
                "http://www.example.com/users/grillymcgrillerson",
                metadata.getFirstValue(Extension.VIDEO.name() + "." + "uploader_info"));
        Assertions.assertEquals(
                "false", metadata.getFirstValue(Extension.VIDEO.name() + "." + "is_live"));
    }

    private void assertMobileAttributes(Metadata metadata) {
        long numAttributes = metadata.keySet(Extension.MOBILE.name() + ".").size();
        Assertions.assertEquals(0, numAttributes);
    }
}
