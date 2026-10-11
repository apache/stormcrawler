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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.stormcrawler.Constants;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.parse.ParsingTester;
import org.apache.stormcrawler.persistence.Status;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * A page decides how the pipeline treats it only through its own metadata: content sniffing must
 * not promote an ordinary HTML page to a sitemap, a sitemap must not enrol URLs on other hosts, and
 * a sitemap marking that no longer parses must not make the URL unschedulable.
 */
class SiteMapParserBoltCrossHostTest extends ParsingTester {

    @BeforeEach
    void setupParserBolt() {
        bolt = new SiteMapParserBolt();
        setupParserBolt(bolt);
    }

    private static byte[] xml(String body) {
        return body.getBytes(StandardCharsets.UTF_8);
    }

    /** A sitemap may only list URLs below its own location, with strict URL checking on. */
    @Test
    void crossSubmittedUrlsAreNotDiscovered() throws IOException {
        Map<String, Object> parserConfig = new HashMap<>();
        parserConfig.put("sitemap.strict", true);
        prepareParserBolt("test.parsefilters.json", parserConfig);
        Metadata metadata = new Metadata();
        metadata.setValue(SiteMapParserBolt.isSitemapKey, "true");
        parse(
                "https://a.example/sitemap.xml",
                xml(
                        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
                                + "<urlset xmlns=\"http://www.sitemaps.org/schemas/sitemap/0.9\">"
                                + "<url><loc>https://a.example/own-page</loc></url>"
                                + "<url><loc>https://b.example/other-page</loc></url>"
                                + "</urlset>"),
                metadata);
        List<List<Object>> emitted = output.getEmitted(Constants.StatusStreamName);
        for (List<Object> t : emitted) {
            Assertions.assertFalse(
                    t.get(0).toString().startsWith("https://b.example/"),
                    "discovered a URL on another host: " + t.get(0));
        }
    }

    /** Strict URL checking is off by default: the spec-violating cross-host URL passes. */
    @Test
    void crossSubmittedUrlsAreDiscoveredWithoutStrictChecking() throws IOException {
        prepareParserBolt("test.parsefilters.json");
        Metadata metadata = new Metadata();
        metadata.setValue(SiteMapParserBolt.isSitemapKey, "true");
        parse(
                "https://a.example/sitemap.xml",
                xml(
                        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
                                + "<urlset xmlns=\"http://www.sitemaps.org/schemas/sitemap/0.9\">"
                                + "<url><loc>https://b.example/other-page</loc></url>"
                                + "</urlset>"),
                metadata);
        List<List<Object>> emitted = output.getEmitted(Constants.StatusStreamName);
        Assertions.assertTrue(
                emitted.stream()
                        .anyMatch(t -> t.get(0).toString().startsWith("https://b.example/")),
                "the default must keep parsing sitemaps which cross hosts");
    }

    /** With strict checking on, a sitemap index may not pull in a sub-sitemap on another host. */
    @Test
    void crossHostSubSitemapIsNotDiscoveredWhenStrict() throws IOException {
        Map<String, Object> parserConfig = new HashMap<>();
        parserConfig.put("sitemap.strict", true);
        prepareParserBolt("test.parsefilters.json", parserConfig);
        Metadata metadata = new Metadata();
        metadata.setValue(SiteMapParserBolt.isSitemapKey, "true");
        parse(
                "https://a.example/sitemap_index.xml",
                xml(
                        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
                                + "<sitemapindex xmlns=\"http://www.sitemaps.org/schemas/sitemap/0.9\">"
                                + "<sitemap><loc>https://a.example/own-sitemap.xml</loc></sitemap>"
                                + "<sitemap><loc>https://b.example/other-sitemap.xml</loc></sitemap>"
                                + "</sitemapindex>"),
                metadata);
        List<List<Object>> emitted = output.getEmitted(Constants.StatusStreamName);
        for (List<Object> t : emitted) {
            Assertions.assertFalse(
                    t.get(0).toString().startsWith("https://b.example/"),
                    "discovered a sub-sitemap on another host: " + t.get(0));
        }
        Assertions.assertTrue(
                emitted.stream()
                        .anyMatch(t -> "https://a.example/own-sitemap.xml".equals(t.get(0))),
                "the index's own sub-sitemap must still be discovered");
    }

    /** A sitemap marked true but served with the wrong content type still parses. */
    @Test
    void sitemapServedAsHtmlStillParses() throws IOException {
        prepareParserBolt("test.parsefilters.json");
        Metadata metadata = new Metadata();
        metadata.setValue(SiteMapParserBolt.isSitemapKey, "true");
        metadata.setValue("content-type", "text/html");
        parse(
                "https://a.example/sitemap.xml",
                xml(
                        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
                                + "<urlset xmlns=\"http://www.sitemaps.org/schemas/sitemap/0.9\">"
                                + "<url><loc>https://a.example/own-page</loc></url>"
                                + "</urlset>"),
                metadata);
        List<List<Object>> emitted = output.getEmitted(Constants.StatusStreamName);
        Assertions.assertTrue(
                emitted.stream()
                        .anyMatch(t -> t.get(0).toString().equals("https://a.example/own-page")),
                "the declared sitemap must parse despite the wrong content type");
    }

    /** Content sniffing must not promote an ordinary HTML page to a sitemap. */
    @Test
    void htmlMentioningTheSitemapNamespaceIsNotASitemap() throws IOException {
        prepareParserBolt("test.parsefilters.json");
        Metadata metadata = new Metadata();
        parse(
                "https://a.example/page.html",
                xml(
                        "<html><body><a href=\"http://www.sitemaps.org/schemas/sitemap/0.9\">"
                                + "sitemaps</a>"
                                + "<urlset xmlns=\"http://www.sitemaps.org/schemas/sitemap/0.9\">"
                                + "<url><loc>https://b.example/other-page</loc></url></urlset>"
                                + "</body></html>"),
                metadata);
        Assertions.assertEquals(
                "false",
                metadata.getFirstValue(SiteMapParserBolt.isSitemapKey),
                "HTML page classified as a sitemap");
    }

    /** A page carrying isSitemap=true that does not parse must stay fetchable. */
    @Test
    void unparseableSitemapIsNotTerminalError() throws IOException {
        prepareParserBolt("test.parsefilters.json");
        Metadata metadata = new Metadata();
        metadata.setValue(SiteMapParserBolt.isSitemapKey, "true");
        parse("https://a.example/page.html", xml("<html><body>hello</body></html>"), metadata);
        List<List<Object>> emitted = output.getEmitted(Constants.StatusStreamName);
        Assertions.assertFalse(emitted.isEmpty());
        for (List<Object> t : emitted) {
            if (t.get(0).toString().equals("https://a.example/page.html")) {
                Assertions.assertNotEquals(Status.ERROR, t.get(2), "emitted as ERROR: " + t.get(0));
                Assertions.assertEquals(Status.FETCH_ERROR, t.get(2));
            }
        }
    }

    /**
     * With sniffing enabled, an HTML content type stops promotion even when the body mentions the
     * sitemap namespace.
     */
    @Test
    void sniffingRequiresSitemapCompatibleContentType() throws IOException {
        Map<String, Object> parserConfig = new HashMap<>();
        parserConfig.put("sitemap.sniffContent", true);
        prepareParserBolt("test.parsefilters.json", parserConfig);
        Metadata metadata = new Metadata();
        metadata.setValue("content-type", "text/html");
        parse(
                "https://a.example/page.html",
                xml(
                        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
                                + "<urlset xmlns=\"http://www.sitemaps.org/schemas/sitemap/0.9\">"
                                + "<url><loc>https://b.example/other-page</loc></url>"
                                + "</urlset>"),
                metadata);
        // the document was passed on to the parser bolt, not consumed as a sitemap
        Assertions.assertEquals(
                "false",
                metadata.getFirstValue(SiteMapParserBolt.isSitemapKey),
                "HTML content type sniffed into a sitemap");
    }

    /** XHTML is a page too: the media type carries xml in it, but must not be promoted. */
    @Test
    void xhtmlIsNotPromotedToSitemap() throws IOException {
        Map<String, Object> parserConfig = new HashMap<>();
        parserConfig.put("sitemap.sniffContent", true);
        prepareParserBolt("test.parsefilters.json", parserConfig);
        Metadata metadata = new Metadata();
        metadata.setValue("content-type", "application/xhtml+xml");
        parse(
                "https://a.example/page.xhtml",
                xml(
                        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
                                + "<html xmlns=\"http://www.w3.org/1999/xhtml\"><body>"
                                + "<urlset xmlns=\"http://www.sitemaps.org/schemas/sitemap/0.9\">"
                                + "<url><loc>https://b.example/other-page</loc></url>"
                                + "</urlset></body></html>"),
                metadata);
        Assertions.assertEquals(
                "false",
                metadata.getFirstValue(SiteMapParserBolt.isSitemapKey),
                "XHTML content type sniffed into a sitemap");
    }

    /** A parameterised HTML content type must not slip through on the parameters. */
    @Test
    void parameterisedHtmlContentTypeIsNotPromoted() throws IOException {
        Map<String, Object> parserConfig = new HashMap<>();
        parserConfig.put("sitemap.sniffContent", true);
        prepareParserBolt("test.parsefilters.json", parserConfig);
        Metadata metadata = new Metadata();
        metadata.setValue("content-type", "text/html; profile=xml");
        parse(
                "https://a.example/page.html",
                xml(
                        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
                                + "<urlset xmlns=\"http://www.sitemaps.org/schemas/sitemap/0.9\">"
                                + "<url><loc>https://b.example/other-page</loc></url>"
                                + "</urlset>"),
                metadata);
        Assertions.assertEquals(
                "false",
                metadata.getFirstValue(SiteMapParserBolt.isSitemapKey),
                "parameterised HTML content type sniffed into a sitemap");
    }

    /** The positive case: sniffing promotes an unmarked XML document with the right type. */
    @Test
    void xmlContentTypeWithNamespaceIsPromotedWhenSniffingEnabled() throws IOException {
        Map<String, Object> parserConfig = new HashMap<>();
        parserConfig.put("sitemap.sniffContent", true);
        prepareParserBolt("test.parsefilters.json", parserConfig);
        Metadata metadata = new Metadata();
        metadata.setValue("content-type", "application/xml");
        parse(
                "https://a.example/sitemap.xml",
                xml(
                        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
                                + "<urlset xmlns=\"http://www.sitemaps.org/schemas/sitemap/0.9\">"
                                + "<url><loc>https://a.example/own-page</loc></url>"
                                + "</urlset>"),
                metadata);
        List<List<Object>> emitted = output.getEmitted(Constants.StatusStreamName);
        Assertions.assertTrue(
                emitted.stream()
                        .anyMatch(t -> t.get(0).toString().equals("https://a.example/own-page")),
                "an unmarked XML document with the right type must be promoted and parsed");
    }
}
