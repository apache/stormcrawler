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
import java.util.List;
import org.apache.stormcrawler.Constants;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.parse.ParsingTester;
import org.apache.stormcrawler.persistence.Status;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * A page decides how the pipeline treats it only through its own metadata: content sniffing must
 * not promote an ordinary HTML page to a sitemap, a sitemap must not enrol URLs on other hosts,
 * and a sitemap marking that no longer parses must not make the URL unschedulable.
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

    /** A sitemap may only list URLs below its own location. */
    @Test
    void crossSubmittedUrlsAreNotDiscovered() throws IOException {
        prepareParserBolt("test.parsefilters.json");
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

    /** With sniffing enabled, an XML content type and the namespace still need to agree. */
    @Test
    void sniffingRequiresSitemapCompatibleContentType() throws IOException {
        prepareParserBolt("test.parsefilters.json");
        Metadata metadata = new Metadata();
        metadata.setValue("Content-Type".toLowerCase(), "text/html");
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
}
