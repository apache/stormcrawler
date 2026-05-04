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

package org.apache.stormcrawler.protocol.playwright.parsefilter;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.apache.stormcrawler.parse.ParseResult;
import org.apache.stormcrawler.protocol.playwright.HttpProtocol;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class JsRenderingDetectorTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private JsRenderingDetector detector(final String paramJson) throws Exception {
        final JsRenderingDetector d = new JsRenderingDetector();
        d.configure(Map.of(), MAPPER.readTree(paramJson));
        return d;
    }

    private ParseResult applyTo(final JsRenderingDetector d, final String url, final String html) {
        final ParseResult parse = new ParseResult();
        d.filter(url, html.getBytes(StandardCharsets.UTF_8), null, parse);
        return parse;
    }

    @Test
    void detectsReactByDataReactRoot() throws Exception {
        final JsRenderingDetector d = detector("{}");
        final ParseResult p =
                applyTo(d, "u", "<html><body><div data-reactroot></div></body></html>");
        Assertions.assertEquals("playwright", p.get("u").getMetadata().getFirstValue("fetch.with"));
        Assertions.assertTrue(
                p.get("u")
                        .getMetadata()
                        .getFirstValue("fetch.with.reason")
                        .startsWith("fingerprint:"));
    }

    @Test
    void detectsAngularByNgVersion() throws Exception {
        final JsRenderingDetector d = detector("{}");
        final ParseResult p =
                applyTo(
                        d,
                        "u",
                        "<html><body><app-root ng-version=\"17.0.0\"></app-root></body></html>");
        Assertions.assertEquals("playwright", p.get("u").getMetadata().getFirstValue("fetch.with"));
    }

    @Test
    void detectsNoscriptJsRequired() throws Exception {
        final JsRenderingDetector d = detector("{}");
        final String html =
                "<html><body><noscript>Please enable JavaScript to view this site.</noscript></body></html>";
        final ParseResult p = applyTo(d, "u", html);
        Assertions.assertEquals(
                "noscript-js-required",
                p.get("u").getMetadata().getFirstValue("fetch.with.reason"));
    }

    @Test
    void detectsEmptySpaRoot() throws Exception {
        final JsRenderingDetector d = detector("{}");
        final ParseResult p =
                applyTo(
                        d,
                        "u",
                        "<html><body><div id=\"root\"></div><script src=\"/app.js\"></script></body></html>");
        Assertions.assertTrue(
                p.get("u")
                        .getMetadata()
                        .getFirstValue("fetch.with.reason")
                        .startsWith("empty-root:"));
    }

    @Test
    void thinContentWithScriptIsFlagged() throws Exception {
        final JsRenderingDetector d = detector("{\"minTextLength\":50,\"minOutlinks\":1}");
        // No fingerprints, no empty-root pattern; only the outcome-based path fires
        final String html =
                "<html><body><p>Hi</p><script>console.log('app')</script></body></html>";
        final ParseResult p = applyTo(d, "u", html);
        Assertions.assertNotNull(p.get("u").getMetadata().getFirstValue("fetch.with"));
        Assertions.assertTrue(
                p.get("u")
                        .getMetadata()
                        .getFirstValue("fetch.with.reason")
                        .startsWith("thin-content:"));
    }

    @Test
    void plainHtmlIsNotFlagged() throws Exception {
        final JsRenderingDetector d = detector("{}");
        final String html =
                "<html><body><p>"
                        + "A".repeat(1000)
                        + "</p><a href='/x'>x</a><a href='/y'>y</a><a href='/z'>z</a></body></html>";
        final ParseResult p = applyTo(d, "u", html);
        Assertions.assertNull(p.get("u").getMetadata().getFirstValue("fetch.with"));
    }

    @Test
    void shortPageWithoutScriptIsNotFlagged() throws Exception {
        // outcome-based fallback is gated on at least one <script>
        final JsRenderingDetector d = detector("{}");
        final ParseResult p = applyTo(d, "u", "<html><body><p>404 Not Found</p></body></html>");
        Assertions.assertNull(p.get("u").getMetadata().getFirstValue("fetch.with"));
    }

    @Test
    void skipsIfAlreadyFetchedByPlaywright() throws Exception {
        final JsRenderingDetector d = detector("{}");
        final ParseResult parse = new ParseResult();
        // simulate metadata coming from a Playwright fetch
        parse.get("u").getMetadata().setValue(HttpProtocol.MD_KEY_END, "2026-05-04T00:00:00Z");
        d.filter("u", "<div data-reactroot></div>".getBytes(StandardCharsets.UTF_8), null, parse);
        Assertions.assertNull(parse.get("u").getMetadata().getFirstValue("fetch.with"));
    }

    @Test
    void skipsIfAlreadyFlagged() throws Exception {
        final JsRenderingDetector d = detector("{}");
        final ParseResult parse = new ParseResult();
        parse.get("u").getMetadata().setValue("fetch.with", "playwright");
        d.filter(
                "u",
                "<html><body><p>some content</p></body></html>".getBytes(StandardCharsets.UTF_8),
                null,
                parse);
        // not overwritten, no reason added
        Assertions.assertEquals(
                "playwright", parse.get("u").getMetadata().getFirstValue("fetch.with"));
        Assertions.assertNull(parse.get("u").getMetadata().getFirstValue("fetch.with.reason"));
    }

    @Test
    void customMetadataKeyAndValue() throws Exception {
        final JsRenderingDetector d =
                detector("{\"metadataKey\":\"render\",\"metadataValue\":\"yes\"}");
        final ParseResult p =
                applyTo(d, "u", "<html><body><div data-reactroot></div></body></html>");
        Assertions.assertEquals("yes", p.get("u").getMetadata().getFirstValue("render"));
        Assertions.assertNotNull(p.get("u").getMetadata().getFirstValue("render.reason"));
    }

    @Test
    void requiredMessageMatchesAnywhere() throws Exception {
        // free-form list, fires outside <noscript> on a substring match
        final JsRenderingDetector d =
                detector(
                        "{\"requiredMessages\":[\"Loading...\",\"[object Object]\","
                                + "\"This page requires JavaScript\"]}");
        final ParseResult p =
                applyTo(
                        d,
                        "u",
                        "<html><body><div id=\"app\"><span>Loading...</span></div></body></html>");
        Assertions.assertEquals("playwright", p.get("u").getMetadata().getFirstValue("fetch.with"));
        Assertions.assertEquals(
                "required-message:Loading...",
                p.get("u").getMetadata().getFirstValue("fetch.with.reason"));
    }

    @Test
    void requiredMessageNoHitDoesNotFlag() throws Exception {
        final JsRenderingDetector d = detector("{\"requiredMessages\":[\"Loading...\"]}");
        final ParseResult p =
                applyTo(
                        d,
                        "u",
                        "<html><body><p>"
                                + "A".repeat(1000)
                                + "</p><a href='/x'>x</a><a href='/y'>y</a><a href='/z'>z</a></body></html>");
        Assertions.assertNull(p.get("u").getMetadata().getFirstValue("fetch.with"));
    }
}
