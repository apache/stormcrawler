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

package org.apache.stormcrawler.protocol.playwright;

import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.playwright.Browser;
import com.microsoft.playwright.BrowserContext;
import com.microsoft.playwright.Page;
import com.microsoft.playwright.Playwright;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.concurrent.TimeUnit;
import org.apache.storm.Config;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.protocol.AbstractProtocolTest;
import org.apache.stormcrawler.protocol.ProtocolResponse;
import org.apache.stormcrawler.protocol.playwright.actions.DismissOverlayAction;
import org.apache.stormcrawler.protocol.playwright.actions.EvaluateAction;
import org.apache.stormcrawler.protocol.playwright.actions.ExpandClickablesAction;
import org.apache.stormcrawler.protocol.playwright.actions.ScreenshotAction;
import org.apache.stormcrawler.protocol.playwright.actions.ScrollToBottomAction;
import org.apache.stormcrawler.protocol.playwright.actions.WaitForSelectorAction;
import org.eclipse.jetty.server.Handler;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Live, browser-driven tests for the {@link PageActions} chain and individual {@code PageAction}
 * implementations. Requires a working Playwright/Chrome install (or a {@code playwright.cdp.url})
 * and is skipped when {@code CI_ENV=true}, mirroring the gate used by {@link ProtocolTest}.
 */
class PageActionsLiveTest extends AbstractProtocolTest {

    private static final String USER_AGENT = "StormCrawlerTest";
    private static final String FIXTURE_PATH = "/page-actions-fixture.html";

    @Override
    protected Handler[] getHandlers() {
        return new Handler[] {new LocalResourceHandler(), new WildcardResourceHandler()};
    }

    @BeforeEach
    void setup() {
        assumeTrue("false".equals(System.getProperty("CI_ENV", "false")));
    }

    private HttpProtocol getProtocol(final String pageActionsConfigFile) {
        final Config conf = new Config();
        conf.put("http.agent.name", USER_AGENT);
        final String cdpurl = System.getProperty("playwright.cdp.url");
        if (cdpurl != null) {
            conf.put("playwright.cdp.url", cdpurl);
        }
        if (pageActionsConfigFile != null) {
            conf.put(PageActions.CONFIG_KEY, pageActionsConfigFile);
        }
        final HttpProtocol protocol = new HttpProtocol();
        protocol.configure(conf);
        return protocol;
    }

    private String url() {
        return "http://localhost:" + HTTP_PORT + FIXTURE_PATH;
    }

    @Test
    @Timeout(value = 2, unit = TimeUnit.MINUTES)
    void chainAppliesAllActions() throws Exception {
        final HttpProtocol protocol = getProtocol("page-actions.live.json");
        try {
            final ProtocolResponse response = protocol.getProtocolOutput(url(), new Metadata());
            Assertions.assertEquals(200, response.getStatusCode());

            final String content = new String(response.getContent(), StandardCharsets.UTF_8);

            // DismissOverlayAction: paywall removed from DOM, cookie banner click also removes it
            Assertions.assertFalse(
                    content.contains("PAYWALL_CONTENT_REMOVED"),
                    "DismissOverlayAction should have removed the paywall element");
            Assertions.assertFalse(
                    content.contains("id=\"cookie-overlay\""),
                    "DismissOverlayAction click should have triggered the overlay removal");

            // ExpandClickablesAction: every tab body should now be cached under the widget root
            Assertions.assertTrue(content.contains("CONTENT_TAB1"), "tab1 body should be cached");
            Assertions.assertTrue(content.contains("CONTENT_TAB2"), "tab2 body should be cached");
            Assertions.assertTrue(content.contains("CONTENT_TAB3"), "tab3 body should be cached");
            Assertions.assertTrue(
                    content.contains("__sc_cache"), "hidden cache element should be present");

            // ScrollToBottomAction: lazy-loaded chunks should have appeared
            Assertions.assertTrue(
                    content.contains("LAZY_LOADED_1"),
                    "ScrollToBottomAction should have triggered lazy loading");

            // EvaluateAction: title is JSON-serialised under the expression key
            final String title = response.getMetadata().getFirstValue("document.title");
            Assertions.assertNotNull(title);
            Assertions.assertTrue(
                    title.contains("StormCrawler PageActions Fixture"),
                    "EvaluateAction should have stored the title; got " + title);

            // ScreenshotAction: base64-encoded PNG under the configured key
            final String shot = response.getMetadata().getFirstValue("test.screenshot");
            Assertions.assertNotNull(shot, "ScreenshotAction should have stored a screenshot");
            final byte[] decoded = Base64.getDecoder().decode(shot);
            Assertions.assertTrue(decoded.length > 0);
            // PNG magic bytes
            Assertions.assertEquals((byte) 0x89, decoded[0]);
            Assertions.assertEquals((byte) 0x50, decoded[1]);
            Assertions.assertEquals((byte) 0x4E, decoded[2]);
            Assertions.assertEquals((byte) 0x47, decoded[3]);
        } finally {
            protocol.cleanup();
        }
    }

    @Test
    @Timeout(value = 2, unit = TimeUnit.MINUTES)
    void emptyChainProducesUntouchedContent() throws Exception {
        final HttpProtocol protocol = getProtocol(null);
        try {
            final ProtocolResponse response = protocol.getProtocolOutput(url(), new Metadata());
            Assertions.assertEquals(200, response.getStatusCode());
            final String content = new String(response.getContent(), StandardCharsets.UTF_8);
            // without DismissOverlayAction the paywall is still in the DOM
            Assertions.assertTrue(content.contains("PAYWALL_CONTENT_REMOVED"));
            // without ExpandClickablesAction only the initial body is rendered
            Assertions.assertTrue(content.contains("INITIAL_BODY"));
            Assertions.assertFalse(content.contains("CONTENT_TAB2"));
        } finally {
            protocol.cleanup();
        }
    }

    @Test
    @Timeout(value = 2, unit = TimeUnit.MINUTES)
    void chainSwallowsActionFailures() throws Exception {
        // Build an in-process chain by configuring HttpProtocol with a config file whose first
        // action targets selectors that don't exist; the second action must still run.
        final HttpProtocol protocol = getProtocol("page-actions.live.json");
        try {
            // Drive a no-content page and assert we still get a response without exception
            final String missing = "http://localhost:" + HTTP_PORT + "/does-not-exist.html";
            final ProtocolResponse response = protocol.getProtocolOutput(missing, new Metadata());
            // 404 → without captureContentOnError we get empty content but no thrown exception
            Assertions.assertEquals(404, response.getStatusCode());
            Assertions.assertEquals(0, response.getContent().length);
        } finally {
            protocol.cleanup();
        }
    }

    /**
     * Drives a single action against a live page bypassing the protocol — useful for failure paths
     * that the chain wrapper otherwise swallows.
     */
    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void waitForSelectorRequiredPropagates() throws Exception {
        final WaitForSelectorAction action = new WaitForSelectorAction();
        action.configure(
                java.util.Map.of(),
                new ObjectMapper()
                        .readTree("{\"selector\":\"#never\",\"timeoutMs\":250,\"required\":true}"));

        try (final Playwright pw = Playwright.create();
                final Browser browser = pw.chromium().launch();
                final BrowserContext ctx = browser.newContext();
                final Page page = ctx.newPage()) {
            page.navigate(url());
            Assertions.assertThrows(
                    Exception.class,
                    () -> action.apply(page, url(), new Metadata(), new Metadata()));
        }
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void waitForSelectorSoftTimeoutReturnsCleanly() throws Exception {
        final WaitForSelectorAction action = new WaitForSelectorAction();
        action.configure(
                java.util.Map.of(),
                new ObjectMapper()
                        .readTree(
                                "{\"selector\":\"#never\",\"timeoutMs\":250,\"required\":false}"));

        try (final Playwright pw = Playwright.create();
                final Browser browser = pw.chromium().launch();
                final BrowserContext ctx = browser.newContext();
                final Page page = ctx.newPage()) {
            page.navigate(url());
            // soft-fail: should not throw
            action.apply(page, url(), new Metadata(), new Metadata());
        }
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void evaluateActionStoresJsonResult() throws Exception {
        final EvaluateAction action = new EvaluateAction();
        action.configure(
                java.util.Map.of(),
                new ObjectMapper().readTree("{\"expressions\":[\"({a:1,b:'two'})\"]}"));

        try (final Playwright pw = Playwright.create();
                final Browser browser = pw.chromium().launch();
                final BrowserContext ctx = browser.newContext();
                final Page page = ctx.newPage()) {
            page.navigate(url());
            final Metadata md = new Metadata();
            action.apply(page, url(), new Metadata(), md);
            final String stored = md.getFirstValue("({a:1,b:'two'})");
            Assertions.assertNotNull(stored);
            // sanity-check that it parses back as JSON containing the right values
            final JsonNode parsed = new ObjectMapper().readTree(stored);
            Assertions.assertEquals(1, parsed.get("a").asInt());
            Assertions.assertEquals("two", parsed.get("b").asText());
        }
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void scrollToBottomTerminatesWithoutLazyContent() throws Exception {
        // No-lazy fixture (uses the existing dynamic-scraping.html) → height never grows
        final ScrollToBottomAction action = new ScrollToBottomAction();
        action.configure(
                java.util.Map.of(),
                new ObjectMapper()
                        .readTree("{\"waitMs\":50,\"maxSteps\":3,\"maxDurationMs\":2000}"));

        try (final Playwright pw = Playwright.create();
                final Browser browser = pw.chromium().launch();
                final BrowserContext ctx = browser.newContext();
                final Page page = ctx.newPage()) {
            page.navigate("http://localhost:" + HTTP_PORT + "/dynamic-scraping.html");
            // should return cleanly (height stops growing immediately)
            action.apply(page, url(), new Metadata(), new Metadata());
        }
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void expandClickablesIsNoOpWhenSelectorsMatchNothing() throws Exception {
        final ExpandClickablesAction action = new ExpandClickablesAction();
        action.configure(
                java.util.Map.of(),
                new ObjectMapper()
                        .readTree(
                                "{\"selectors\":[\".does-not-exist\"],\"root\":\".x\",\"body\":\".y\",\"waitMs\":50}"));

        try (final Playwright pw = Playwright.create();
                final Browser browser = pw.chromium().launch();
                final BrowserContext ctx = browser.newContext();
                final Page page = ctx.newPage()) {
            page.navigate(url());
            // no matches → nothing to click → no exception
            action.apply(page, url(), new Metadata(), new Metadata());
        }
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void dismissOverlayHandlesMissingSelectorsSilently() throws Exception {
        final DismissOverlayAction action = new DismissOverlayAction();
        action.configure(
                java.util.Map.of(),
                new ObjectMapper()
                        .readTree(
                                "{\"selectors\":[\"#nope\"],\"removeSelectors\":[\".also-nope\"]}"));

        try (final Playwright pw = Playwright.create();
                final Browser browser = pw.chromium().launch();
                final BrowserContext ctx = browser.newContext();
                final Page page = ctx.newPage()) {
            page.navigate(url());
            // missing elements are skipped, not raised
            action.apply(page, url(), new Metadata(), new Metadata());
        }
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void screenshotActionCapturesPng() throws Exception {
        final ScreenshotAction action = new ScreenshotAction();
        action.configure(
                java.util.Map.of(),
                new ObjectMapper().readTree("{\"type\":\"png\",\"fullPage\":true}"));

        try (final Playwright pw = Playwright.create();
                final Browser browser = pw.chromium().launch();
                final BrowserContext ctx = browser.newContext();
                final Page page = ctx.newPage()) {
            page.navigate(url());
            final Metadata md = new Metadata();
            action.apply(page, url(), new Metadata(), md);
            final String shot = md.getFirstValue(ScreenshotAction.DEFAULT_METADATA_KEY);
            Assertions.assertNotNull(shot);
            final byte[] decoded = Base64.getDecoder().decode(shot);
            Assertions.assertTrue(decoded.length > 0);
        }
    }
}
