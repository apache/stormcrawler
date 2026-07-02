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

import com.fasterxml.jackson.databind.JsonNode;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.parse.ParseData;
import org.apache.stormcrawler.parse.ParseFilter;
import org.apache.stormcrawler.parse.ParseResult;
import org.apache.stormcrawler.protocol.playwright.HttpProtocol;
import org.apache.stormcrawler.util.CharsetIdentification;
import org.jetbrains.annotations.NotNull;
import org.slf4j.LoggerFactory;
import org.w3c.dom.DocumentFragment;

/**
 * Heuristic parse filter that flags URLs whose content looks like it requires JavaScript rendering,
 * intended to be paired with {@link org.apache.stormcrawler.protocol.DelegatorProtocol} so that
 * subsequent fetches of those URLs are routed to the Playwright protocol.
 *
 * <p>The filter sets a metadata key (default {@code fetch.with=playwright}) on URLs that match any
 * of the following signals, in cheapest-first order:
 *
 * <ol>
 *   <li>SPA framework fingerprints found in the raw HTML (Angular {@code ng-version}, React {@code
 *       data-reactroot}, Next.js {@code __NEXT_DATA__}, Nuxt {@code window.__NUXT__}, Svelte {@code
 *       data-svelte-h}, Astro islands, Angular {@code <router-outlet>}, ...).
 *   <li>{@code <noscript>} blocks containing language like "enable JavaScript".
 *   <li>An empty SPA hydration root: {@code <div id="root"></div>} / {@code #app} / {@code #__next}
 *       / {@code #__nuxt} with no children.
 *   <li>Outcome-based fallback: very short text and few outlinks despite at least one {@code
 *       <script>} tag being present.
 * </ol>
 *
 * <p>To avoid loops, detection is skipped when the URL was just fetched by Playwright (recognised
 * by the presence of {@link HttpProtocol#MD_KEY_END} in its metadata) or when the routing key has
 * already been set on the URL.
 *
 * <h3>Wiring</h3>
 *
 * Register the filter in {@code parsefilters.json}:
 *
 * <pre>{@code
 * {
 *   "class": "org.apache.stormcrawler.protocol.playwright.parsefilter.JsRenderingDetector",
 *   "name": "js-rendering-detector",
 *   "params": { "minTextLength": 200, "minOutlinks": 2 }
 * }
 * }</pre>
 *
 * Route on the metadata key it sets via {@code DelegatorProtocol}:
 *
 * <pre>{@code
 * http.protocol.implementation: "org.apache.stormcrawler.protocol.DelegatorProtocol"
 * https.protocol.implementation: "org.apache.stormcrawler.protocol.DelegatorProtocol"
 * protocol.delegator.config:
 *   - className: "org.apache.stormcrawler.protocol.playwright.HttpProtocol"
 *     filters:
 *       fetch.with: "playwright"
 *   - className: "org.apache.stormcrawler.protocol.okhttp.HttpProtocol"
 * }</pre>
 *
 * <p>Decisions stick on the URL via metadata persistence in the status backend, so the second time
 * the URL is fetched it is dispatched to Playwright. For host-wide propagation (so siblings of the
 * detected URL also get Playwright on first fetch) pair this with a host-keyed metadata transfer
 * scheme — that's intentionally out of scope here.
 *
 * <h3>Parameters</h3>
 *
 * <ul>
 *   <li>{@code metadataKey} (string, default {@code fetch.with}) — routing key set on hits
 *   <li>{@code metadataValue} (string, default {@code playwright}) — value to set
 *   <li>{@code minTextLength} (int, default 200) — outcome-based threshold for visible text
 *   <li>{@code minOutlinks} (int, default 2) — outcome-based threshold for extracted outlinks
 *   <li>{@code fingerprints} (string array) — substrings searched in raw HTML; replaces defaults
 *   <li>{@code emptyRootIds} (string array) — element IDs treated as empty SPA hydration roots
 *   <li>{@code requiredMessages} (string array, default empty) — additional substrings that, when
 *       found anywhere in the HTML, flag the URL. Use this for site-specific JS-required prompts
 *       and loader text that don't fit the noscript pattern (e.g. {@code "Loading..."}, {@code
 *       "[object Object]"}, {@code "Please enable cookies"}).
 *   <li>{@code skipIfMetadataPresent} (string, default {@link HttpProtocol#MD_KEY_END}) —
 *       short-circuit when this metadata key is set; defeat with empty string to always re-evaluate
 *   <li>{@code recordReason} (bool, default true) — also set {@code metadataKey + ".reason"}
 *       describing which signal fired (useful for triage)
 * </ul>
 */
public class JsRenderingDetector extends ParseFilter {

    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(JsRenderingDetector.class);

    private static final List<String> DEFAULT_FINGERPRINTS =
            List.of(
                    "ng-version=",
                    "data-reactroot",
                    "__NEXT_DATA__",
                    "window.__NUXT__",
                    "data-svelte-h=",
                    "data-vue-app",
                    "data-astro-cid",
                    "<router-outlet");

    private static final List<String> DEFAULT_EMPTY_ROOT_IDS =
            List.of("root", "app", "__next", "__nuxt");

    private static final Pattern NOSCRIPT_JS_REQUIRED =
            Pattern.compile(
                    "<noscript[^>]*>[\\s\\S]*?(enable\\s+JavaScript|requires?\\s+JavaScript|JavaScript\\s+is\\s+disabled)[\\s\\S]*?</noscript>",
                    Pattern.CASE_INSENSITIVE);

    private static final Pattern HAS_SCRIPT_TAG =
            Pattern.compile("<script\\b", Pattern.CASE_INSENSITIVE);

    private String metadataKey = "fetch.with";
    private String metadataValue = "playwright";
    private int minTextLength = 200;
    private int minOutlinks = 2;
    private List<String> fingerprints = DEFAULT_FINGERPRINTS;
    private List<String> emptyRootIds = DEFAULT_EMPTY_ROOT_IDS;
    private List<String> requiredMessages = List.of();
    private String skipIfMetadataPresent = HttpProtocol.MD_KEY_END;
    private boolean recordReason = true;

    private final List<Pattern> emptyRootPatterns = new ArrayList<>();

    @Override
    public void configure(
            @NotNull final Map<String, Object> stormConf, @NotNull final JsonNode params) {
        if (params != null && !params.isMissingNode() && !params.isNull()) {
            if (params.has("metadataKey")) {
                this.metadataKey = params.get("metadataKey").asText();
            }
            if (params.has("metadataValue")) {
                this.metadataValue = params.get("metadataValue").asText();
            }
            if (params.has("minTextLength")) {
                this.minTextLength = params.get("minTextLength").asInt(this.minTextLength);
            }
            if (params.has("minOutlinks")) {
                this.minOutlinks = params.get("minOutlinks").asInt(this.minOutlinks);
            }
            if (params.has("fingerprints")) {
                this.fingerprints = readStringArray(params.get("fingerprints"));
            }
            if (params.has("emptyRootIds")) {
                this.emptyRootIds = readStringArray(params.get("emptyRootIds"));
            }
            if (params.has("requiredMessages")) {
                this.requiredMessages = readStringArray(params.get("requiredMessages"));
            }
            if (params.has("skipIfMetadataPresent")) {
                this.skipIfMetadataPresent = params.get("skipIfMetadataPresent").asText();
            }
            if (params.has("recordReason")) {
                this.recordReason = params.get("recordReason").asBoolean(this.recordReason);
            }
        }
        emptyRootPatterns.clear();
        for (final String id : emptyRootIds) {
            // <div|main|section ... id="ID" ...>\s*</tag> — empty hydration root
            emptyRootPatterns.add(
                    Pattern.compile(
                            "<(?:div|main|section)\\b[^>]*\\bid\\s*=\\s*[\"']?"
                                    + Pattern.quote(id)
                                    + "[\"']?[^>]*>\\s*</(?:div|main|section)>",
                            Pattern.CASE_INSENSITIVE));
        }
    }

    @Override
    public boolean needsDOM() {
        return false;
    }

    @Override
    public void filter(
            final String url,
            final byte[] content,
            final DocumentFragment doc,
            final ParseResult parse) {
        if (content == null || content.length == 0) {
            return;
        }

        final Metadata md = parse.get(url).getMetadata();
        if (skipIfMetadataPresent != null
                && !skipIfMetadataPresent.isEmpty()
                && md.containsKey(skipIfMetadataPresent)) {
            return;
        }
        if (md.containsKey(metadataKey)) {
            return;
        }

        final String reason = detectReason(url, content, parse);
        if (reason != null) {
            md.setValue(metadataKey, metadataValue);
            if (recordReason) {
                md.setValue(metadataKey + ".reason", reason);
            }
            LOG.debug("Flagged {} for JS rendering ({})", url, reason);
        }
    }

    private String detectReason(final String url, final byte[] content, final ParseResult parse) {
        final Metadata md = parse.get(url).getMetadata();
        final String charsetName = CharsetIdentification.getCharsetFast(md, content, -1);
        java.nio.charset.Charset cs;
        try {
            cs = java.nio.charset.Charset.forName(charsetName);
        } catch (Exception e) {
            cs = StandardCharsets.UTF_8;
        }
        final String html = new String(content, cs);

        // 1. SPA framework fingerprints
        for (final String fp : fingerprints) {
            if (html.contains(fp)) {
                return "fingerprint:" + fp;
            }
        }

        // 2. <noscript> with explicit JS-required language
        if (NOSCRIPT_JS_REQUIRED.matcher(html).find()) {
            return "noscript-js-required";
        }

        // 2b. Free-form JS-required / loader / cookie-required messages
        for (final String msg : requiredMessages) {
            if (!msg.isEmpty() && html.contains(msg)) {
                return "required-message:" + msg;
            }
        }

        // 3. Empty SPA hydration root
        for (int i = 0; i < emptyRootPatterns.size(); i++) {
            if (emptyRootPatterns.get(i).matcher(html).find()) {
                return "empty-root:" + emptyRootIds.get(i);
            }
        }

        // 4. Outcome-based: gated on at least one <script> being present so we don't flag
        //    plain HTML stubs (e.g. very short error pages).
        if (!HAS_SCRIPT_TAG.matcher(html).find()) {
            return null;
        }

        final ParseData data = parse.get(url);
        final String text = data.getText();
        final int textLen = text == null ? 0 : text.trim().length();
        final int outlinks = parse.getOutlinks() == null ? 0 : parse.getOutlinks().size();
        if (textLen < minTextLength && outlinks < minOutlinks) {
            return "thin-content:text=" + textLen + ",outlinks=" + outlinks;
        }
        return null;
    }

    private static List<String> readStringArray(final JsonNode node) {
        if (node == null || !node.isArray()) {
            return List.of();
        }
        final List<String> list = new ArrayList<>(node.size());
        node.forEach(n -> list.add(n.asText()));
        return list;
    }
}
