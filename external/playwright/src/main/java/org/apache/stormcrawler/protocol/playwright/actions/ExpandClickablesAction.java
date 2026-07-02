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

package org.apache.stormcrawler.protocol.playwright.actions;

import com.fasterxml.jackson.databind.JsonNode;
import com.microsoft.playwright.ElementHandle;
import com.microsoft.playwright.Page;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.protocol.playwright.PageAction;
import org.jetbrains.annotations.NotNull;
import org.slf4j.LoggerFactory;

/**
 * Clicks every element matching a list of selectors and, after each click, clones the rendered body
 * container into a hidden cache under the same widget root. After the action runs, {@link
 * Page#content()} contains the HTML of every panel a tab/accordion would normally hide behind user
 * interaction — useful for SPAs whose visible markup depends on the active tab.
 *
 * <p>Anchor elements with an {@code href} are skipped to avoid following links.
 *
 * <h3>Parameters</h3>
 *
 * <ul>
 *   <li>{@code selectors} (required, array): selectors whose matches will be clicked
 *   <li>{@code root} (required, string): selector for the widget root containing both the clickable
 *       and its body
 *   <li>{@code body} (required, string): selector for the body container that should be cached
 *   <li>{@code waitMs} (optional, int, default 200): time to wait after each click before caching
 *   <li>{@code clickTimeoutMs} (optional, int, default 2000): per-click timeout
 * </ul>
 */
public class ExpandClickablesAction extends PageAction {

    private static final org.slf4j.Logger LOG =
            LoggerFactory.getLogger(ExpandClickablesAction.class);

    /**
     * Walks up to the configured root, finds the body, and appends a clone of it into a hidden
     * cache element under the same root. Repeated calls accumulate every clicked panel.
     */
    private static final String CACHE_BODY_JS =
            "(el, opts) => {\n"
                    + "  const widget = el.closest(opts.root);\n"
                    + "  if (!widget) return;\n"
                    + "  const bodyEl = widget.querySelector(opts.body);\n"
                    + "  if (!bodyEl) return;\n"
                    + "  let cache = widget.querySelector(':scope > .__sc_cache');\n"
                    + "  if (!cache) {\n"
                    + "    cache = document.createElement('div');\n"
                    + "    cache.className = '__sc_cache';\n"
                    + "    cache.setAttribute('aria-hidden', 'true');\n"
                    + "    cache.style.display = 'none';\n"
                    + "    widget.appendChild(cache);\n"
                    + "  }\n"
                    + "  cache.appendChild(bodyEl.cloneNode(true));\n"
                    + "}";

    private static final String IS_LINK_JS = "e => e.tagName === 'A' && !!e.getAttribute('href')";

    private List<String> selectors = List.of();
    private String rootSelector;
    private String bodySelector;
    private int waitMs = 200;
    private int clickTimeoutMs = 2000;

    @Override
    public void configure(
            @NotNull final Map<String, Object> stormConf, @NotNull final JsonNode params) {
        if (params == null || params.isMissingNode() || params.isNull()) {
            return;
        }
        final JsonNode sels = params.get("selectors");
        if (sels != null && sels.isArray()) {
            final List<String> list = new ArrayList<>(sels.size());
            sels.forEach(n -> list.add(n.asText()));
            this.selectors = list;
        }
        if (params.has("root")) {
            this.rootSelector = params.get("root").asText();
        }
        if (params.has("body")) {
            this.bodySelector = params.get("body").asText();
        }
        if (params.has("waitMs")) {
            this.waitMs = params.get("waitMs").asInt(this.waitMs);
        }
        if (params.has("clickTimeoutMs")) {
            this.clickTimeoutMs = params.get("clickTimeoutMs").asInt(this.clickTimeoutMs);
        }

        if (rootSelector == null || bodySelector == null) {
            throw new IllegalArgumentException(
                    "ExpandClickablesAction requires both 'root' and 'body' selectors");
        }
    }

    @Override
    public void apply(
            @NotNull final Page page,
            @NotNull final String url,
            @NotNull final Metadata sourceMetadata,
            @NotNull final Metadata responseMetadata) {
        final Map<String, String> opts = new HashMap<>();
        opts.put("root", rootSelector);
        opts.put("body", bodySelector);

        int clicked = 0;
        for (final String selector : selectors) {
            final List<ElementHandle> handles;
            try {
                handles = page.querySelectorAll(selector);
            } catch (final Exception e) {
                LOG.debug("Selector {} failed on {}: {}", selector, url, e.getMessage());
                continue;
            }
            for (final ElementHandle handle : handles) {
                try {
                    if (Boolean.TRUE.equals(handle.evaluate(IS_LINK_JS))) {
                        continue;
                    }
                    handle.scrollIntoViewIfNeeded();
                    handle.click(new ElementHandle.ClickOptions().setTimeout(clickTimeoutMs));
                    page.waitForTimeout(waitMs);
                    handle.evaluate(CACHE_BODY_JS, opts);
                    clicked++;
                } catch (final Exception e) {
                    LOG.debug("Skipping click on {} for {}: {}", selector, url, e.getMessage());
                }
            }
        }
        LOG.debug("ExpandClickablesAction clicked {} elements on {}", clicked, url);
    }
}
