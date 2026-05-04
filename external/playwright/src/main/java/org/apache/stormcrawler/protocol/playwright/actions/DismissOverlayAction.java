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
import java.util.List;
import java.util.Map;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.protocol.playwright.PageAction;
import org.jetbrains.annotations.NotNull;
import org.slf4j.LoggerFactory;

/**
 * Dismisses cookie banners, GDPR walls, paywalls, newsletter modals, etc. by clicking the first
 * matching element of each configured selector. Each click is independently bounded by {@code
 * timeoutMs}; missing elements and click failures are silently skipped, so it is safe to pass an
 * over-broad set of fallback selectors.
 *
 * <h3>Parameters</h3>
 *
 * <ul>
 *   <li>{@code selectors} (required, array of strings)
 *   <li>{@code timeoutMs} (optional, int, default 1500): per-click timeout
 *   <li>{@code removeSelectors} (optional, array of strings): elements matching these selectors are
 *       removed from the DOM after the clicks (useful for sticky overlays that don't have a close
 *       button)
 * </ul>
 */
public class DismissOverlayAction extends PageAction {

    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(DismissOverlayAction.class);

    private static final String REMOVE_JS = "el => el.remove()";

    private List<String> selectors = List.of();
    private List<String> removeSelectors = List.of();
    private int timeoutMs = 1500;

    @Override
    public void configure(
            @NotNull final Map<String, Object> stormConf, @NotNull final JsonNode params) {
        if (params == null || params.isMissingNode() || params.isNull()) {
            return;
        }
        this.selectors = readStringArray(params, "selectors");
        this.removeSelectors = readStringArray(params, "removeSelectors");
        if (params.has("timeoutMs")) {
            this.timeoutMs = params.get("timeoutMs").asInt(this.timeoutMs);
        }
        if (selectors.isEmpty() && removeSelectors.isEmpty()) {
            throw new IllegalArgumentException(
                    "DismissOverlayAction requires non-empty 'selectors' or 'removeSelectors'");
        }
    }

    @Override
    public void apply(
            @NotNull final Page page,
            @NotNull final String url,
            @NotNull final Metadata sourceMetadata,
            @NotNull final Metadata responseMetadata) {
        for (final String selector : selectors) {
            try {
                final ElementHandle handle = page.querySelector(selector);
                if (handle == null) {
                    continue;
                }
                handle.click(new ElementHandle.ClickOptions().setTimeout(timeoutMs));
            } catch (final Exception e) {
                LOG.debug("Could not click overlay {} on {}: {}", selector, url, e.getMessage());
            }
        }
        for (final String selector : removeSelectors) {
            try {
                for (final ElementHandle handle : page.querySelectorAll(selector)) {
                    handle.evaluate(REMOVE_JS);
                }
            } catch (final Exception e) {
                LOG.debug("Could not remove overlay {} on {}: {}", selector, url, e.getMessage());
            }
        }
    }

    private static List<String> readStringArray(final JsonNode params, final String key) {
        final JsonNode node = params.get(key);
        if (node == null || !node.isArray()) {
            return List.of();
        }
        final List<String> list = new ArrayList<>(node.size());
        node.forEach(n -> list.add(n.asText()));
        return list;
    }
}
