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
import com.microsoft.playwright.Page;
import java.util.Map;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.protocol.playwright.PageAction;
import org.jetbrains.annotations.NotNull;
import org.slf4j.LoggerFactory;

/**
 * Repeatedly scrolls to the bottom of the page until the document height stops growing, the max
 * number of steps is reached, or {@code maxDurationMs} elapses. Useful for infinite-scroll feeds
 * that lazy-load on viewport entry.
 *
 * <h3>Parameters</h3>
 *
 * <ul>
 *   <li>{@code waitMs} (optional, int, default 500): time to wait after each scroll before
 *       re-measuring height
 *   <li>{@code maxSteps} (optional, int, default 20): hard cap on scroll iterations
 *   <li>{@code maxDurationMs} (optional, int, default 15000): hard cap on total time spent
 * </ul>
 */
public class ScrollToBottomAction extends PageAction {

    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(ScrollToBottomAction.class);

    private static final String SCROLL_JS = "() => window.scrollTo(0, document.body.scrollHeight)";
    private static final String HEIGHT_JS = "() => document.body.scrollHeight";

    private int waitMs = 500;
    private int maxSteps = 20;
    private int maxDurationMs = 15_000;

    @Override
    public void configure(
            @NotNull final Map<String, Object> stormConf, @NotNull final JsonNode params) {
        if (params == null || params.isMissingNode() || params.isNull()) {
            return;
        }
        if (params.has("waitMs")) {
            this.waitMs = params.get("waitMs").asInt(this.waitMs);
        }
        if (params.has("maxSteps")) {
            this.maxSteps = params.get("maxSteps").asInt(this.maxSteps);
        }
        if (params.has("maxDurationMs")) {
            this.maxDurationMs = params.get("maxDurationMs").asInt(this.maxDurationMs);
        }
    }

    @Override
    public void apply(
            @NotNull final Page page,
            @NotNull final String url,
            @NotNull final Metadata sourceMetadata,
            @NotNull final Metadata responseMetadata) {
        final long deadline = System.currentTimeMillis() + maxDurationMs;
        long previousHeight = -1;
        int steps = 0;
        while (steps < maxSteps && System.currentTimeMillis() < deadline) {
            final long height = ((Number) page.evaluate(HEIGHT_JS)).longValue();
            if (height == previousHeight) {
                break;
            }
            previousHeight = height;
            page.evaluate(SCROLL_JS);
            page.waitForTimeout(waitMs);
            steps++;
        }
        LOG.debug("ScrollToBottomAction stopped after {} steps on {}", steps, url);
    }
}
