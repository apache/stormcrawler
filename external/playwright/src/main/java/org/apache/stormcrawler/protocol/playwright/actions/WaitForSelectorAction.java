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
import com.microsoft.playwright.options.WaitForSelectorState;
import java.util.Locale;
import java.util.Map;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.protocol.playwright.PageAction;
import org.jetbrains.annotations.NotNull;
import org.slf4j.LoggerFactory;

/**
 * Waits for a selector to reach a given state before allowing the chain to proceed. By default a
 * timeout is treated as a soft failure (logged and swallowed) so the rest of the chain still runs;
 * set {@code required: true} to make it propagate.
 *
 * <h3>Parameters</h3>
 *
 * <ul>
 *   <li>{@code selector} (required, string)
 *   <li>{@code state} (optional, string, default {@code visible}): one of {@code attached}, {@code
 *       detached}, {@code visible}, {@code hidden}
 *   <li>{@code timeoutMs} (optional, int, default 5000)
 *   <li>{@code required} (optional, bool, default false): if true, a timeout aborts the action (and
 *       is logged and swallowed by the chain wrapper)
 * </ul>
 */
public class WaitForSelectorAction extends PageAction {

    private static final org.slf4j.Logger LOG =
            LoggerFactory.getLogger(WaitForSelectorAction.class);

    private String selector;
    private WaitForSelectorState state = WaitForSelectorState.VISIBLE;
    private int timeoutMs = 5000;
    private boolean required = false;

    @Override
    public void configure(
            @NotNull final Map<String, Object> stormConf, @NotNull final JsonNode params) {
        if (params == null || params.isMissingNode() || params.isNull()) {
            throw new IllegalArgumentException("WaitForSelectorAction requires 'selector'");
        }
        if (params.has("selector")) {
            this.selector = params.get("selector").asText();
        }
        if (params.has("timeoutMs")) {
            this.timeoutMs = params.get("timeoutMs").asInt(this.timeoutMs);
        }
        if (params.has("required")) {
            this.required = params.get("required").asBoolean(false);
        }
        if (params.has("state")) {
            final String s = params.get("state").asText().toUpperCase(Locale.ROOT);
            switch (s) {
                case "ATTACHED":
                    this.state = WaitForSelectorState.ATTACHED;
                    break;
                case "DETACHED":
                    this.state = WaitForSelectorState.DETACHED;
                    break;
                case "HIDDEN":
                    this.state = WaitForSelectorState.HIDDEN;
                    break;
                case "VISIBLE":
                    this.state = WaitForSelectorState.VISIBLE;
                    break;
                default:
                    throw new IllegalArgumentException(
                            "Unknown state '"
                                    + s
                                    + "' (expected attached/detached/visible/hidden)");
            }
        }
        if (selector == null || selector.isEmpty()) {
            throw new IllegalArgumentException("WaitForSelectorAction requires 'selector'");
        }
    }

    @Override
    public void apply(
            @NotNull final Page page,
            @NotNull final String url,
            @NotNull final Metadata sourceMetadata,
            @NotNull final Metadata responseMetadata)
            throws Exception {
        try {
            page.waitForSelector(
                    selector,
                    new Page.WaitForSelectorOptions().setState(state).setTimeout(timeoutMs));
        } catch (final Exception e) {
            if (required) {
                throw e;
            }
            LOG.debug(
                    "Selector {} did not reach state {} within {}ms on {}: {}",
                    selector,
                    state,
                    timeoutMs,
                    url,
                    e.getMessage());
        }
    }
}
