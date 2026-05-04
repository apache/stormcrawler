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
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Pure unit tests for the configure() side of each built-in {@code PageAction}. They do not need a
 * browser so they always run.
 */
class ActionConfigureTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Map<String, Object> EMPTY_CONF = Map.of();

    private static JsonNode params(final String json) throws Exception {
        return MAPPER.readTree(json);
    }

    // --- ExpandClickablesAction ---

    @Test
    void expandClickablesRequiresRootAndBody() throws Exception {
        final ExpandClickablesAction action = new ExpandClickablesAction();
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> action.configure(EMPTY_CONF, params("{\"selectors\":[\".x\"]}")));
    }

    @Test
    void expandClickablesAcceptsValidParams() throws Exception {
        final ExpandClickablesAction action = new ExpandClickablesAction();
        action.configure(
                EMPTY_CONF,
                params(
                        "{\"selectors\":[\".tab .header\"],\"root\":\".tab\",\"body\":\".tab-body\",\"waitMs\":50}"));
    }

    // --- EvaluateAction ---

    @Test
    void evaluateRequiresNonEmptyExpressions() throws Exception {
        final EvaluateAction action = new EvaluateAction();
        Assertions.assertThrows(
                IllegalArgumentException.class, () -> action.configure(EMPTY_CONF, params("{}")));
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> action.configure(EMPTY_CONF, params("{\"expressions\":[]}")));
    }

    @Test
    void evaluateAcceptsExpressions() throws Exception {
        final EvaluateAction action = new EvaluateAction();
        action.configure(
                EMPTY_CONF,
                params("{\"expressions\":[\"window.location.href\"],\"keyPrefix\":\"e\"}"));
    }

    // --- ScrollToBottomAction ---

    @Test
    void scrollToBottomAcceptsEmptyConfig() throws Exception {
        final ScrollToBottomAction action = new ScrollToBottomAction();
        action.configure(EMPTY_CONF, params("{}"));
    }

    @Test
    void scrollToBottomAcceptsOverrides() throws Exception {
        final ScrollToBottomAction action = new ScrollToBottomAction();
        action.configure(
                EMPTY_CONF, params("{\"waitMs\":100,\"maxSteps\":5,\"maxDurationMs\":2000}"));
    }

    // --- WaitForSelectorAction ---

    @Test
    void waitForSelectorRequiresSelector() throws Exception {
        final WaitForSelectorAction action = new WaitForSelectorAction();
        Assertions.assertThrows(
                IllegalArgumentException.class, () -> action.configure(EMPTY_CONF, params("{}")));
    }

    @Test
    void waitForSelectorRejectsUnknownState() throws Exception {
        final WaitForSelectorAction action = new WaitForSelectorAction();
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () ->
                        action.configure(
                                EMPTY_CONF,
                                params("{\"selector\":\"#x\",\"state\":\"sideways\"}")));
    }

    @Test
    void waitForSelectorAcceptsAllValidStates() throws Exception {
        for (final String state : new String[] {"attached", "detached", "visible", "hidden"}) {
            final WaitForSelectorAction action = new WaitForSelectorAction();
            action.configure(
                    EMPTY_CONF,
                    params(
                            "{\"selector\":\"#x\",\"state\":\""
                                    + state
                                    + "\",\"timeoutMs\":250,\"required\":true}"));
        }
    }

    // --- DismissOverlayAction ---

    @Test
    void dismissOverlayRequiresAtLeastOneList() throws Exception {
        final DismissOverlayAction action = new DismissOverlayAction();
        Assertions.assertThrows(
                IllegalArgumentException.class, () -> action.configure(EMPTY_CONF, params("{}")));
    }

    @Test
    void dismissOverlayAcceptsClickList() throws Exception {
        final DismissOverlayAction action = new DismissOverlayAction();
        action.configure(EMPTY_CONF, params("{\"selectors\":[\"#cookie-accept\"]}"));
    }

    @Test
    void dismissOverlayAcceptsRemoveListOnly() throws Exception {
        final DismissOverlayAction action = new DismissOverlayAction();
        action.configure(EMPTY_CONF, params("{\"removeSelectors\":[\".paywall\"]}"));
    }

    // --- ScreenshotAction ---

    @Test
    void screenshotAcceptsEmptyConfig() throws Exception {
        final ScreenshotAction action = new ScreenshotAction();
        action.configure(EMPTY_CONF, params("{}"));
    }

    @Test
    void screenshotRejectsUnknownType() throws Exception {
        final ScreenshotAction action = new ScreenshotAction();
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> action.configure(EMPTY_CONF, params("{\"type\":\"webp\"}")));
    }

    @Test
    void screenshotAcceptsPngAndJpeg() throws Exception {
        final ScreenshotAction png = new ScreenshotAction();
        png.configure(EMPTY_CONF, params("{\"type\":\"png\",\"fullPage\":true}"));

        final ScreenshotAction jpeg = new ScreenshotAction();
        jpeg.configure(
                EMPTY_CONF,
                params("{\"type\":\"jpeg\",\"quality\":80,\"metadataKey\":\"my.shot\"}"));
    }
}
