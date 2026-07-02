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
import com.microsoft.playwright.options.ScreenshotType;
import java.util.Base64;
import java.util.Locale;
import java.util.Map;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.protocol.playwright.PageAction;
import org.jetbrains.annotations.NotNull;
import org.slf4j.LoggerFactory;

/**
 * Captures a screenshot of the page and stores it base64-encoded in the response metadata. Larger
 * crawls should write to a blob store instead — this action is intended for diagnostics, sample
 * runs, and small-volume use cases where carrying the image alongside the document is convenient.
 *
 * <h3>Parameters</h3>
 *
 * <ul>
 *   <li>{@code metadataKey} (optional, string, default {@code playwright.screenshot}): metadata key
 *       under which the base64 string is stored
 *   <li>{@code fullPage} (optional, bool, default false): capture the entire scrollable page
 *   <li>{@code type} (optional, string, default {@code png}): {@code png} or {@code jpeg}
 *   <li>{@code quality} (optional, int, 0-100): only honoured for {@code jpeg}
 * </ul>
 */
public class ScreenshotAction extends PageAction {

    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(ScreenshotAction.class);

    public static final String DEFAULT_METADATA_KEY = "playwright.screenshot";

    private String metadataKey = DEFAULT_METADATA_KEY;
    private boolean fullPage = false;
    private ScreenshotType type = ScreenshotType.PNG;
    private Integer quality;

    @Override
    public void configure(
            @NotNull final Map<String, Object> stormConf, @NotNull final JsonNode params) {
        if (params == null || params.isMissingNode() || params.isNull()) {
            return;
        }
        if (params.has("metadataKey")) {
            this.metadataKey = params.get("metadataKey").asText();
        }
        if (params.has("fullPage")) {
            this.fullPage = params.get("fullPage").asBoolean(false);
        }
        if (params.has("type")) {
            final String t = params.get("type").asText().toLowerCase(Locale.ROOT);
            switch (t) {
                case "jpeg":
                case "jpg":
                    this.type = ScreenshotType.JPEG;
                    break;
                case "png":
                    this.type = ScreenshotType.PNG;
                    break;
                default:
                    throw new IllegalArgumentException(
                            "Unknown screenshot type '" + t + "' (expected png or jpeg)");
            }
        }
        if (params.has("quality")) {
            this.quality = params.get("quality").asInt();
        }
    }

    @Override
    public void apply(
            @NotNull final Page page,
            @NotNull final String url,
            @NotNull final Metadata sourceMetadata,
            @NotNull final Metadata responseMetadata) {
        final Page.ScreenshotOptions options =
                new Page.ScreenshotOptions().setFullPage(fullPage).setType(type);
        if (type == ScreenshotType.JPEG && quality != null) {
            options.setQuality(quality);
        }
        try {
            final byte[] bytes = page.screenshot(options);
            responseMetadata.setValue(metadataKey, Base64.getEncoder().encodeToString(bytes));
        } catch (final Exception e) {
            LOG.debug("Screenshot failed for {}: {}", url, e.getMessage());
        }
    }
}
