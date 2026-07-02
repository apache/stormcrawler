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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.playwright.Page;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.stormcrawler.JSONResource;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.util.ConfUtils;
import org.apache.stormcrawler.util.Configurable;
import org.jetbrains.annotations.NotNull;
import org.slf4j.LoggerFactory;

/**
 * Ordered chain of {@link PageAction}s loaded from a JSON configuration file. The file is
 * referenced by the {@code playwright.page.actions.config.file} configuration key and follows the
 * same shape as URL/parse filter configs:
 *
 * <pre>{@code
 * {
 *   "org.apache.stormcrawler.protocol.playwright.PageActions": [
 *     { "class": "...ExpandClickablesAction", "name": "tabs",
 *       "params": { "selectors": [".tab .header"], "root": ".tab", "body": ".tab-body" } }
 *   ]
 * }
 * }</pre>
 *
 * @see Configurable#createConfiguredInstance(Class, Class, Map, JsonNode)
 */
public class PageActions implements JSONResource {

    public static final String CONFIG_KEY = "playwright.page.actions.config.file";

    public static final PageActions emptyPageActions = new PageActions();

    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(PageActions.class);

    private PageAction[] actions = new PageAction[0];

    private final String configFile;

    private final Map<String, Object> stormConf;

    private PageActions() {
        this.configFile = null;
        this.stormConf = null;
    }

    public PageActions(final Map<String, Object> stormConf, final String configFile)
            throws IOException {
        this.configFile = configFile;
        this.stormConf = stormConf;
        try {
            loadJSONResources();
        } catch (final Exception e) {
            throw new IOException("Unable to build JSON object from file " + configFile, e);
        }
    }

    /** Loads and configures the chain from the storm config, or returns an empty chain. */
    public static PageActions fromConf(final Map<String, Object> stormConf) {
        final String configFile = ConfUtils.getString(stormConf, CONFIG_KEY);
        if (StringUtils.isNotBlank(configFile)) {
            try {
                return new PageActions(stormConf, configFile);
            } catch (final IOException e) {
                final String message =
                        "Exception caught while loading PageActions from " + configFile;
                LOG.error(message);
                throw new RuntimeException(message, e);
            }
        }
        return PageActions.emptyPageActions;
    }

    @Override
    public String getResourceFile() {
        return this.configFile;
    }

    @Override
    public void loadJSONResources(final InputStream inputStream) throws IOException {
        final ObjectMapper mapper = new ObjectMapper();
        final JsonNode confNode = mapper.readValue(inputStream, JsonNode.class);
        final List<PageAction> list =
                Configurable.createConfiguredInstance(
                        this.getClass(), PageAction.class, stormConf, confNode);
        actions = list.toArray(new PageAction[0]);
    }

    /**
     * Run every action in order. Failures are logged and swallowed so one bad action cannot abort
     * the rest of the chain.
     */
    public void apply(
            @NotNull final Page page,
            @NotNull final String url,
            @NotNull final Metadata sourceMetadata,
            @NotNull final Metadata responseMetadata) {
        for (final PageAction action : actions) {
            final long start = System.currentTimeMillis();
            try {
                action.apply(page, url, sourceMetadata, responseMetadata);
            } catch (final Exception e) {
                LOG.warn(
                        "PageAction {} ({}) failed for {}: {}",
                        action.getClass().getName(),
                        action.getName(),
                        url,
                        e.getMessage());
            }
            LOG.debug(
                    "PageAction {} took {} msec",
                    action.getClass().getName(),
                    System.currentTimeMillis() - start);
        }
    }

    public void cleanup() {
        for (final PageAction action : actions) {
            try {
                action.cleanup();
            } catch (final Exception e) {
                LOG.warn(
                        "PageAction {} cleanup failed: {}",
                        action.getClass().getName(),
                        e.getMessage());
            }
        }
    }

    public int size() {
        return actions.length;
    }
}
