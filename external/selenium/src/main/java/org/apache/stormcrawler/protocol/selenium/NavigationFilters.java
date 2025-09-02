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
package org.apache.stormcrawler.protocol.selenium;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang.StringUtils;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.protocol.ProtocolResponse;
import org.apache.stormcrawler.util.ConfUtils;
import org.apache.stormcrawler.util.Configurable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.openqa.selenium.remote.RemoteWebDriver;
import org.slf4j.LoggerFactory;

/**
 * Wrapper for the NavigationFilter defined in a JSON configuration
 *
 * @see org.apache.stormcrawler.util.Configurable#createConfiguredInstance(Class, Class, Map,
 *     JsonNode) for more information.
 */
public class NavigationFilters extends NavigationFilter {

    public static final NavigationFilters emptyNavigationFilters = new NavigationFilters();

    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(NavigationFilters.class);

    private NavigationFilter[] filters;

    private NavigationFilters() {
        filters = new NavigationFilter[0];
    }

    public @Nullable ProtocolResponse filter(
            @NotNull RemoteWebDriver driver, @NotNull Metadata metadata) {
        for (NavigationFilter filter : filters) {
            ProtocolResponse response = filter.filter(driver, metadata);
            if (response != null) return response;
        }
        return null;
    }

    /**
     * Loads and configure the NavigationFilters based on the storm config if there is one otherwise
     * returns an emptyNavigationFilters.
     */
    public static NavigationFilters fromConf(@NotNull Map<String, Object> stormConf) {
        String configfile = ConfUtils.getString(stormConf, "navigationfilters.config.file");
        if (StringUtils.isNotBlank(configfile)) {
            try {
                return new NavigationFilters(stormConf, configfile);
            } catch (IOException e) {
                String message =
                        "Exception caught while loading the NavigationFilters from " + configfile;
                LOG.error(message);
                throw new RuntimeException(message, e);
            }
        }

        return NavigationFilters.emptyNavigationFilters;
    }

    /**
     * loads the filters from a JSON configuration file
     *
     * @throws IOException
     */
    public NavigationFilters(@NotNull Map<String, Object> stormConf, @NotNull String configFile)
            throws IOException {
        // load the JSON configFile
        // build a JSON object out of it
        JsonNode confNode;
        try (InputStream confStream = getClass().getClassLoader().getResourceAsStream(configFile)) {
            ObjectMapper mapper = new ObjectMapper();
            confNode = mapper.readValue(confStream, JsonNode.class);
        } catch (Exception e) {
            throw new IOException("Unable to build JSON object from file", e);
        }

        configure(stormConf, confNode);
    }

    @Override
    public void configure(@NotNull Map<String, Object> stormConf, @NotNull JsonNode filtersConf) {
        List<NavigationFilter> filterLists =
                Configurable.createConfiguredInstance(
                        this.getClass(), NavigationFilter.class, stormConf, filtersConf);

        filters = filterLists.toArray(new NavigationFilter[0]);
    }
}
