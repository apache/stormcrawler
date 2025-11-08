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

package org.apache.stormcrawler.util;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.List;
import java.util.Map;
import org.jetbrains.annotations.NotNull;

/**
 * An interface marking the implementing class as initializeable and configurable via {@link
 * Configurable#createConfiguredInstance(String, Class, Map, JsonNode)} The implementing class
 * <b>HAS</b> to implement an empty constructor.
 */
public interface Configurable {

    public String getName();

    /**
     * Called when this filter is being initialized.
     *
     * @param stormConf The Storm configuration used for the configurable
     * @param filterParams the filter specific configuration. Never null
     */
    default void configure(
            @NotNull Map<String, Object> stormConf, @NotNull JsonNode filterParams) {}

    /**
     * Called when this filter is being initialized.
     *
     * @param stormConf The Storm configuration used for the configurable
     * @param filterParams the filter specific configuration. Never null
     * @param filterName The filter name.
     */
    default void configure(
            @NotNull Map<String, Object> stormConf,
            @NotNull JsonNode filterParams,
            @NotNull String filterName) {}

    /**
     * @deprecated Replace with {@link Configurable#createConfiguredInstance(Class, Class, Map,
     *     JsonNode)} or {@link Configurable#createConfiguredInstance(String, Class, Map, JsonNode)}
     */
    @Deprecated
    @NotNull
    static <T extends Configurable> List<@NotNull T> configure(
            @NotNull Map<String, Object> stormConf,
            @NotNull JsonNode filtersConf,
            @NotNull Class<T> filterClass,
            @NotNull String callingClass) {
        return ConfigurableHelper.createConfiguredInstance(
                callingClass, filterClass, stormConf, filtersConf);
    }

    /**
     * Calls {@link Configurable#createConfiguredInstance(String, Class, Map, JsonNode)} with {@code
     * caller.getName()} for {@code configName}.
     *
     * @see Configurable#createConfiguredInstance(String, Class, Map, JsonNode) for more
     *     information.
     */
    @NotNull
    static <T extends Configurable> List<@NotNull T> createConfiguredInstance(
            @NotNull Class<?> caller,
            @NotNull Class<T> filterClass,
            @NotNull Map<String, Object> stormConf,
            @NotNull JsonNode filtersConf) {
        return createConfiguredInstance(caller.getName(), filterClass, stormConf, filtersConf);
    }

    /**
     * Used by classes like URLFilters and ParseFilters to load the configuration of utilized
     * filters from the provided JSON config.
     *
     * <p>The functions searches for a childNode in {@code filtersConf} with the given {@code
     * configName}. If the childNode is found it initializes all elements in the list provided by
     * the {@code filtersConf} and initialized them as {@code filterClass}.
     *
     * <p>The following snippet shows the JSON-Schema for a config file, if the config file does not
     * meet the schema, the function fails.
     *
     * <pre>{@code
     * {
     *   "$id": "https://stormcrawler.net/schemas/configurable/config",
     *   "$schema": "https://json-schema.org/draft/2020-12/schema",
     *   "type": "object",
     *   "properties": {
     *     <configName>: {
     *       "type": "array",
     *       "contains": {
     *         "type": "object",
     *         "properties": {
     *           "name": {
     *             "type": "string",
     *             "default": "<unnamed>"
     *           },
     *           "class": {
     *             "type": "string"
     *           },
     *           "properties": {
     *             "type": "array",
     *             "default": null
     *           }
     *         },
     *         "required": [
     *           "class"
     *         ]
     *       }
     *     }
     *   }
     * }
     * }</pre>
     */
    @NotNull
    static <T extends Configurable> List<@NotNull T> createConfiguredInstance(
            @NotNull String configName,
            @NotNull Class<T> filterClass,
            @NotNull Map<String, Object> stormConf,
            @NotNull JsonNode filtersConf) {
        return ConfigurableHelper.createConfiguredInstance(
                configName, filterClass, stormConf, filtersConf);
    }
}
