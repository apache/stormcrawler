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
import com.fasterxml.jackson.databind.node.NullNode;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.jetbrains.annotations.NotNull;
import org.slf4j.LoggerFactory;

final class ConfigurableHelper {
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(ConfigurableHelper.class);

    private ConfigurableHelper() {}

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
        // initialises the filters
        List<T> filterLists = new ArrayList<>();

        // get the filters part
        filtersConf = filtersConf.get(configName);
        if (filtersConf == null) {
            LOG.info("No field {} in JSON config. Skipping...", configName);
            return filterLists;
        }

        // conf node contains a list of objects
        Iterator<JsonNode> filterIter = filtersConf.elements();
        while (filterIter.hasNext()) {
            JsonNode afilterConf = filterIter.next();
            String filterName = "<unnamed>";
            JsonNode nameNode = afilterConf.get("name");
            if (nameNode != null) {
                filterName = nameNode.textValue();
            }
            JsonNode classNode = afilterConf.get("class");
            if (classNode == null) {
                LOG.error("Filter {} doesn't specified a 'class' attribute", filterName);
                continue;
            }
            String className = classNode.textValue().trim();
            filterName += '[' + className + ']';
            // check that it is available and implements the interface
            // ParseFilter
            try {
                T filterInstance =
                        InitialisationUtil.initializeFromQualifiedName(className, filterClass);

                JsonNode paramNode = afilterConf.get("params");
                if (paramNode != null) {
                    filterInstance.configure(stormConf, paramNode, filterName);
                } else {
                    // Pass in a nullNode if missing
                    filterInstance.configure(stormConf, NullNode.getInstance(), filterName);
                }

                filterLists.add(filterInstance);
                LOG.info("Setup {}", filterName);
            } catch (Exception e) {
                LOG.error("Can't setup {}: {}", filterName, e);
                throw new RuntimeException("Can't setup " + filterName, e);
            }
        }

        return filterLists;
    }
}
