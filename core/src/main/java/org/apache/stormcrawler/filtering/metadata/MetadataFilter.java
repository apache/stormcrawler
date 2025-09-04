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
package org.apache.stormcrawler.filtering.metadata;

import com.fasterxml.jackson.databind.JsonNode;
import java.net.URL;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.function.Predicate;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.filtering.URLFilter;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Filter out URLs based on metadata in the source document. The following json configurations are
 * working perfectly.<br>
 * Example 1:
 *
 * <pre>
 *  {
 *    "class": "org.apache.stormcrawler.filtering.metadata.MetadataFilter",
 *    "name": "MetadataFilter",
 *    "params": {
 *      "key": "val"
 *    }
 *  }
 * </pre>
 *
 * Example 2:
 *
 * <pre>
 *  {
 *    "class": "org.apache.stormcrawler.filtering.metadata.MetadataFilter",
 *    "name": "MetadataFilter",
 *    "params": {
 *      "operation": "AND",
 *      "filters": {
 *        "key": "val",
 *        "key2": "val2"
 *      }
 *    }
 *  }
 * </pre>
 *
 * Example 3:
 *
 * <pre>
 *  {
 *    "class": "org.apache.stormcrawler.filtering.metadata.MetadataFilter",
 *    "name": "MetadataFilter",
 *    "params": {
 *      "operation": "AND",
 *      "filters": {
 *        "key": "val",
 *        "unique_key_for_complex_filtering_1": {
 *          "operation": "OR",
 *          "filters": {
 *            "key2": "val2",
 *            "key3": "val3"
 *          }
 *        }
 *      }
 *    }
 *  }
 * </pre>
 */
public class MetadataFilter extends URLFilter {

    private static final Logger LOG = LoggerFactory.getLogger(MetadataFilter.class);
    public static final String COMPLEX_FILTERING_KEY_PREFIX = "unique_key_for_complex_filtering_";
    public static final String OPERATION_KEY = "operation";
    public static final String FILTERS_KEY = "filters";

    private final ComplexFilter filters = new ComplexFilter();

    @Override
    public void configure(@NotNull Map<String, Object> stormConf, @NotNull JsonNode paramNode) {
        configure(this.filters, paramNode);
    }

    private void configure(@NotNull ComplexFilter filters, @NotNull JsonNode paramNode) {
        if (!paramNode.has(OPERATION_KEY) && !paramNode.has(FILTERS_KEY)) {
            java.util.Iterator<Entry<String, JsonNode>> iter = paramNode.fields();
            while (iter.hasNext()) {
                Entry<String, JsonNode> entry = iter.next();
                String key = entry.getKey();
                String value = entry.getValue().asText();
                filters.addFilter(key, value);
            }
        }

        if (paramNode.has(OPERATION_KEY)) {
            if (paramNode.get(OPERATION_KEY).asText().equalsIgnoreCase("AND")) {
                filters.setOperation(FilterOperation.AND);
            } else if (paramNode.get(OPERATION_KEY).asText().equalsIgnoreCase("OR")) {
                filters.setOperation(FilterOperation.OR);
            }
        }
        if (paramNode.has(FILTERS_KEY)) {
            paramNode
                    .get(FILTERS_KEY)
                    .fields()
                    .forEachRemaining(
                            entry -> {
                                String key = entry.getKey();
                                if (!key.startsWith(COMPLEX_FILTERING_KEY_PREFIX)) {
                                    String value = entry.getValue().asText();
                                    filters.addFilter(key, value);
                                }
                            });
            Iterator<String> fieldNames = paramNode.get(FILTERS_KEY).fieldNames();
            while (fieldNames.hasNext()) {
                String fieldName = fieldNames.next();
                if (fieldName.startsWith(COMPLEX_FILTERING_KEY_PREFIX)) {
                    ComplexFilter subFilters = new ComplexFilter();
                    filters.addFilter(subFilters);
                    configure(subFilters, paramNode.get(FILTERS_KEY).get(fieldName));
                }
            }
        }
    }

    public void addFilter(String key, String value) {
        filters.addFilter(key, value);
    }

    public void addFilter(ComplexFilter filter) {
        filters.addFilter(filter);
    }

    public void setOperation(FilterOperation operation) {
        filters.setOperation(operation);
    }

    @Override
    public @Nullable String filter(
            @Nullable URL pageUrl, @Nullable Metadata sourceMetadata, @NotNull String urlToFilter) {
        if (sourceMetadata == null) {
            return urlToFilter;
        }
        if (sourceMetadata.asMap().isEmpty()) {
            return urlToFilter;
        }
        if (filters.filters.isEmpty()) {
            return urlToFilter;
        }

        boolean shouldFilter =
                recursiveFilter(filters.operation, filters, sourceMetadata, urlToFilter);
        if (shouldFilter) {
            return null;
        }

        return urlToFilter;
    }

    private static boolean recursiveFilter(
            FilterOperation operation,
            ComplexFilter complexFilter,
            Metadata sourceMetadata,
            String urlToFilter) {
        if (operation == FilterOperation.OR) {
            return complexFilter.filters.entrySet().stream()
                    .anyMatch(getPredicate(sourceMetadata, urlToFilter));
        } else if (operation == FilterOperation.AND) {
            return complexFilter.filters.entrySet().stream()
                    .allMatch(getPredicate(sourceMetadata, urlToFilter));
        }
        return false;
    }

    private static @NotNull Predicate<Entry<String, Object>> getPredicate(
            Metadata sourceMetadata, String urlToFilter) {
        return entrySet -> {
            if (entrySet.getValue() instanceof ComplexFilter) {
                return recursiveFilter(
                        ((ComplexFilter) entrySet.getValue()).operation,
                        (ComplexFilter) entrySet.getValue(),
                        sourceMetadata,
                        urlToFilter);
            }
            String[] vals = sourceMetadata.getValues(entrySet.getKey());
            if (vals == null) {
                return false;
            }

            for (String v : vals) {
                if (entrySet.getValue() instanceof String) {
                    if (v.equalsIgnoreCase((String) entrySet.getValue())) {
                        LOG.debug(
                                "Filtering {} matching metadata {}:{}",
                                urlToFilter,
                                entrySet.getKey(),
                                entrySet.getValue());
                        return true;
                    }
                }
            }

            return false;
        };
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof MetadataFilter that)) return false;
        return Objects.equals(filters, that.filters);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(filters);
    }

    @Override
    public String toString() {
        return "MetadataFilter{" + "filters=" + filters + '}';
    }

    public static class ComplexFilter {
        private final Map<String, Object> filters = new HashMap<>();
        private FilterOperation operation = FilterOperation.OR;

        public void setOperation(FilterOperation operation) {
            this.operation = operation;
        }

        public void addFilter(String key, String value) {
            filters.put(key, value);
        }

        public void addFilter(ComplexFilter filter) {
            String key = COMPLEX_FILTERING_KEY_PREFIX;
            int counter = 1;
            while (filters.containsKey(key + counter)) {
                counter++;
            }
            filters.put(key + counter, filter);
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof ComplexFilter that)) return false;
            return Objects.equals(filters, that.filters) && operation == that.operation;
        }

        @Override
        public int hashCode() {
            return Objects.hash(filters, operation);
        }

        @Override
        public String toString() {
            return "ComplexFilter{" + "filters=" + filters + ", operation=" + operation + '}';
        }
    }

    public enum FilterOperation {
        OR,
        AND
    }
}
