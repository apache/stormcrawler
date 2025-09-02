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
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Predicate;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.filtering.URLFilter;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Filter out URLs based on metadata in the source document */
public class MetadataFilter extends URLFilter {

    private static final Logger LOG = LoggerFactory.getLogger(MetadataFilter.class);

    private final ComplexFilter filters = new ComplexFilter();

    @Override
    public void configure(@NotNull Map<String, Object> stormConf, @NotNull JsonNode paramNode) {
        java.util.Iterator<Entry<String, JsonNode>> iter = paramNode.fields();
        while (iter.hasNext()) {
            Entry<String, JsonNode> entry = iter.next();
            String key = entry.getKey();
            String value = entry.getValue().asText();
            filters.addFilter(key, value);
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
            String key = "unique_key_for_complex_filtering_";
            int counter = 1;
            while (filters.containsKey(key + counter)) {
                counter++;
            }
            filters.put(key + counter, filter);
        }
    }

    public enum FilterOperation {
        OR,
        AND
    }
}
