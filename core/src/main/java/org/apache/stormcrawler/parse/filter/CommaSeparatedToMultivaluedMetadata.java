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

package org.apache.stormcrawler.parse.filter;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.parse.ParseFilter;
import org.apache.stormcrawler.parse.ParseResult;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.DocumentFragment;

/**
 * Rewrites single metadata containing comma separated values into multiple values for the same key,
 * useful for instance for keyword tags. The number of values produced from a single entry is
 * bounded by the optional {@code maxTokens} parameter, 128 by default.
 */
public class CommaSeparatedToMultivaluedMetadata extends ParseFilter {

    private static final Logger LOG =
            LoggerFactory.getLogger(CommaSeparatedToMultivaluedMetadata.class);

    /**
     * Default upper bound on the number of values a single comma separated entry can produce, so
     * that a page cannot generate an unbounded amount of metadata.
     */
    private static final int MAX_TOKENS_DEFAULT = 128;

    private final Set<String> keys = new HashSet<>();

    private int maxTokens = MAX_TOKENS_DEFAULT;

    @Override
    public void configure(@NotNull Map<String, Object> stormConf, @NotNull JsonNode filterParams) {
        JsonNode node = filterParams.get("keys");
        if (node == null) {
            return;
        }
        if (node.isArray()) {
            for (JsonNode jsonNode : node) {
                keys.add(jsonNode.asText());
            }
        } else {
            keys.add(node.asText());
        }

        node = filterParams.get("maxTokens");
        if (node != null && node.isInt() && node.asInt() > 0) {
            maxTokens = node.asInt();
        }
    }

    @Override
    public void filter(String url, byte[] content, DocumentFragment doc, ParseResult parse) {
        Metadata m = parse.getOrCreate(url).getMetadata();
        for (String key : keys) {
            String val = m.getFirstValue(key);
            if (val == null) {
                continue;
            }
            m.remove(key);
            String[] tokens = val.split(" *, *");
            // drop blank tokens, as Metadata.addValue used to, then store everything in one call:
            // appending one token at a time copies the whole array for every token
            List<String> values = new ArrayList<>(tokens.length);
            for (String t : tokens) {
                if (StringUtils.isNotBlank(t)) {
                    values.add(t);
                }
            }
            if (values.size() > maxTokens) {
                LOG.warn(
                        "Key [{}] has [{}] comma separated values, keeping only the first [{}]",
                        key,
                        values.size(),
                        maxTokens);
                values.subList(maxTokens, values.size()).clear();
            }
            m.setValues(key, values.toArray(new String[0]));
        }
    }
}
