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
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.parse.ParseFilter;
import org.apache.stormcrawler.parse.ParseResult;
import org.jetbrains.annotations.NotNull;
import org.w3c.dom.DocumentFragment;

/**
 * Rewrites single metadata containing comma separated values into multiple values for the same key,
 * useful for instance for keyword tags.
 */
public class CommaSeparatedToMultivaluedMetadata extends ParseFilter {

    private final Set<String> keys = new HashSet<>();

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
    }

    @Override
    public void filter(String url, byte[] content, DocumentFragment doc, ParseResult parse) {
        Metadata m = parse.get(url).getMetadata();
        for (String key : keys) {
            String val = m.getFirstValue(key);
            if (val == null) {
                continue;
            }
            m.remove(key);
            String[] tokens = val.split(" *, *");
            for (String t : tokens) {
                m.addValue(key, t);
            }
        }
    }
}
