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

package org.apache.stormcrawler.jsoup;

import com.fasterxml.jackson.core.JsonPointer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.parse.JSoupFilter;
import org.apache.stormcrawler.parse.ParseData;
import org.apache.stormcrawler.parse.ParseResult;
import org.apache.stormcrawler.util.AbstractConfigurable;
import org.jetbrains.annotations.NotNull;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Extracts data from JSON-LD representation (https://json-ld.org/). Illustrates how to use the
 * JSoupFilters
 */
public class LDJsonParseFilter extends AbstractConfigurable implements JSoupFilter {

    public static final Logger LOG = LoggerFactory.getLogger(LDJsonParseFilter.class);

    private static final ObjectMapper mapper = new ObjectMapper();

    private final List<LabelledJsonPointer> expressions = new LinkedList<>();

    public static JsonNode filterJson(Document doc) throws Exception {

        Element el = doc.selectFirst("script[type=application/ld+json]");
        if (el == null) {
            return null;
        }
        return mapper.readValue(el.data(), JsonNode.class);
    }

    @Override
    public void configure(@NotNull Map<String, Object> stormConf, @NotNull JsonNode filterParams) {
        Iterator<Entry<String, JsonNode>> iter = filterParams.fields();
        while (iter.hasNext()) {
            Entry<String, JsonNode> entry = iter.next();
            String key = entry.getKey();
            JsonNode node = entry.getValue();
            LabelledJsonPointer labelP =
                    new LabelledJsonPointer(key, JsonPointer.valueOf(node.asText()));
            expressions.add(labelP);
        }
    }

    static class LabelledJsonPointer {

        String label;
        JsonPointer pointer;

        public LabelledJsonPointer(String label, JsonPointer pointer) {
            this.label = label;
            this.pointer = pointer;
        }

        @Override
        public String toString() {
            return label + " => " + pointer.toString();
        }
    }

    @Override
    public void filter(
            @NotNull String url,
            byte[] content,
            @NotNull Document doc,
            @NotNull ParseResult parse) {
        try {
            JsonNode json = filterJson(doc);
            if (json == null) {
                return;
            }

            ParseData parseData = parse.get(url);
            Metadata metadata = parseData.getMetadata();

            // extract patterns and store as metadata
            for (LabelledJsonPointer expression : expressions) {
                JsonNode match = json.at(expression.pointer);
                if (match.isMissingNode()) {
                    continue;
                }
                metadata.addValue(expression.label, match.asText());
            }

        } catch (Exception e) {
            LOG.error("Exception caught when extracting json", e);
        }
    }
}
