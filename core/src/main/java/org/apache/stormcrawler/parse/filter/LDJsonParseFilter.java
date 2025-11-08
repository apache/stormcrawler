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

import com.fasterxml.jackson.core.JsonPointer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathFactory;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.parse.ParseData;
import org.apache.stormcrawler.parse.ParseFilter;
import org.apache.stormcrawler.parse.ParseResult;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.DocumentFragment;
import org.w3c.dom.Node;

/** Extracts data from JSON-LD representation (https://json-ld.org/). */
public class LDJsonParseFilter extends ParseFilter {

    public static final Logger LOG = LoggerFactory.getLogger(LDJsonParseFilter.class);

    private static XPathFactory factory = XPathFactory.newInstance();
    private static XPath xpath = factory.newXPath();

    private static ObjectMapper mapper = new ObjectMapper();

    private List<LabelledJsonPointer> expressions = new LinkedList<>();

    @Override
    public void filter(String url, byte[] content, DocumentFragment doc, ParseResult parse) {
        if (doc == null) {
            return;
        }
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

    public static JsonNode filterJson(DocumentFragment doc) throws Exception {
        XPathExpression expressionJobPosting =
                xpath.compile("//SCRIPT[@type=\"application/ld+json\"]");
        Node scriptNode = (Node) expressionJobPosting.evaluate(doc, XPathConstants.NODE);
        if (scriptNode == null) {
            return null;
        }
        return mapper.readValue(scriptNode.getTextContent(), JsonNode.class);
    }

    public void configure(@NotNull Map<String, Object> stormConf, @NotNull JsonNode filterParams) {
        java.util.Iterator<Entry<String, JsonNode>> iter = filterParams.fields();
        while (iter.hasNext()) {
            Entry<String, JsonNode> entry = iter.next();
            String key = entry.getKey();
            JsonNode node = entry.getValue();
            LabelledJsonPointer labelP =
                    new LabelledJsonPointer(key, JsonPointer.valueOf(node.asText()));
            expressions.add(labelP);
        }
    }

    class LabelledJsonPointer {

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
    public boolean needsDOM() {
        return true;
    }
}
