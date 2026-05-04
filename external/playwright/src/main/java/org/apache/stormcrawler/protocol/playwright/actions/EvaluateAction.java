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

package org.apache.stormcrawler.protocol.playwright.actions;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.playwright.Page;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.protocol.playwright.PageAction;
import org.jetbrains.annotations.NotNull;
import org.slf4j.LoggerFactory;

/**
 * Evaluates a list of JavaScript expressions on the page and stores the JSON-serialised result of
 * each in the response metadata.
 *
 * <h3>Parameters</h3>
 *
 * <ul>
 *   <li>{@code expressions} (required, array of strings): JS expressions to evaluate
 *   <li>{@code keyPrefix} (optional, string): if set, the metadata key for each expression is
 *       {@code keyPrefix + index}; otherwise the expression itself is used as the key (matches the
 *       legacy {@code playwright.evaluations} behaviour)
 * </ul>
 */
public class EvaluateAction extends PageAction {

    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(EvaluateAction.class);

    private final ObjectMapper mapper = new ObjectMapper();

    private List<String> expressions = List.of();
    private String keyPrefix;

    @Override
    public void configure(
            @NotNull final Map<String, Object> stormConf, @NotNull final JsonNode params) {
        if (params == null || params.isMissingNode() || params.isNull()) {
            return;
        }
        final JsonNode exprs = params.get("expressions");
        if (exprs != null && exprs.isArray()) {
            final List<String> list = new ArrayList<>(exprs.size());
            exprs.forEach(n -> list.add(n.asText()));
            this.expressions = list;
        }
        if (params.has("keyPrefix")) {
            this.keyPrefix = params.get("keyPrefix").asText();
        }
        if (expressions.isEmpty()) {
            throw new IllegalArgumentException("EvaluateAction requires non-empty 'expressions'");
        }
    }

    @Override
    public void apply(
            @NotNull final Page page,
            @NotNull final String url,
            @NotNull final Metadata sourceMetadata,
            @NotNull final Metadata responseMetadata) {
        for (int i = 0; i < expressions.size(); i++) {
            final String expression = expressions.get(i);
            try {
                final Object result = page.evaluate(expression);
                if (result == null) {
                    continue;
                }
                final String json =
                        mapper.writerWithDefaultPrettyPrinter().writeValueAsString(result);
                final String key = keyPrefix == null ? expression : keyPrefix + i;
                responseMetadata.setValue(key, json);
            } catch (final JsonProcessingException e) {
                LOG.debug(
                        "Could not serialise result of {} on {}: {}",
                        expression,
                        url,
                        e.getMessage());
            } catch (final Exception e) {
                LOG.debug("Evaluate {} failed on {}: {}", expression, url, e.getMessage());
            }
        }
    }
}
