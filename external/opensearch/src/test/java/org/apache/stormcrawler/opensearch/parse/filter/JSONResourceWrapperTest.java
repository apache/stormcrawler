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

package org.apache.stormcrawler.opensearch.parse.filter;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import org.apache.stormcrawler.JSONResource;
import org.apache.stormcrawler.parse.ParseFilter;
import org.apache.stormcrawler.parse.ParseResult;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;
import org.w3c.dom.DocumentFragment;

class JSONResourceWrapperTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    /** Minimal ParseFilter + JSONResource implementation. */
    public static class StubParseFilter extends ParseFilter implements JSONResource {

        @Override
        public void configure(@NotNull Map<String, Object> stormConf, @NotNull JsonNode params) {}

        @Override
        public void filter(String url, byte[] content, DocumentFragment doc, ParseResult parse) {}

        @Override
        public String getResourceFile() {
            return "stub.json";
        }

        @Override
        public void loadJSONResources(InputStream inputStream) {}
    }

    /** ParseFilter which requires a DOM. */
    public static class DOMStubParseFilter extends StubParseFilter {
        @Override
        public boolean needsDOM() {
            return true;
        }
    }

    private static boolean wrapperNeedsDOM(Class<?> delegateClass) {
        ObjectNode delegate = MAPPER.createObjectNode();
        delegate.put("class", delegateClass.getName());
        delegate.set("params", MAPPER.createObjectNode());
        ObjectNode params = MAPPER.createObjectNode();
        params.set("delegate", delegate);

        JSONResourceWrapper wrapper = new JSONResourceWrapper();
        wrapper.configure(new HashMap<>(), params);
        try {
            return wrapper.needsDOM();
        } finally {
            wrapper.cleanup();
        }
    }

    @Test
    void needsDOMIsDelegated() {
        assertTrue(wrapperNeedsDOM(DOMStubParseFilter.class));
        assertFalse(wrapperNeedsDOM(StubParseFilter.class));
    }
}
