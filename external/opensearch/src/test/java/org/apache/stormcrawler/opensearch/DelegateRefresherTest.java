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

package org.apache.stormcrawler.opensearch;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.stormcrawler.JSONResource;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.filtering.URLFilter;
import org.apache.stormcrawler.parse.ParseFilter;
import org.apache.stormcrawler.parse.ParseResult;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Test;
import org.w3c.dom.DocumentFragment;

class DelegateRefresherTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    /** Minimal URLFilter + JSONResource implementation for testing. */
    public static class StubURLFilter extends URLFilter implements JSONResource {

        public final AtomicBoolean configured = new AtomicBoolean(false);

        @Override
        public void configure(@NotNull Map<String, Object> stormConf, @NotNull JsonNode params) {
            configured.set(true);
        }

        @Override
        public @Nullable String filter(
                @Nullable URL sourceUrl,
                @Nullable Metadata sourceMetadata,
                @NotNull String urlToFilter) {
            return urlToFilter;
        }

        @Override
        public String getResourceFile() {
            return "stub.json";
        }

        @Override
        public void loadJSONResources(InputStream inputStream) throws IOException {}
    }

    /** Minimal ParseFilter + JSONResource implementation for testing. */
    public static class StubParseFilter extends ParseFilter implements JSONResource {

        public final AtomicBoolean configured = new AtomicBoolean(false);

        @Override
        public void configure(@NotNull Map<String, Object> stormConf, @NotNull JsonNode params) {
            configured.set(true);
        }

        @Override
        public void filter(String URL, byte[] content, DocumentFragment doc, ParseResult parse) {}

        @Override
        public String getResourceFile() {
            return "stub.json";
        }

        @Override
        public void loadJSONResources(InputStream inputStream) throws IOException {}
    }

    /** A URLFilter that does NOT implement JSONResource. */
    public static class NonJsonResourceURLFilter extends URLFilter {

        @Override
        public @Nullable String filter(
                @Nullable URL sourceUrl,
                @Nullable Metadata sourceMetadata,
                @NotNull String urlToFilter) {
            return urlToFilter;
        }
    }

    /** Not a URLFilter at all. */
    public static class NotAFilter {}

    private JsonNode buildParams(String delegateClass) {
        return buildParams(delegateClass, 600);
    }

    private JsonNode buildParams(String delegateClass, int refreshRate) {
        ObjectNode delegate = MAPPER.createObjectNode();
        delegate.put("class", delegateClass);
        delegate.set("params", MAPPER.createObjectNode());

        ObjectNode params = MAPPER.createObjectNode();
        params.set("delegate", delegate);
        params.put("refresh", refreshRate);
        return params;
    }

    @Test
    void loadsURLFilterDelegate() {
        JsonNode params = buildParams(StubURLFilter.class.getName());
        Map<String, Object> conf = new HashMap<>();

        DelegateRefresher<URLFilter> refresher =
                new DelegateRefresher<>(
                        URLFilter.class, conf, params, (d, c, p) -> d.configure(c, p));

        try {
            assertNotNull(refresher.getDelegate());
            assertInstanceOf(StubURLFilter.class, refresher.getDelegate());
            assertTrue(((StubURLFilter) refresher.getDelegate()).configured.get());
        } finally {
            refresher.cleanup();
        }
    }

    @Test
    void loadsParseFilterDelegate() {
        JsonNode params = buildParams(StubParseFilter.class.getName());
        Map<String, Object> conf = new HashMap<>();

        DelegateRefresher<ParseFilter> refresher =
                new DelegateRefresher<>(
                        ParseFilter.class, conf, params, (d, c, p) -> d.configure(c, p));

        try {
            assertNotNull(refresher.getDelegate());
            assertInstanceOf(StubParseFilter.class, refresher.getDelegate());
            assertTrue(((StubParseFilter) refresher.getDelegate()).configured.get());
        } finally {
            refresher.cleanup();
        }
    }

    @Test
    void delegateFilterActuallyWorks() {
        JsonNode params = buildParams(StubURLFilter.class.getName());
        Map<String, Object> conf = new HashMap<>();

        DelegateRefresher<URLFilter> refresher =
                new DelegateRefresher<>(
                        URLFilter.class, conf, params, (d, c, p) -> d.configure(c, p));

        try {
            String result = refresher.getDelegate().filter(null, null, "http://example.com");
            assertEquals("http://example.com", result);
        } finally {
            refresher.cleanup();
        }
    }

    @Test
    void throwsWhenDelegateNodeMissing() {
        ObjectNode params = MAPPER.createObjectNode();
        // no "delegate" key
        Map<String, Object> conf = new HashMap<>();

        assertThrows(
                RuntimeException.class,
                () ->
                        new DelegateRefresher<>(
                                URLFilter.class, conf, params, (d, c, p) -> d.configure(c, p)));
    }

    @Test
    void throwsWhenClassMissing() {
        ObjectNode delegate = MAPPER.createObjectNode();
        // no "class" key
        ObjectNode params = MAPPER.createObjectNode();
        params.set("delegate", delegate);
        Map<String, Object> conf = new HashMap<>();

        assertThrows(
                RuntimeException.class,
                () ->
                        new DelegateRefresher<>(
                                URLFilter.class, conf, params, (d, c, p) -> d.configure(c, p)));
    }

    @Test
    void throwsWhenClassDoesNotExtendBaseType() {
        JsonNode params = buildParams(NotAFilter.class.getName());
        Map<String, Object> conf = new HashMap<>();

        RuntimeException ex =
                assertThrows(
                        RuntimeException.class,
                        () ->
                                new DelegateRefresher<>(
                                        URLFilter.class,
                                        conf,
                                        params,
                                        (d, c, p) -> d.configure(c, p)));
        assertTrue(ex.getMessage().contains("does not extend"));
    }

    @Test
    void throwsWhenClassDoesNotImplementJSONResource() {
        JsonNode params = buildParams(NonJsonResourceURLFilter.class.getName());
        Map<String, Object> conf = new HashMap<>();

        RuntimeException ex =
                assertThrows(
                        RuntimeException.class,
                        () ->
                                new DelegateRefresher<>(
                                        URLFilter.class,
                                        conf,
                                        params,
                                        (d, c, p) -> d.configure(c, p)));
        assertTrue(ex.getMessage().contains("does not implement JSONResource"));
    }

    @Test
    void cleanupIsIdempotent() {
        JsonNode params = buildParams(StubURLFilter.class.getName());
        Map<String, Object> conf = new HashMap<>();

        DelegateRefresher<URLFilter> refresher =
                new DelegateRefresher<>(
                        URLFilter.class, conf, params, (d, c, p) -> d.configure(c, p));

        // calling cleanup twice should not throw
        refresher.cleanup();
        refresher.cleanup();
    }
}
