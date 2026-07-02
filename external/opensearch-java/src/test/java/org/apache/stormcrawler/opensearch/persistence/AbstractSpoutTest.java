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

package org.apache.stormcrawler.opensearch.persistence;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.json.stream.JsonParser;
import java.io.StringReader;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.opensearch.client.json.JsonData;
import org.opensearch.client.json.JsonpMapper;
import org.opensearch.client.json.jackson.JacksonJsonpMapper;

/**
 * Unit tests for {@link AbstractSpout#sourceAsMap(JsonData)}.
 *
 * <p>Regression coverage for the {@code ClassCastException: JsonStringImpl cannot be cast to
 * String} that was thrown by {@link AggregationSpout} and {@link HybridSpout} when the previous
 * implementation called {@code hit.source().to(Object.class)}: in the OpenSearch Java client,
 * {@code Object.class} is bound to a built-in {@code jsonValueDeserializer} that returns the raw
 * Jakarta JSON-P {@code JsonValue} tree (Parsson types) instead of going through the configured
 * Jackson mapper. The fix routes deserialisation through {@code Map.class}, which correctly falls
 * through to {@link JacksonJsonpMapper} and yields native Java types.
 */
class AbstractSpoutTest {

    private static JsonData parse(JsonpMapper mapper, String json) {
        try (JsonParser parser = mapper.jsonProvider().createParser(new StringReader(json))) {
            return JsonData._DESERIALIZER.deserialize(parser, mapper);
        }
    }

    @Test
    @DisplayName("sourceAsMap returns Java Strings (not Parsson JsonString) for string fields")
    void sourceAsMap_returnsJavaStrings() {
        JsonpMapper mapper = new JacksonJsonpMapper();
        JsonData data =
                parse(
                        mapper,
                        "{\"url\":\"http://example.com/page\","
                                + "\"nextFetchDate\":\"2026-01-01T00:00:00Z\"}");

        Map<String, Object> map = AbstractSpout.sourceAsMap(data);

        assertNotNull(map);
        Object url = map.get("url");
        assertInstanceOf(
                String.class,
                url,
                "url should be deserialised as java.lang.String, got: "
                        + (url == null ? "null" : url.getClass().getName()));
        assertEquals("http://example.com/page", url);

        Object nextFetchDate = map.get("nextFetchDate");
        assertInstanceOf(String.class, nextFetchDate);
        assertEquals("2026-01-01T00:00:00Z", nextFetchDate);
    }

    @Test
    @DisplayName("sourceAsMap preserves nested metadata maps with multi-valued lists")
    void sourceAsMap_preservesNestedMetadata() {
        JsonpMapper mapper = new JacksonJsonpMapper();
        JsonData data =
                parse(
                        mapper,
                        "{\"url\":\"http://example.com/\","
                                + "\"metadata\":{\"key\":[\"v1\",\"v2\"],\"single\":\"only\"}}");

        Map<String, Object> map = AbstractSpout.sourceAsMap(data);

        @SuppressWarnings("unchecked")
        Map<String, Object> metadata = (Map<String, Object>) map.get("metadata");
        assertNotNull(metadata);

        Object multi = metadata.get("key");
        assertInstanceOf(List.class, multi);
        @SuppressWarnings("unchecked")
        List<Object> values = (List<Object>) multi;
        assertEquals(2, values.size());
        assertInstanceOf(String.class, values.get(0));
        assertEquals("v1", values.get(0));
        assertEquals("v2", values.get(1));

        Object single = metadata.get("single");
        assertInstanceOf(String.class, single);
        assertEquals("only", single);
    }

    @Test
    @DisplayName("sourceAsMap returns an empty map for null source")
    void sourceAsMap_nullSource_returnsEmpty() {
        Map<String, Object> map = AbstractSpout.sourceAsMap(null);
        assertNotNull(map);
        assertTrue(map.isEmpty());
    }

    @Test
    @DisplayName(
            "control: JsonData.to(Object.class) leaks Jakarta JsonValue types — proves the bug")
    void controlTest_toObjectClassLeaksJsonValue() {
        // This test pins the underlying root cause of the original ClassCastException:
        // calling JsonData#to(Object.class) bypasses JacksonJsonpMapper and returns a raw
        // Jakarta JSON-P JsonValue, NOT a java.lang.String. If a future opensearch-java
        // release ever changes this behaviour to align with Map.class, we can simplify the
        // production code — until then, AbstractSpout#sourceAsMap must keep using Map.class.
        JsonpMapper mapper = new JacksonJsonpMapper();
        JsonData data = parse(mapper, "{\"url\":\"http://example.com/\"}");

        Object asObject = data.to(Object.class);

        // It is a Jakarta JsonObject, not a Java Map<String,String>:
        assertInstanceOf(jakarta.json.JsonObject.class, asObject);
        jakarta.json.JsonObject jsonObject = (jakarta.json.JsonObject) asObject;
        // And the "url" entry is a JsonString, not a java.lang.String — exactly the cast that
        // blew up at runtime in AggregationSpout before the fix.
        assertInstanceOf(jakarta.json.JsonString.class, jsonObject.get("url"));
    }
}
