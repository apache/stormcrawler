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

package org.apache.stormcrawler.persistence;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Date;
import org.apache.stormcrawler.persistence.StatusUpdaterBenchmark.Item;
import org.junit.jupiter.api.Test;

class StatusUpdaterBenchmarkTest {

    @Test
    void plainUrl() {
        Item item = StatusUpdaterBenchmark.parse("  https://example.com/\r");
        assertEquals("https://example.com/", item.url());
        assertEquals(Status.DISCOVERED, item.status());
        assertTrue(item.nextFetch().isPresent());
        assertTrue(item.metadata().keySet().isEmpty());
    }

    @Test
    void discovered() {
        Item item =
                StatusUpdaterBenchmark.parse(
                        "{\"discovered\": {\"info\": {\"url\": \"https://example.com/é\","
                                + " \"crawlID\": \"x\", \"metadata\": {\"depth\": {\"values\":"
                                + " [\"0\"]}, \"tags\": {\"values\": [\"a\", \"b\"]}}}}}");
        assertEquals("https://example.com/é", item.url());
        assertEquals(Status.DISCOVERED, item.status());
        assertTrue(item.nextFetch().isPresent());
        assertArrayEquals(new String[] {"0"}, item.metadata().getValues("depth"));
        assertArrayEquals(new String[] {"a", "b"}, item.metadata().getValues("tags"));
    }

    @Test
    void known() {
        Item item =
                StatusUpdaterBenchmark.parse(
                        "{\"known\": {\"refetchable_from_date\": \"1700000000\", \"info\":"
                                + " {\"url\": \"https://example.com/\"}}}");
        assertEquals(Status.FETCHED, item.status());
        assertEquals(new Date(1700000000L * 1000), item.nextFetch().get());

        item =
                StatusUpdaterBenchmark.parse(
                        "{\"known\": {\"refetchableFromDate\": 1700000000,"
                                + " \"refetchable_from_date\": 5, \"info\":"
                                + " {\"url\": \"https://example.com/\"}}}");
        assertEquals(new Date(1700000000L * 1000), item.nextFetch().get());

        item =
                StatusUpdaterBenchmark.parse(
                        "{\"known\": {\"info\": {\"url\": \"https://example.com/\"}}}");
        assertTrue(item.nextFetch().isEmpty());
    }

    @Test
    void invalid() {
        assertNull(StatusUpdaterBenchmark.parse("{\"discovered\": {\"info\": {}}}"));
        assertNull(StatusUpdaterBenchmark.parse("{\"discovered\": {\"info\": {\"url\": 1}}}"));
        assertNull(StatusUpdaterBenchmark.parse("{\"other\": {}}"));
        assertNull(StatusUpdaterBenchmark.parse("{\"discovered\": {\"info\": {\"url\": \"a"));
    }
}
