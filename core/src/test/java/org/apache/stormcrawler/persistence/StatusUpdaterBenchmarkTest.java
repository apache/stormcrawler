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

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.stormcrawler.persistence.StatusUpdaterBenchmark.Block;
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
    void discoveredTakesPrecedence() {
        String discovered = "\"discovered\": {\"info\": {\"url\": \"https://example.com/d\"}}";
        String known =
                "\"known\": {\"refetchable_from_date\": 1, \"info\": {\"url\":"
                        + " \"https://example.com/k\"}}";
        for (String line :
                List.of(
                        "{" + known + ", " + discovered + "}",
                        "{" + discovered + ", " + known + "}")) {
            Item item = StatusUpdaterBenchmark.parse(line);
            assertEquals("https://example.com/d", item.url());
            assertEquals(Status.DISCOVERED, item.status());
        }
    }

    /** Splits the input into blocks of the given size, then the blocks into lines. */
    private static List<String> lines(String input, int blockSize) throws Exception {
        LinkedBlockingQueue<Block> blocks = new LinkedBlockingQueue<>();
        StatusUpdaterBenchmark.readBlocks(
                new ByteArrayInputStream(input.getBytes(StandardCharsets.UTF_8)),
                blockSize,
                blocks);
        List<String> lines = new ArrayList<>();
        for (Block block : blocks) {
            // every block but the last one ends with a full line
            assertTrue(block.length() > 0);
            StatusUpdaterBenchmark.forEachLine(
                    block,
                    (data, off, len) ->
                            lines.add(new String(data, off, len, StandardCharsets.UTF_8)));
        }
        return lines;
    }

    @Test
    void lineSplitAcrossBlocks() throws Exception {
        // the second line straddles the end of the first block
        assertEquals(List.of("aaaa", "bbbbbb", "cc"), lines("aaaa\nbbbbbb\ncc\n", 8));
        for (int size = 1; size <= 16; size++) {
            assertEquals(List.of("aaaa", "bbbbbb", "cc"), lines("aaaa\nbbbbbb\ncc\n", size));
        }
    }

    @Test
    void lineLongerThanBlock() throws Exception {
        String longLine = "x".repeat(100);
        assertEquals(List.of("a", longLine, "b"), lines("a\n" + longLine + "\nb\n", 8));
        assertEquals(List.of(longLine), lines(longLine, 8));
    }

    @Test
    void lastLineWithoutLineFeed() throws Exception {
        assertEquals(List.of("a", "b"), lines("a\nb", 4));
        assertEquals(List.of("a", "b"), lines("a\nb", 1024));
    }

    @Test
    void crlfLineEndings() throws Exception {
        // whatever the size of the blocks, the CR is removed with the rest of the whitespace
        for (int size = 1; size <= 16; size++) {
            assertEquals(List.of("aaa", "bbb", "c"), lines("aaa\r\nbbb\r\nc\r\n", size));
        }
    }

    @Test
    void blankLines() throws Exception {
        assertEquals(List.of("a", "b", "c"), lines("\n  \n a \n\t\n\r\nb\n \t \nc", 4));
        assertEquals(List.of(), lines("", 4));
        assertEquals(List.of(), lines(" \n\n", 4));
    }

    @Test
    void invalid() {
        assertNull(StatusUpdaterBenchmark.parse("{\"discovered\": {\"info\": {}}}"));
        assertNull(StatusUpdaterBenchmark.parse("{\"discovered\": {\"info\": {\"url\": 1}}}"));
        assertNull(StatusUpdaterBenchmark.parse("{\"other\": {}}"));
        assertNull(StatusUpdaterBenchmark.parse("{\"discovered\": {\"info\": {\"url\": \"a"));
    }
}
