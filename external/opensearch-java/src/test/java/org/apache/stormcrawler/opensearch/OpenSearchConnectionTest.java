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

import org.junit.jupiter.api.Test;

class OpenSearchConnectionTest {

    @Test
    void parseSeconds() {
        assertEquals(5000, OpenSearchConnection.parseTimeValueToMillis("5s", 0));
    }

    @Test
    void parseMilliseconds() {
        assertEquals(500, OpenSearchConnection.parseTimeValueToMillis("500ms", 0));
    }

    @Test
    void parseMinutes() {
        assertEquals(120000, OpenSearchConnection.parseTimeValueToMillis("2m", 0));
    }

    @Test
    void parsePlainNumber() {
        assertEquals(42, OpenSearchConnection.parseTimeValueToMillis("42", 0));
    }

    @Test
    void nullReturnsDefault() {
        assertEquals(5000, OpenSearchConnection.parseTimeValueToMillis(null, 5000));
    }

    @Test
    void emptyReturnsDefault() {
        assertEquals(5000, OpenSearchConnection.parseTimeValueToMillis("", 5000));
    }

    @Test
    void blankReturnsDefault() {
        assertEquals(5000, OpenSearchConnection.parseTimeValueToMillis("   ", 5000));
    }

    @Test
    void invalidReturnsDefault() {
        assertEquals(3000, OpenSearchConnection.parseTimeValueToMillis("abc", 3000));
    }

    @Test
    void invalidWithSuffixReturnsDefault() {
        assertEquals(3000, OpenSearchConnection.parseTimeValueToMillis("abcs", 3000));
    }

    @Test
    void whitespaceIsTrimmed() {
        assertEquals(5000, OpenSearchConnection.parseTimeValueToMillis("  5s  ", 0));
    }
}
