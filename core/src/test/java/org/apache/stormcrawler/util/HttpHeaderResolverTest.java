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

package org.apache.stormcrawler.util;

import org.apache.http.HttpHeaders;
import org.apache.stormcrawler.Metadata;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class HttpHeaderResolverTest {

    @Test
    void testExactHeaderTakesPrecedence() {
        Metadata metadata = new Metadata();
        metadata.setValue("Location", "https://example.com/exact");
        metadata.setValue("Location_", "https://example.com/alias");

        Assertions.assertEquals(
                "https://example.com/exact",
                HttpHeaderResolver.getFirstValue(metadata, HttpHeaders.LOCATION));
    }

    @Test
    void testSeparatorAliasesAreResolved() {
        Metadata metadata = new Metadata();
        metadata.setValue("ContentType", "text/html");

        Assertions.assertEquals(
                "text/html", HttpHeaderResolver.getFirstValue(metadata, HttpHeaders.CONTENT_TYPE));

        metadata = new Metadata();
        metadata.setValue("content_type", "application/json");

        Assertions.assertEquals(
                "application/json",
                HttpHeaderResolver.getFirstValue(metadata, HttpHeaders.CONTENT_TYPE));
    }

    @Test
    void testExtensionHeaderIsNotTreatedAsAlias() {
        Metadata metadata = new Metadata();
        metadata.setValue("X-Location", "cached");

        Assertions.assertNull(
                HttpHeaderResolver.getFirstValue(metadata, HttpHeaders.LOCATION));
        Assertions.assertEquals("cached", metadata.getFirstValue("X-Location"));
    }

    @Test
    void testMisspellingIsNotFuzzyMatched() {
        Metadata metadata = new Metadata();
        metadata.setValue("ConTnTtYpe", "text/html");

        Assertions.assertNull(
                HttpHeaderResolver.getFirstValue(metadata, HttpHeaders.CONTENT_TYPE));
        Assertions.assertEquals("text/html", metadata.getFirstValue("ConTnTtYpe"));
    }

    @Test
    void testAmbiguousAliasesAreIgnored() {
        Metadata metadata = new Metadata();
        metadata.setValue("ContentType", "text/html");
        metadata.setValue("Content_Type", "application/json");

        Assertions.assertNull(
                HttpHeaderResolver.getFirstValue(metadata, HttpHeaders.CONTENT_TYPE));
    }

    @Test
    void testPrefixedHeaderAliasIsResolved() {
        Metadata metadata = new Metadata();
        metadata.setValue("http.ContentType", "text/html");

        Assertions.assertEquals(
                "text/html",
                HttpHeaderResolver.getFirstValue(
                        metadata, HttpHeaders.CONTENT_TYPE, "http."));
    }
}
