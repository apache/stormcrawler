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

package org.apache.stormcrawler.filtering;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.util.URLUtil;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Behaviour of the filter chain when one of its filters throws. */
class URLFiltersExceptionTest {

    /** Stands in for any filter that throws at evaluation time. */
    public static class ThrowingURLFilter extends URLFilter {
        @Override
        public @Nullable String filter(
                @Nullable URL sourceUrl,
                @Nullable Metadata sourceMetadata,
                @NotNull String urlToFilter) {
            throw new NullPointerException("filter blew up");
        }
    }

    /** Stands in for an exclusion rule placed after it, such as the private-range regexes. */
    public static class RejectEverythingURLFilter extends URLFilter {
        @Override
        public @Nullable String filter(
                @Nullable URL sourceUrl,
                @Nullable Metadata sourceMetadata,
                @NotNull String urlToFilter) {
            return null;
        }
    }

    @Test
    void urlIsRejectedWhenAnEarlierFilterThrows() throws IOException, MalformedURLException {
        Map<String, Object> conf = new HashMap<>();
        URLFilters filters = new URLFilters(conf, "urlfilters-throwing.json");
        URL source = URLUtil.toURL("http://www.example.com/index.html");
        Assertions.assertNull(
                filters.filter(source, new Metadata(), "http://www.example.com/outlink.html"),
                "a filter that throws must reject the URL, not let it through");
        Assertions.assertEquals(1, filters.getExceptionsCount());
    }

    @Test
    void rejectionShortensTheChain() throws IOException, MalformedURLException {
        Map<String, Object> conf = new HashMap<>();
        URLFilters filters = new URLFilters(conf, "urlfilters-throwing.json");
        URL source = URLUtil.toURL("http://www.example.com/index.html");
        Assertions.assertNull(filters.filter(source, new Metadata(), "http://www.example.com/"));
        Assertions.assertEquals(1, filters.getExceptionsCount());
    }
}
