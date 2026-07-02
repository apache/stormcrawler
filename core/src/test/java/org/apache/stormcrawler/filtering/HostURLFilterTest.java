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

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.filtering.host.HostURLFilter;
import org.apache.stormcrawler.util.URLUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Utility class which encapsulates the filtering of URLs based on the hostname or domain of the
 * source URL.
 */
class HostURLFilterTest {

    private HostURLFilter createFilter(boolean ignoreOutsideHost, boolean ignoreOutsideDomain) {
        HostURLFilter filter = new HostURLFilter();
        ObjectNode filterParams = new ObjectNode(JsonNodeFactory.instance);
        filterParams.put("ignoreOutsideHost", Boolean.valueOf(ignoreOutsideHost));
        filterParams.put("ignoreOutsideDomain", Boolean.valueOf(ignoreOutsideDomain));
        Map<String, Object> conf = new HashMap<>();
        filter.configure(conf, filterParams);
        return filter;
    }

    @Test
    void testAllAllowed() throws MalformedURLException {
        HostURLFilter allAllowed = createFilter(false, false);
        URL sourceURL = URLUtil.toURL("http://www.sourcedomain.com/index.html");
        Metadata metadata = new Metadata();
        String filterResult =
                allAllowed.filter(sourceURL, metadata, "http://www.sourcedomain.com/index.html");
        Assertions.assertEquals("http://www.sourcedomain.com/index.html", filterResult);
        filterResult =
                allAllowed.filter(sourceURL, metadata, "http://www.anotherDomain.com/index.html");
        Assertions.assertEquals("http://www.anotherDomain.com/index.html", filterResult);
        filterResult =
                allAllowed.filter(sourceURL, metadata, "http://sub.sourcedomain.com/index.html");
        Assertions.assertEquals("http://sub.sourcedomain.com/index.html", filterResult);
    }

    @Test
    void testAllForbidden() throws MalformedURLException {
        HostURLFilter allAllowed = createFilter(true, true);
        URL sourceURL = URLUtil.toURL("http://www.sourcedomain.com/index.html");
        Metadata metadata = new Metadata();
        String filterResult =
                allAllowed.filter(sourceURL, metadata, "http://www.sourcedomain.com/index.html");
        Assertions.assertEquals("http://www.sourcedomain.com/index.html", filterResult);
        filterResult =
                allAllowed.filter(sourceURL, metadata, "http://www.anotherDomain.com/index.html");
        Assertions.assertNull(filterResult);
        filterResult =
                allAllowed.filter(sourceURL, metadata, "http://sub.sourcedomain.com/index.html");
        Assertions.assertNull(filterResult);
    }

    @Test
    void testWithinHostOnly() throws MalformedURLException {
        HostURLFilter allAllowed = createFilter(true, false);
        URL sourceURL = URLUtil.toURL("http://www.sourcedomain.com/index.html");
        Metadata metadata = new Metadata();
        String filterResult =
                allAllowed.filter(sourceURL, metadata, "http://www.sourcedomain.com/index.html");
        Assertions.assertEquals("http://www.sourcedomain.com/index.html", filterResult);
        filterResult =
                allAllowed.filter(sourceURL, metadata, "http://www.anotherDomain.com/index.html");
        Assertions.assertNull(filterResult);
        filterResult =
                allAllowed.filter(sourceURL, metadata, "http://sub.sourcedomain.com/index.html");
        Assertions.assertNull(filterResult);
    }

    @Test
    void testWithinDomain() throws MalformedURLException {
        HostURLFilter allAllowed = createFilter(false, true);
        URL sourceURL = URLUtil.toURL("http://www.sourcedomain.com/index.html");
        Metadata metadata = new Metadata();
        String filterResult =
                allAllowed.filter(sourceURL, metadata, "http://www.sourcedomain.com/index.html");
        Assertions.assertEquals("http://www.sourcedomain.com/index.html", filterResult);
        filterResult =
                allAllowed.filter(sourceURL, metadata, "http://www.anotherDomain.com/index.html");
        Assertions.assertNull(filterResult);
        filterResult =
                allAllowed.filter(sourceURL, metadata, "http://sub.sourcedomain.com/index.html");
        Assertions.assertEquals("http://sub.sourcedomain.com/index.html", filterResult);
    }
}
