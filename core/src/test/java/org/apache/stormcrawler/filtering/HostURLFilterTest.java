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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
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
        ObjectNode filterParams = new ObjectNode(JsonNodeFactory.instance);
        filterParams.put("ignoreOutsideHost", Boolean.valueOf(ignoreOutsideHost));
        filterParams.put("ignoreOutsideDomain", Boolean.valueOf(ignoreOutsideDomain));
        return createFilter(filterParams);
    }

    private HostURLFilter createFilter(ObjectNode filterParams) {
        HostURLFilter filter = new HostURLFilter();
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

    /** The two modes are independent, so ignoreOutsideDomain must be honoured on its own. */
    @Test
    void testWithinDomainWithoutHostParameter() throws MalformedURLException {
        ObjectNode filterParams = new ObjectNode(JsonNodeFactory.instance);
        filterParams.put("ignoreOutsideDomain", Boolean.TRUE);
        HostURLFilter withinDomain = createFilter(filterParams);
        URL sourceURL = URLUtil.toURL("http://www.sourcedomain.com/index.html");
        Metadata metadata = new Metadata();
        String filterResult =
                withinDomain.filter(sourceURL, metadata, "http://sub.sourcedomain.com/index.html");
        Assertions.assertEquals("http://sub.sourcedomain.com/index.html", filterResult);
        filterResult =
                withinDomain.filter(sourceURL, metadata, "http://www.anotherDomain.com/index.html");
        Assertions.assertNull(filterResult);
    }

    /**
     * A configuration which sets ignoreOutsideHost only must not fail on the missing domain key.
     */
    @Test
    void testHostParameterWithoutDomainParameter() throws MalformedURLException {
        ObjectNode filterParams = new ObjectNode(JsonNodeFactory.instance);
        filterParams.put("ignoreOutsideHost", Boolean.FALSE);
        HostURLFilter allAllowed = createFilter(filterParams);
        URL sourceURL = URLUtil.toURL("http://www.sourcedomain.com/index.html");
        Metadata metadata = new Metadata();
        String filterResult =
                allAllowed.filter(sourceURL, metadata, "http://www.anotherDomain.com/index.html");
        Assertions.assertEquals("http://www.anotherDomain.com/index.html", filterResult);
    }

    /**
     * A single filter instance is shared by all the fetcher threads of a bolt, each of them calling
     * it with its own source URL. The decision must depend on the source URL passed in and on
     * nothing which is kept between calls, see issue #2101.
     */
    @Test
    void testSourceUrlNotSharedBetweenThreads() throws Exception {
        final int threads = 8;
        final int iterations = 2000;
        final HostURLFilter filter = createFilter(true, false);
        final ExecutorService executor = Executors.newFixedThreadPool(threads);
        final CountDownLatch start = new CountDownLatch(1);
        final List<Future<String>> results = new ArrayList<>();
        try {
            for (int t = 0; t < threads; t++) {
                final String host = "host" + t + ".example.com";
                results.add(
                        executor.submit(
                                () -> {
                                    final URL sourceURL = URLUtil.toURL("http://" + host + "/");
                                    final Metadata metadata = new Metadata();
                                    start.await();
                                    for (int i = 0; i < iterations; i++) {
                                        final String sameHost = "http://" + host + "/page" + i;
                                        if (!sameHost.equals(
                                                filter.filter(sourceURL, metadata, sameHost))) {
                                            return "dropped a URL on its own host: " + sameHost;
                                        }
                                        final String otherHost =
                                                "http://elsewhere.example.org/page" + i;
                                        if (filter.filter(sourceURL, metadata, otherHost) != null) {
                                            return "admitted a URL outside the source host, source "
                                                    + sourceURL;
                                        }
                                    }
                                    return null;
                                }));
            }
            start.countDown();
            for (Future<String> result : results) {
                Assertions.assertNull(result.get(60, TimeUnit.SECONDS));
            }
        } finally {
            executor.shutdownNow();
        }
    }
}
