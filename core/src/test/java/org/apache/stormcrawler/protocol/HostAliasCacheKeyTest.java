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

package org.apache.stormcrawler.protocol;

import java.net.URL;
import okhttp3.HttpUrl;
import org.apache.stormcrawler.util.URLUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Two URLs whose host strings differ only by percent-escaping or by a trailing dot are sent to the
 * same server by okhttp, so they must share one robots.txt cache entry and one politeness queue.
 */
class HostAliasCacheKeyTest {

    @Test
    void okhttpCollapsesHostAliases() {
        // what the client actually connects to; okhttp percent-decodes and
        // lowercases the host but keeps a trailing dot (as the JDK does)
        Assertions.assertEquals(
                "example.org", HttpUrl.parse("http://%65xample.org/a").host(), "percent-escaped");
        Assertions.assertEquals(
                "example.org", HttpUrl.parse("http://exampl%65.org/a").host(), "percent-escaped");
        Assertions.assertEquals(
                "example.org", HttpUrl.parse("http://EXAMPLE.org/a").host(), "upper case");
        Assertions.assertEquals(
                "example.org.", HttpUrl.parse("http://example.org./a").host(), "trailing dot");
    }

    @Test
    void canonicalHostMatchesWhatOkHttpConnectsTo() throws Exception {
        Assertions.assertEquals(
                HttpUrl.parse("http://exampl%65.org/a").host(),
                URLUtil.getCanonicalHost(new URL("http://exampl%65.org/a")));
        Assertions.assertEquals(
                "example.org", URLUtil.getCanonicalHost(new URL("http://example.org./a")));
        Assertions.assertEquals(
                "example.org", URLUtil.getCanonicalHost(new URL("http://EXAMPLE.org/a")));
    }

    @Test
    void robotsCacheKeyIsTheSameForHostAliases() throws Exception {
        String canonical = HttpRobotRulesParser.getCacheKey(new URL("http://example.org/a"));
        Assertions.assertEquals(
                canonical, HttpRobotRulesParser.getCacheKey(new URL("http://exampl%65.org/a")));
        Assertions.assertEquals(
                canonical, HttpRobotRulesParser.getCacheKey(new URL("http://example.org./a")));
        Assertions.assertEquals(
                canonical, HttpRobotRulesParser.getCacheKey(new URL("http://EXAMPLE.org/a")));
    }
}
