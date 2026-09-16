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

package org.apache.stormcrawler.protocol.playwright;

import com.microsoft.playwright.options.ServiceWorkerPolicy;
import org.apache.storm.Config;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Checks the IP filter applied to the requests of the browser. Does not need a browser. */
class IPFilterTest {

    private HttpProtocol protocol(final Config conf) {
        final HttpProtocol protocol = new HttpProtocol();
        protocol.configureIPFilter(conf);
        return protocol;
    }

    private Config excludeLocal() {
        final Config conf = new Config();
        conf.put("http.filter.ipaddress.exclude", "localhost,sitelocal");
        return conf;
    }

    @Test
    void everythingAllowedWithoutRules() {
        final HttpProtocol protocol = protocol(new Config());
        Assertions.assertTrue(protocol.isAllowedAddress("http://127.0.0.1/"));
    }

    @Test
    void excludedAddressesAreRejected() {
        final HttpProtocol protocol = protocol(excludeLocal());
        Assertions.assertFalse(protocol.isAllowedAddress("http://127.0.0.1:8080/page.html"));
        Assertions.assertFalse(protocol.isAllowedAddress("https://localhost/"));
        Assertions.assertFalse(protocol.isAllowedAddress("http://[::1]/"));
        Assertions.assertFalse(protocol.isAllowedAddress("http://192.168.1.1/"));
        Assertions.assertTrue(protocol.isAllowedAddress("http://8.8.8.8/"));
    }

    @Test
    void urlsWithoutConnectionAreAllowed() {
        final HttpProtocol protocol = protocol(excludeLocal());
        Assertions.assertTrue(protocol.isAllowedAddress("data:text/plain,hello"));
    }

    @Test
    void urlsChromiumSendsAsIsAreParsed() {
        final HttpProtocol protocol = protocol(excludeLocal());
        Assertions.assertFalse(protocol.isAllowedAddress("http://127.0.0.1/a|b[c]?q={x}^"));
        Assertions.assertTrue(protocol.isAllowedAddress("http://8.8.8.8/a|b[c]?q={x}^"));
    }

    @Test
    void serviceWorkersBlockedOnlyWithRules() {
        Assertions.assertEquals(
                ServiceWorkerPolicy.BLOCK,
                protocol(excludeLocal())
                        .buildContextOptions(excludeLocal(), "test")
                        .serviceWorkers);
        Assertions.assertNull(
                protocol(new Config()).buildContextOptions(new Config(), "test").serviceWorkers);
    }

    @Test
    void unresolvableHostsAreRejected() {
        final HttpProtocol protocol = protocol(excludeLocal());
        Assertions.assertFalse(protocol.isAllowedAddress("http://does-not-exist.invalid/"));
    }

    @Test
    void notAppliedThroughProxy() {
        final Config conf = excludeLocal();
        conf.put("http.proxy", "http://proxy.example.com:3128");
        final HttpProtocol protocol = protocol(conf);
        Assertions.assertTrue(protocol.isAllowedAddress("http://127.0.0.1/"));
    }
}
