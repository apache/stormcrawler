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

import com.microsoft.playwright.Browser.NewContextOptions;
import org.apache.storm.Config;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for the browser context options built from the configuration. They do not need a
 * browser so they always run.
 */
class ContextOptionsTest {

    private static final String USER_AGENT = "StormCrawlerTest";

    private static final String PROXY = "http://proxy.example.com:3128";

    @Test
    void certificatesValidatedByDefault() {
        final NewContextOptions options =
                HttpProtocol.buildContextOptions(new Config(), USER_AGENT);
        Assertions.assertNull(options.proxy);
        Assertions.assertEquals(Boolean.FALSE, options.ignoreHTTPSErrors);
        Assertions.assertEquals(USER_AGENT, options.userAgent);
    }

    @Test
    void proxyDoesNotDisableCertificateValidation() {
        final Config conf = new Config();
        conf.put("http.proxy", PROXY);
        conf.put("http.proxy.username", "user");
        conf.put("http.proxy.password", "secret");
        final NewContextOptions options = HttpProtocol.buildContextOptions(conf, USER_AGENT);
        Assertions.assertNotNull(options.proxy);
        Assertions.assertEquals(PROXY, options.proxy.server);
        Assertions.assertEquals("user", options.proxy.username);
        Assertions.assertEquals("secret", options.proxy.password);
        Assertions.assertEquals(Boolean.FALSE, options.ignoreHTTPSErrors);
    }

    @Test
    void ignoreHttpsErrorsWithoutProxy() {
        final Config conf = new Config();
        conf.put(HttpProtocol.IGNORE_HTTPS_ERRORS_KEY, true);
        final NewContextOptions options = HttpProtocol.buildContextOptions(conf, USER_AGENT);
        Assertions.assertNull(options.proxy);
        Assertions.assertEquals(Boolean.TRUE, options.ignoreHTTPSErrors);
    }

    @Test
    void ignoreHttpsErrorsWithProxy() {
        final Config conf = new Config();
        conf.put("http.proxy", PROXY);
        conf.put(HttpProtocol.IGNORE_HTTPS_ERRORS_KEY, true);
        final NewContextOptions options = HttpProtocol.buildContextOptions(conf, USER_AGENT);
        Assertions.assertNotNull(options.proxy);
        Assertions.assertEquals(Boolean.TRUE, options.ignoreHTTPSErrors);
    }

    @Test
    void explicitFalseKeepsCertificateValidationWithProxy() {
        final Config conf = new Config();
        conf.put("http.proxy", PROXY);
        conf.put(HttpProtocol.IGNORE_HTTPS_ERRORS_KEY, false);
        final NewContextOptions options = HttpProtocol.buildContextOptions(conf, USER_AGENT);
        Assertions.assertNotNull(options.proxy);
        Assertions.assertEquals(Boolean.FALSE, options.ignoreHTTPSErrors);
    }
}
