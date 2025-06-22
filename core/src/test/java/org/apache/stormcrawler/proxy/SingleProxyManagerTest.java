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
package org.apache.stormcrawler.proxy;

import java.util.Optional;
import org.apache.storm.Config;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class SingleProxyManagerTest {

    @Test
    void testSimpleProxyManager() throws RuntimeException {
        Config config = new Config();
        config.put("http.proxy.host", "example.com");
        config.put("http.proxy.type", "HTTP");
        config.put("http.proxy.port", 8080);
        config.put("http.proxy.user", "user1");
        config.put("http.proxy.pass", "pass1");
        SingleProxyManager pm = new SingleProxyManager();
        pm.configure(config);
        Optional<SCProxy> proxyOptional = pm.getProxy(null);
        Assertions.assertTrue(proxyOptional.isPresent());
        SCProxy proxy = proxyOptional.get();
        Assertions.assertEquals(proxy.getProtocol(), "http");
        Assertions.assertEquals(proxy.getAddress(), "example.com");
        Assertions.assertEquals(proxy.getPort(), "8080");
        Assertions.assertEquals(proxy.getUsername(), "user1");
        Assertions.assertEquals(proxy.getPassword(), "pass1");
        Assertions.assertEquals(proxy.toString(), "http://user1:pass1@example.com:8080");
    }
}
