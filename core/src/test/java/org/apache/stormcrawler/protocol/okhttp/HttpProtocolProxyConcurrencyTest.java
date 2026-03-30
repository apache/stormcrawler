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

package org.apache.stormcrawler.protocol.okhttp;

import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.Optional;
import okhttp3.OkHttpClient;
import org.apache.storm.Config;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.protocol.AbstractProtocolTest;
import org.apache.stormcrawler.proxy.ProxyManager;
import org.apache.stormcrawler.proxy.SCProxy;
import org.junit.jupiter.api.Test;

/**
 * Verifies that a proxied request does not pollute the shared OkHttpClient.Builder.
 *
 * <p>Before the fix, {@code getProtocolOutput()} mutated the shared {@code builder} field with
 * proxy settings. This meant that after a proxied request, any subsequent call to {@code
 * builder.build()} (e.g., for a different proxied request) would start with the stale proxy from
 * the previous request. Under concurrency, this causes proxy settings from one thread to leak into
 * another's client.
 *
 * <p>The fix creates a per-request builder via {@code client.newBuilder()}, leaving the shared
 * builder untouched.
 */
class HttpProtocolProxyConcurrencyTest extends AbstractProtocolTest {

    /**
     * After a proxied request completes, the shared builder should not retain any proxy
     * configuration. With the old buggy code, {@code builder.proxy(proxy)} permanently set the
     * proxy on the shared builder. With the fix, the shared builder is never touched.
     */
    @Test
    void proxiedRequestShouldNotPolluteSharedBuilder() throws Exception {
        HttpProtocol protocol = new HttpProtocol();
        Config conf = new Config();
        conf.put("http.agent.name", "test");
        conf.put("http.agent.version", "1.0");
        conf.put("http.agent.description", "test");
        conf.put("http.agent.url", "http://test.example.com");
        conf.put("http.agent.email", "test@example.com");
        protocol.configure(conf);

        protocol.proxyManager =
                new ProxyManager() {
                    @Override
                    public void configure(Config conf) {}

                    @Override
                    public Optional<SCProxy> getProxy(Metadata metadata) {
                        return Optional.of(new SCProxy("http://127.0.0.1:19999"));
                    }
                };

        // Make a request that uses a proxy. The connection through the fake
        // proxy will fail, but the important thing is that getProtocolOutput
        // goes through the proxy builder path.
        try {
            protocol.getProtocolOutput("http://127.0.0.1:" + HTTP_PORT, Metadata.empty);
        } catch (Exception ignored) {
            // expected — the proxy at 127.0.0.1:19999 doesn't exist
        }

        // Now check the shared builder. With the old code, builder.proxy was
        // set to the proxy above. With the fix, builder is untouched.
        java.lang.reflect.Field builderField = HttpProtocol.class.getDeclaredField("builder");
        builderField.setAccessible(true);
        OkHttpClient.Builder sharedBuilder = (OkHttpClient.Builder) builderField.get(protocol);

        // Build a client from the shared builder and verify it has no proxy.
        // If the shared builder was polluted, this client would have a proxy.
        OkHttpClient clientFromSharedBuilder = sharedBuilder.build();
        assertNull(
                clientFromSharedBuilder.proxy(),
                "Shared builder should not retain proxy settings from a previous request. "
                        + "This indicates the bug where getProtocolOutput() mutates the shared "
                        + "builder instead of using a per-request builder.");
    }
}
