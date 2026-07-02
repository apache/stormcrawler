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
import org.apache.stormcrawler.Metadata;
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
        Assertions.assertEquals("http", proxy.getProtocol());
        Assertions.assertEquals("example.com", proxy.getAddress());
        Assertions.assertEquals("8080", proxy.getPort());
        Assertions.assertEquals("user1", proxy.getUsername());
        Assertions.assertEquals("pass1", proxy.getPassword());
        Assertions.assertEquals("http://user1:pass1@example.com:8080", proxy.toString());
    }

    @Test
    void configuredProxyFieldsCanUseNoAuth() {
        Config config = new Config();
        config.put("http.proxy.host", "example.com");
        config.put("http.proxy.type", "HTTP");
        config.put("http.proxy.port", 8080);
        SingleProxyManager pm = new SingleProxyManager();
        pm.configure(config);

        Optional<SCProxy> proxyOptional = pm.getProxy(null);
        Assertions.assertTrue(proxyOptional.isPresent());
        SCProxy proxy = proxyOptional.get();
        Assertions.assertNull(proxy.getUsername());
        Assertions.assertNull(proxy.getPassword());
        Assertions.assertEquals("http://example.com:8080", proxy.toString());
    }

    @Test
    void configuredProxyFieldsPreserveReservedAuthCharacters() {
        Config config = new Config();
        config.put("http.proxy.host", "example.com");
        config.put("http.proxy.type", "HTTP");
        config.put("http.proxy.port", 8080);
        config.put("http.proxy.user", "user:name");
        config.put("http.proxy.pass", "pass:word@token");
        SingleProxyManager pm = new SingleProxyManager();
        pm.configure(config);

        Optional<SCProxy> proxyOptional = pm.getProxy(null);
        Assertions.assertTrue(proxyOptional.isPresent());
        SCProxy proxy = proxyOptional.get();
        Assertions.assertEquals("user:name", proxy.getUsername());
        Assertions.assertEquals("pass:word@token", proxy.getPassword());
    }

    @Test
    void configuredProxyPortMustBeInRange() {
        assertInvalidConfiguredProxyPort(0);
        assertInvalidConfiguredProxyPort(65536);
    }

    @Test
    void metadataConnectionStringOverridesConfiguredProxy() {
        SingleProxyManager pm = new SingleProxyManager();
        pm.configure(configuredSingleProxy());

        Metadata metadata = new Metadata();
        metadata.setValue("http.proxy", "https://metadata.example.com:9443");

        Optional<SCProxy> proxyOptional = pm.getProxy(metadata);
        Assertions.assertTrue(proxyOptional.isPresent());
        Assertions.assertEquals(
                "https://metadata.example.com:9443", proxyOptional.get().toString());
    }

    @Test
    void metadataConnectionStringOverridesComponentFields() {
        SingleProxyManager pm = new SingleProxyManager();
        pm.configure(configuredSingleProxy());

        Metadata metadata = new Metadata();
        metadata.setValue("http.proxy", "https://full.example.com:9443");
        metadata.setValue("http.proxy.host", "component.example.com");
        metadata.setValue("http.proxy.port", "8081");

        Optional<SCProxy> proxyOptional = pm.getProxy(metadata);
        Assertions.assertTrue(proxyOptional.isPresent());
        Assertions.assertEquals("https://full.example.com:9443", proxyOptional.get().toString());
    }

    @Test
    void metadataProxyFieldsOverrideConfiguredProxy() {
        SingleProxyManager pm = new SingleProxyManager();
        pm.configure(configuredSingleProxy());

        Metadata metadata = new Metadata();
        metadata.setValue("http.proxy.host", "metadata.example.com");
        metadata.setValue("http.proxy.port", "9443");
        metadata.setValue("http.proxy.type", "HTTPS");
        metadata.setValue("http.proxy.user", "metadata-user");
        metadata.setValue("http.proxy.pass", "metadata-pass");

        Optional<SCProxy> proxyOptional = pm.getProxy(metadata);
        Assertions.assertTrue(proxyOptional.isPresent());
        Assertions.assertEquals(
                "https://metadata-user:metadata-pass@metadata.example.com:9443",
                proxyOptional.get().toString());
    }

    @Test
    void metadataProxyFieldsPreserveReservedAuthCharacters() {
        SingleProxyManager pm = new SingleProxyManager();
        pm.configure(configuredSingleProxy());

        Metadata metadata = new Metadata();
        metadata.setValue("http.proxy.host", "metadata.example.com");
        metadata.setValue("http.proxy.port", "9443");
        metadata.setValue("http.proxy.user", "metadata:user");
        metadata.setValue("http.proxy.pass", "metadata:pass@token");

        Optional<SCProxy> proxyOptional = pm.getProxy(metadata);
        Assertions.assertTrue(proxyOptional.isPresent());
        SCProxy proxy = proxyOptional.get();
        Assertions.assertEquals("metadata:user", proxy.getUsername());
        Assertions.assertEquals("metadata:pass@token", proxy.getPassword());
    }

    @Test
    void metadataSkipDisablesConfiguredProxy() {
        SingleProxyManager pm = new SingleProxyManager();
        pm.configure(configuredSingleProxy());

        Metadata metadata = new Metadata();
        metadata.setValue("http.proxy.skip", "true");

        Assertions.assertTrue(pm.getProxy(metadata).isEmpty());
    }

    @Test
    void metadataSkipWinsOverMetadataOverride() {
        SingleProxyManager pm = new SingleProxyManager();
        pm.configure(configuredSingleProxy());

        Metadata metadata = new Metadata();
        metadata.setValue("http.proxy.skip", "true");
        metadata.setValue("http.proxy", "http://metadata.example.com:8080");

        Assertions.assertTrue(pm.getProxy(metadata).isEmpty());
    }

    @Test
    void metadataSkipFalseUsesConfiguredProxy() {
        SingleProxyManager pm = new SingleProxyManager();
        pm.configure(configuredSingleProxy());

        Metadata metadata = new Metadata();
        metadata.setValue("http.proxy.skip", "false");

        Optional<SCProxy> proxyOptional = pm.getProxy(metadata);
        Assertions.assertTrue(proxyOptional.isPresent());
        Assertions.assertEquals(
                "http://user1:pass1@example.com:8080", proxyOptional.get().toString());
    }

    @Test
    void invalidMetadataSkipFailsFast() {
        SingleProxyManager pm = new SingleProxyManager();
        pm.configure(configuredSingleProxy());

        Metadata metadata = new Metadata();
        metadata.setValue("http.proxy.skip", "not-a-boolean");

        Assertions.assertThrows(IllegalArgumentException.class, () -> pm.getProxy(metadata));
    }

    @Test
    void explicitManagerWithoutDefaultProxySupportsMetadataOnlyUrls() {
        SingleProxyManager pm = new SingleProxyManager();
        pm.configure(new Config());

        Assertions.assertTrue(pm.getProxy(new Metadata()).isEmpty());

        Metadata metadata = new Metadata();
        metadata.setValue("http.proxy", "http://metadata-only.example.com:8080");

        Optional<SCProxy> proxyOptional = pm.getProxy(metadata);
        Assertions.assertTrue(proxyOptional.isPresent());
        Assertions.assertEquals(
                "http://metadata-only.example.com:8080", proxyOptional.get().toString());
    }

    @Test
    void invalidMetadataProxyFailsFast() {
        SingleProxyManager pm = new SingleProxyManager();
        pm.configure(configuredSingleProxy());

        Metadata metadata = new Metadata();
        metadata.setValue("http.proxy", "metadata.example.com:8080");

        Assertions.assertThrows(IllegalArgumentException.class, () -> pm.getProxy(metadata));
    }

    @Test
    void invalidMetadataProxyDoesNotLeakCredentialsInMessage() {
        SingleProxyManager pm = new SingleProxyManager();
        pm.configure(configuredSingleProxy());

        Metadata metadata = new Metadata();
        metadata.setValue(
                "http.proxy",
                "http://metadata-user:metadata-secret@metadata.example.com:not-a-port");

        IllegalArgumentException exception =
                Assertions.assertThrows(
                        IllegalArgumentException.class, () -> pm.getProxy(metadata));
        Assertions.assertEquals(
                "metadata key `http.proxy` must be a valid proxy connection string",
                exception.getMessage());
        Assertions.assertFalse(exception.getMessage().contains("metadata-user"));
        Assertions.assertFalse(exception.getMessage().contains("metadata-secret"));
        Assertions.assertNull(exception.getCause());
    }

    @Test
    void invalidMetadataProxyPortsFailFast() {
        assertInvalidMetadataProxyPort("0");
        assertInvalidMetadataProxyPort("65536");
        assertInvalidMetadataProxyPort("not-a-port");
    }

    @Test
    void metadataProxyFieldsRequireCompleteAuth() {
        assertInvalidMetadataProxyAuth("metadata-user", null);
        assertInvalidMetadataProxyAuth(null, "metadata-pass");
    }

    private void assertInvalidConfiguredProxyPort(int port) {
        Config config = configuredSingleProxy();
        config.put("http.proxy.port", port);

        SingleProxyManager pm = new SingleProxyManager();
        Assertions.assertThrows(IllegalArgumentException.class, () -> pm.configure(config));
    }

    private void assertInvalidMetadataProxyPort(String port) {
        SingleProxyManager pm = new SingleProxyManager();
        pm.configure(configuredSingleProxy());

        Metadata metadata = new Metadata();
        metadata.setValue("http.proxy.host", "metadata.example.com");
        metadata.setValue("http.proxy.port", port);

        Assertions.assertThrows(IllegalArgumentException.class, () -> pm.getProxy(metadata));
    }

    private void assertInvalidMetadataProxyAuth(String username, String password) {
        SingleProxyManager pm = new SingleProxyManager();
        pm.configure(configuredSingleProxy());

        Metadata metadata = new Metadata();
        metadata.setValue("http.proxy.host", "metadata.example.com");
        metadata.setValue("http.proxy.port", "8080");
        if (username != null) {
            metadata.setValue("http.proxy.user", username);
        }
        if (password != null) {
            metadata.setValue("http.proxy.pass", password);
        }

        Assertions.assertThrows(IllegalArgumentException.class, () -> pm.getProxy(metadata));
    }

    private Config configuredSingleProxy() {
        Config config = new Config();
        config.put("http.proxy.host", "example.com");
        config.put("http.proxy.type", "HTTP");
        config.put("http.proxy.port", 8080);
        config.put("http.proxy.user", "user1");
        config.put("http.proxy.pass", "pass1");
        return config;
    }
}
