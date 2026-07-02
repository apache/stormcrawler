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

import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Optional;
import java.util.function.Supplier;
import org.apache.storm.Config;
import org.apache.stormcrawler.Metadata;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class MultiProxyManagerTest {

    @Test
    void testMultiProxyManagerConstructorArray() {
        String[] proxyStrings = {
            "http://example.com:8080",
            "https://example.com:8080",
            "http://user1:pass1@example.com:8080",
            "sock5://user1:pass1@example.com:8080",
            "http://example.com:80",
            "sock5://example.com:64000"
        };
        MultiProxyManager pm = new MultiProxyManager();
        pm.configure(MultiProxyManager.ProxyRotation.RANDOM, proxyStrings);
        Assertions.assertEquals(pm.proxyCount(), proxyStrings.length);
    }

    @Test
    void testMultiProxyManagerConstructorFile() throws IOException {
        String[] proxyStrings = {
            "http://example.com:8080",
            "https://example.com:8080",
            "http://user1:pass1@example.com:8080",
            "sock5://user1:pass1@example.com:8080",
            "http://example.com:80",
            "sock5://example.com:64000"
        };
        String fileName = Files.createTempFile("proxies", "txt").toString();
        FileWriter writer = new FileWriter(fileName, StandardCharsets.UTF_8);
        for (String proxyString : proxyStrings) {
            writer.write("# fake comment to test" + "\n");
            writer.write("// fake comment to test" + "\n");
            writer.write("       " + "\n");
            writer.write("\n");
            writer.write(proxyString + "\n");
        }
        writer.close();
        Config config = new Config();
        config.put("http.proxy.file", fileName);
        config.put("http.proxy.rotation", "ROUND_ROBIN");
        MultiProxyManager pm = new MultiProxyManager();
        pm.configure(config);
        Assertions.assertEquals(pm.proxyCount(), proxyStrings.length);
        Files.deleteIfExists(Paths.get(fileName));
    }

    @Test
    void testGetRandom() {
        String[] proxyStrings = {
            "http://example.com:8080",
            "https://example.com:8080",
            "http://user1:pass1@example.com:8080",
            "sock5://user1:pass1@example.com:8080",
            "http://example.com:80",
            "sock5://example.com:64000"
        };
        MultiProxyManager pm = new MultiProxyManager();
        pm.configure(MultiProxyManager.ProxyRotation.RANDOM, proxyStrings);
        for (int i = 0; i < 1000; i++) {
            assertAndGetProxyCreation(() -> pm.getProxy(null), true);
        }
    }

    @Test
    void testGetRoundRobin() {
        String[] proxyStrings = {
            "http://example.com:8080",
            "https://example.com:8080",
            "http://user1:pass1@example.com:8080",
            "sock5://user1:pass1@example.com:8080",
            "http://example.com:80",
            "sock5://example.com:64000"
        };
        MultiProxyManager pm = new MultiProxyManager();
        pm.configure(MultiProxyManager.ProxyRotation.ROUND_ROBIN, proxyStrings);
        SCProxy proxy1 = assertAndGetProxyCreation(() -> pm.getProxy(null), false);
        SCProxy proxy2 = assertAndGetProxyCreation(() -> pm.getProxy(null), false);
        SCProxy proxy3 = assertAndGetProxyCreation(() -> pm.getProxy(null), false);
        Assertions.assertNotEquals(proxy1.toString(), proxy2.toString());
        Assertions.assertNotEquals(proxy1.toString(), proxy3.toString());
        Assertions.assertNotEquals(proxy2.toString(), proxy1.toString());
        Assertions.assertNotEquals(proxy2.toString(), proxy3.toString());
        Assertions.assertNotEquals(proxy3.toString(), proxy1.toString());
        Assertions.assertNotEquals(proxy3.toString(), proxy2.toString());
        for (int i = 0; i < 3; i++) {
            pm.getProxy(null);
        }
        SCProxy proxy4 = assertAndGetProxyCreation(() -> pm.getProxy(null), false);
        SCProxy proxy5 = assertAndGetProxyCreation(() -> pm.getProxy(null), false);
        SCProxy proxy6 = assertAndGetProxyCreation(() -> pm.getProxy(null), false);
        Assertions.assertNotEquals(proxy4.toString(), proxy5.toString());
        Assertions.assertNotEquals(proxy4.toString(), proxy6.toString());
        Assertions.assertNotEquals(proxy5.toString(), proxy4.toString());
        Assertions.assertNotEquals(proxy5.toString(), proxy6.toString());
        Assertions.assertNotEquals(proxy6.toString(), proxy4.toString());
        Assertions.assertNotEquals(proxy6.toString(), proxy5.toString());
        Assertions.assertEquals(proxy1.toString(), proxy4.toString());
        Assertions.assertEquals(proxy2.toString(), proxy5.toString());
        Assertions.assertEquals(proxy3.toString(), proxy6.toString());
    }

    @Test
    void testGetLeastUsed() {
        String[] proxyStrings = {
            "http://example.com:8080",
            "https://example.com:8080",
            "http://user1:pass1@example.com:8080",
            "sock5://user1:pass1@example.com:8080",
            "http://example.com:80",
            "sock5://example.com:64000"
        };
        MultiProxyManager pm = new MultiProxyManager();
        pm.configure(MultiProxyManager.ProxyRotation.LEAST_USED, proxyStrings);
        SCProxy proxy1 = assertAndGetProxyCreation(() -> pm.getProxy(null), false);
        SCProxy proxy2 = assertAndGetProxyCreation(() -> pm.getProxy(null), false);
        SCProxy proxy3 = assertAndGetProxyCreation(() -> pm.getProxy(null), false);
        Assertions.assertNotEquals(proxy1.toString(), proxy2.toString());
        Assertions.assertNotEquals(proxy1.toString(), proxy3.toString());
        Assertions.assertNotEquals(proxy2.toString(), proxy1.toString());
        Assertions.assertNotEquals(proxy2.toString(), proxy3.toString());
        Assertions.assertNotEquals(proxy3.toString(), proxy1.toString());
        Assertions.assertNotEquals(proxy3.toString(), proxy2.toString());
        Assertions.assertEquals(1, proxy1.getUsage());
        Assertions.assertEquals(1, proxy2.getUsage());
        Assertions.assertEquals(1, proxy3.getUsage());
        for (int i = 0; i < 3; i++) {
            pm.getProxy(null);
        }
        SCProxy proxy4 = assertAndGetProxyCreation(() -> pm.getProxy(null), false);
        SCProxy proxy5 = assertAndGetProxyCreation(() -> pm.getProxy(null), false);
        SCProxy proxy6 = assertAndGetProxyCreation(() -> pm.getProxy(null), false);
        Assertions.assertNotEquals(proxy4.toString(), proxy5.toString());
        Assertions.assertNotEquals(proxy4.toString(), proxy6.toString());
        Assertions.assertNotEquals(proxy5.toString(), proxy4.toString());
        Assertions.assertNotEquals(proxy5.toString(), proxy6.toString());
        Assertions.assertNotEquals(proxy6.toString(), proxy4.toString());
        Assertions.assertNotEquals(proxy6.toString(), proxy5.toString());
        Assertions.assertEquals(2, proxy4.getUsage());
        Assertions.assertEquals(2, proxy5.getUsage());
        Assertions.assertEquals(2, proxy6.getUsage());
        Assertions.assertEquals(proxy1.toString(), proxy4.toString());
        Assertions.assertEquals(proxy2.toString(), proxy5.toString());
        Assertions.assertEquals(proxy3.toString(), proxy6.toString());
    }

    @Test
    void metadataSkipDoesNotAdvanceRoundRobinRotation() {
        String[] proxyStrings = {
            "http://first.example.com:8080", "http://second.example.com:8080",
        };
        MultiProxyManager pm = new MultiProxyManager();
        pm.configure(MultiProxyManager.ProxyRotation.ROUND_ROBIN, proxyStrings);

        Metadata metadata = new Metadata();
        metadata.setValue("http.proxy.skip", "true");
        metadata.setValue("http.proxy", "http://metadata.example.com:8080");

        Assertions.assertTrue(pm.getProxy(metadata).isEmpty());
        Assertions.assertEquals(
                "http://first.example.com:8080", pm.getProxy(null).get().toString());
    }

    @Test
    void metadataSkipFalseUsesRoundRobinRotation() {
        String[] proxyStrings = {
            "http://first.example.com:8080", "http://second.example.com:8080",
        };
        MultiProxyManager pm = new MultiProxyManager();
        pm.configure(MultiProxyManager.ProxyRotation.ROUND_ROBIN, proxyStrings);

        Metadata metadata = new Metadata();
        metadata.setValue("http.proxy.skip", "false");

        Assertions.assertEquals(
                "http://first.example.com:8080", pm.getProxy(metadata).get().toString());
    }

    @Test
    void invalidMetadataSkipFailsFast() {
        String[] proxyStrings = {
            "http://first.example.com:8080", "http://second.example.com:8080",
        };
        MultiProxyManager pm = new MultiProxyManager();
        pm.configure(MultiProxyManager.ProxyRotation.ROUND_ROBIN, proxyStrings);

        Metadata metadata = new Metadata();
        metadata.setValue("http.proxy.skip", "not-a-boolean");

        Assertions.assertThrows(IllegalArgumentException.class, () -> pm.getProxy(metadata));
    }

    @Test
    void metadataOverrideUsesConfiguredProxyInstanceWhenMatched() {
        String[] proxyStrings = {
            "http://first.example.com:8080", "http://second.example.com:8080",
        };
        MultiProxyManager pm = new MultiProxyManager();
        pm.configure(MultiProxyManager.ProxyRotation.LEAST_USED, proxyStrings);

        Metadata metadata = new Metadata();
        metadata.setValue("http.proxy", "http://second.example.com:8080");

        SCProxy metadataProxy = pm.getProxy(metadata).get();
        Assertions.assertEquals("http://second.example.com:8080", metadataProxy.toString());
        Assertions.assertEquals(1, metadataProxy.getUsage());

        SCProxy nextProxy = pm.getProxy(null).get();
        Assertions.assertEquals("http://first.example.com:8080", nextProxy.toString());
    }

    @Test
    void metadataComponentOverrideUsesConfiguredProxyInstanceWhenProtocolCaseDiffers() {
        String[] proxyStrings = {
            "http://first.example.com:8080", "HTTP://second.example.com:8080",
        };
        MultiProxyManager pm = new MultiProxyManager();
        pm.configure(MultiProxyManager.ProxyRotation.LEAST_USED, proxyStrings);

        SCProxy firstProxy = pm.getProxy(null).get();
        Assertions.assertEquals("http://first.example.com:8080", firstProxy.toString());
        Assertions.assertEquals(1, firstProxy.getUsage());

        Metadata metadata = new Metadata();
        metadata.setValue("http.proxy.host", "second.example.com");
        metadata.setValue("http.proxy.port", "8080");
        metadata.setValue("http.proxy.type", "HTTP");

        SCProxy metadataProxy = pm.getProxy(metadata).get();
        Assertions.assertEquals("HTTP://second.example.com:8080", metadataProxy.toString());
        Assertions.assertEquals(1, metadataProxy.getUsage());

        SCProxy nextProxy = pm.getProxy(null).get();
        Assertions.assertEquals("http://first.example.com:8080", nextProxy.toString());
        Assertions.assertEquals(2, nextProxy.getUsage());
    }

    @Test
    void metadataOverrideCanUseProxyOutsideConfiguredRotation() {
        String[] proxyStrings = {
            "http://first.example.com:8080", "http://second.example.com:8080",
        };
        MultiProxyManager pm = new MultiProxyManager();
        pm.configure(MultiProxyManager.ProxyRotation.ROUND_ROBIN, proxyStrings);

        Metadata metadata = new Metadata();
        metadata.setValue("http.proxy.host", "metadata.example.com");
        metadata.setValue("http.proxy.port", "9443");
        metadata.setValue("http.proxy.type", "HTTPS");

        SCProxy metadataProxy = pm.getProxy(metadata).get();
        Assertions.assertEquals("https://metadata.example.com:9443", metadataProxy.toString());
        Assertions.assertEquals(1, metadataProxy.getUsage());

        Assertions.assertEquals(
                "http://first.example.com:8080", pm.getProxy(null).get().toString());
    }

    @Test
    void metadataOverridePreservesReservedAuthCharacters() {
        String[] proxyStrings = {
            "http://first.example.com:8080", "http://second.example.com:8080",
        };
        MultiProxyManager pm = new MultiProxyManager();
        pm.configure(MultiProxyManager.ProxyRotation.ROUND_ROBIN, proxyStrings);

        Metadata metadata = new Metadata();
        metadata.setValue("http.proxy.host", "metadata.example.com");
        metadata.setValue("http.proxy.port", "9443");
        metadata.setValue("http.proxy.user", "metadata:user");
        metadata.setValue("http.proxy.pass", "metadata:pass@token");

        SCProxy metadataProxy = pm.getProxy(metadata).get();
        Assertions.assertEquals("metadata:user", metadataProxy.getUsername());
        Assertions.assertEquals("metadata:pass@token", metadataProxy.getPassword());
        Assertions.assertEquals(1, metadataProxy.getUsage());
    }

    @Test
    void invalidMetadataProxyFailsFast() {
        String[] proxyStrings = {
            "http://first.example.com:8080", "http://second.example.com:8080",
        };
        MultiProxyManager pm = new MultiProxyManager();
        pm.configure(MultiProxyManager.ProxyRotation.ROUND_ROBIN, proxyStrings);

        assertInvalidMetadataProxyPort(pm, "0");
        assertInvalidMetadataProxyPort(pm, "65536");
        assertInvalidMetadataProxyPort(pm, "not-a-port");
        assertInvalidMetadataProxyAuth(pm, "metadata-user", null);
        assertInvalidMetadataProxyAuth(pm, null, "metadata-pass");
    }

    private void assertInvalidMetadataProxyPort(MultiProxyManager pm, String port) {
        Metadata metadata = new Metadata();
        metadata.setValue("http.proxy.host", "metadata.example.com");
        metadata.setValue("http.proxy.port", port);

        Assertions.assertThrows(IllegalArgumentException.class, () -> pm.getProxy(metadata));
    }

    private void assertInvalidMetadataProxyAuth(
            MultiProxyManager pm, String username, String password) {
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

    private SCProxy assertAndGetProxyCreation(
            Supplier<Optional<SCProxy>> proxySupplier, boolean validateProxyContent) {
        Optional<SCProxy> proxyOptional = proxySupplier.get();
        Assertions.assertTrue(proxyOptional.isPresent());
        SCProxy proxy = proxyOptional.get();
        if (validateProxyContent) {
            Assertions.assertFalse(proxy.toString().isEmpty());
        }
        return proxy;
    }
}
