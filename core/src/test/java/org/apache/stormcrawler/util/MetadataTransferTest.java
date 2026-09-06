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

package org.apache.stormcrawler.util;

import java.net.MalformedURLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.stormcrawler.Metadata;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class MetadataTransferTest {

    @Test
    void testTransfer() throws MalformedURLException {
        Map<String, Object> conf = new HashMap<>();
        conf.put(MetadataTransfer.trackDepthParamName, true);
        conf.put(MetadataTransfer.metadataTransferParamName, List.of("cookie.*"));
        MetadataTransfer mdt = MetadataTransfer.getInstance(conf);
        Metadata parentMD = new Metadata();
        parentMD.addValue("cookie.id", "42");
        parentMD.addValue("cookie.source", "example.com");
        parentMD.addValue("fetchInterval", "200");
        Metadata outlinkMD =
                mdt.getMetaForOutlink(
                        "http://www.example.com/outlink.html", "http://www.example.com", parentMD);
        // test the value of track seed, depth and fetch fields
        Assertions.assertEquals("1", outlinkMD.getFirstValue(MetadataTransfer.depthKeyName));
        Set<String> expectedFields =
                Set.of(
                        MetadataTransfer.urlPathKeyName,
                        MetadataTransfer.depthKeyName,
                        "cookie.id",
                        "cookie.source");
        Assertions.assertEquals(expectedFields, outlinkMD.keySet());
        String[] urlpath = outlinkMD.getValues(MetadataTransfer.urlPathKeyName);
        Assertions.assertEquals(1, urlpath.length);
    }

    @Test
    void testCookieTransfer() throws MalformedURLException {
        Map<String, Object> conf = new HashMap<>();
        conf.put(
                MetadataTransfer.metadataTransferParamName,
                List.of("protocol.set-cookie", "protocol.set-cookie-origin"));
        Metadata parentMD = new Metadata();
        parentMD.addValue("protocol.set-cookie", "sid=42; Path=/");
        parentMD.addValue("protocol.set-cookie-origin", "http://www.example.com");
        Metadata outlinkMD =
                MetadataTransfer.getInstance(conf)
                        .getMetaForOutlink(
                                "http://www.example.com/outlink.html",
                                "http://www.example.com",
                                parentMD);
        Assertions.assertEquals("sid=42; Path=/", outlinkMD.getFirstValue("protocol.set-cookie"));
        Assertions.assertEquals(
                "http://www.example.com", outlinkMD.getFirstValue("protocol.set-cookie-origin"));
    }

    @Test
    void testCookieTransferWithWildcard() throws MalformedURLException {
        Map<String, Object> conf = new HashMap<>();
        conf.put(MetadataTransfer.metadataTransferParamName, List.of("protocol.set-cookie*"));
        Metadata parentMD = new Metadata();
        parentMD.addValue("protocol.set-cookie", "sid=42; Path=/");
        parentMD.addValue("protocol.set-cookie-origin", "http://www.example.com");
        Metadata outlinkMD =
                MetadataTransfer.getInstance(conf)
                        .getMetaForOutlink(
                                "http://www.example.com/outlink.html",
                                "http://www.example.com",
                                parentMD);
        Assertions.assertEquals("sid=42; Path=/", outlinkMD.getFirstValue("protocol.set-cookie"));
        Assertions.assertEquals(
                "http://www.example.com", outlinkMD.getFirstValue("protocol.set-cookie-origin"));
    }

    @Test
    void testCustomTransferClass() throws MalformedURLException {
        Map<String, Object> conf = new HashMap<>();
        conf.put(MetadataTransfer.metadataTransferClassParamName, "thisclassnameWillNEVERexist");
        boolean hasThrownException = false;
        try {
            MetadataTransfer.getInstance(conf);
        } catch (Exception e) {
            hasThrownException = true;
        }
        Assertions.assertTrue(hasThrownException);
        conf = new HashMap<>();
        conf.put(
                MetadataTransfer.metadataTransferClassParamName,
                MyCustomTransferClass.class.getName());
        hasThrownException = false;
        try {
            MetadataTransfer.getInstance(conf);
        } catch (Exception e) {
            hasThrownException = true;
        }
        Assertions.assertFalse(hasThrownException);
    }

    @Test
    void testFilterWithAsterisk() {
        Metadata metadata = new Metadata();
        metadata.addValue("fetch.statusCode", "500");
        metadata.addValue("fetch.error.count", "2");
        metadata.addValue("fetch.exception", "java.lang.Exception");
        metadata.addValue("fetchInterval", "200");
        metadata.addValue("isFeed", "true");
        metadata.addValue("depth", "1");
        // test for empty metadata.persist list
        Map<String, Object> conf = new HashMap<>();
        conf.put(MetadataTransfer.metadataPersistParamName, List.of());
        MetadataTransfer mdt = MetadataTransfer.getInstance(conf);
        Metadata filteredMetadata = mdt.filter(metadata);
        Assertions.assertEquals(2, filteredMetadata.size());
        // test for metadata.persist list with asterisk entry
        conf = new HashMap<>();
        conf.put(MetadataTransfer.metadataPersistParamName, List.of("fetch*"));
        mdt = MetadataTransfer.getInstance(conf);
        filteredMetadata = mdt.filter(metadata);
        Assertions.assertEquals(5, filteredMetadata.size());
        // test for metadata.persist list with asterisk entry after a dot
        conf = new HashMap<>();
        conf.put(MetadataTransfer.metadataPersistParamName, List.of("fetch.*"));
        mdt = MetadataTransfer.getInstance(conf);
        filteredMetadata = mdt.filter(metadata);
        Assertions.assertEquals(4, filteredMetadata.size());
        // test for persist all metadata
        conf = new HashMap<>();
        conf.put(MetadataTransfer.metadataPersistParamName, List.of("*"));
        mdt = MetadataTransfer.getInstance(conf);
        filteredMetadata = mdt.filter(metadata);
        Assertions.assertEquals(6, filteredMetadata.size());
    }

    static class MyCustomTransferClass extends MetadataTransfer {}

    @Test
    void testWildcardPrefixIsCaseInsensitiveAndSelective() throws MalformedURLException {
        Map<String, Object> conf = new HashMap<>();
        conf.put(MetadataTransfer.trackPathParamName, false);
        conf.put(MetadataTransfer.trackDepthParamName, false);
        conf.put(MetadataTransfer.metadataTransferParamName, List.of("Cookie.*", "exact"));
        Metadata parentMD = new Metadata();
        parentMD.addValue("cookie.id", "42");
        parentMD.addValue("cookies", "not a prefix match");
        parentMD.addValue("cook", "no");
        parentMD.addValue("exact", "yes");
        parentMD.addValue("exactly", "no");
        Metadata outlinkMD =
                MetadataTransfer.getInstance(conf)
                        .getMetaForOutlink(
                                "http://www.example.com/outlink.html",
                                "http://www.example.com",
                                parentMD);
        Assertions.assertEquals(Set.of("cookie.id", "exact"), outlinkMD.keySet());
        Assertions.assertEquals("42", outlinkMD.getFirstValue("cookie.id"));
    }

    /** Subclass that extends the transfer set after the base configuration, a supported pattern. */
    static class ExtendingTransferClass extends MetadataTransfer {
        @Override
        protected void configure(Map<String, Object> conf) {
            super.configure(conf);
            mdToTransfer.add("added.*");
        }
    }

    @Test
    void testSubclassCanExtendTransferSetInConfigure() throws MalformedURLException {
        Map<String, Object> conf = new HashMap<>();
        conf.put(MetadataTransfer.trackPathParamName, false);
        conf.put(MetadataTransfer.trackDepthParamName, false);
        conf.put(
                MetadataTransfer.metadataTransferClassParamName,
                ExtendingTransferClass.class.getName());
        conf.put(MetadataTransfer.metadataTransferParamName, List.of("cookie.*"));
        Metadata parentMD = new Metadata();
        parentMD.addValue("cookie.id", "42");
        parentMD.addValue("added.key", "yes");
        parentMD.addValue("other", "no");
        Metadata outlinkMD =
                MetadataTransfer.getInstance(conf)
                        .getMetaForOutlink(
                                "http://www.example.com/outlink.html",
                                "http://www.example.com",
                                parentMD);
        Assertions.assertEquals(Set.of("cookie.id", "added.key"), outlinkMD.keySet());
    }

    @Test
    void testSameSizeMutationOfTransferSetIsHonoured() throws MalformedURLException {
        Map<String, Object> conf = new HashMap<>();
        conf.put(MetadataTransfer.trackPathParamName, false);
        conf.put(MetadataTransfer.trackDepthParamName, false);
        conf.put(MetadataTransfer.metadataTransferParamName, List.of("cookie.*"));
        MetadataTransfer mdt = MetadataTransfer.getInstance(conf);
        Metadata parentMD = new Metadata();
        parentMD.addValue("cookie.id", "42");
        parentMD.addValue("other", "yes");
        Assertions.assertEquals(
                Set.of("cookie.id"),
                mdt.getMetaForOutlink(
                                "http://www.example.com/outlink.html",
                                "http://www.example.com",
                                parentMD)
                        .keySet());
        // same size, different content: the compiled filter must not be stale
        mdt.mdToTransfer.remove("cookie.*");
        mdt.mdToTransfer.add("other");
        Assertions.assertEquals(
                Set.of("other"),
                mdt.getMetaForOutlink(
                                "http://www.example.com/outlink.html",
                                "http://www.example.com",
                                parentMD)
                        .keySet());
    }

    @Test
    void testNullValueArrayIsSkipped() throws MalformedURLException {
        Map<String, Object> conf = new HashMap<>();
        conf.put(MetadataTransfer.trackPathParamName, false);
        conf.put(MetadataTransfer.trackDepthParamName, false);
        conf.put(MetadataTransfer.metadataTransferParamName, List.of("cookie.*", "exact"));
        Map<String, String[]> backing = new HashMap<>();
        backing.put("cookie.id", new String[] {"42"});
        backing.put("cookie.broken", null);
        backing.put("exact", null);
        Metadata outlinkMD =
                MetadataTransfer.getInstance(conf)
                        .getMetaForOutlink(
                                "http://www.example.com/outlink.html",
                                "http://www.example.com",
                                new Metadata(backing));
        Assertions.assertEquals(Set.of("cookie.id"), outlinkMD.keySet());
    }
}
