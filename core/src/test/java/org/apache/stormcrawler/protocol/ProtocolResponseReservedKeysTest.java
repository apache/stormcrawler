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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** The guard the protocols apply to response header names, see issue #2091. */
class ProtocolResponseReservedKeysTest {

    @ParameterizedTest
    @ValueSource(
            strings = {
                "_request.headers_",
                "_response.headers_",
                "_response.ip_",
                "_request.time_",
                "_protocol_versions_",
                "_cipher_suite_",
                "http.trimmed",
                "http.trimmed.reason",
                "_redirectedTo",
                "metrics.dns.resolution.msec",
                "metrics.anything.a.server.invents"
            })
    void reservedKeysAreRecognised(String key) {
        assertTrue(ProtocolResponse.isReservedMetadataKey(key));
    }

    /**
     * Header names arrive in whatever case the server chose and metadata keys are lowercased, so
     * the check has to be case insensitive in both directions. {@code _redirectedTo} is the key
     * which is not already lowercase.
     */
    @ParameterizedTest
    @ValueSource(strings = {"_redirectedTo", "_redirectedto", "_REDIRECTEDTO", "METRICS.foo"})
    void theCheckIsCaseInsensitive(String key) {
        assertTrue(ProtocolResponse.isReservedMetadataKey(key));
    }

    @Test
    void ordinaryHeadersAreNotReserved() {
        assertFalse(ProtocolResponse.isReservedMetadataKey("content-type"));
        assertFalse(ProtocolResponse.isReservedMetadataKey("set-cookie"));
        assertFalse(ProtocolResponse.isReservedMetadataKey("etag"));
        // near misses
        assertFalse(ProtocolResponse.isReservedMetadataKey("metrics"));
        assertFalse(ProtocolResponse.isReservedMetadataKey("x-metrics.foo"));
        assertFalse(ProtocolResponse.isReservedMetadataKey("http.trimmedish"));
    }
}
