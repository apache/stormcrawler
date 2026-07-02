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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.net.MalformedURLException;
import java.net.URL;
import org.junit.jupiter.api.Test;

class URLUtilToURLTest {

    @Test
    void testValidHttpUrl() throws MalformedURLException {
        URL url = URLUtil.toURL("http://example.com/path?query=value");
        assertNotNull(url);
        assertEquals("http", url.getProtocol());
        assertEquals("example.com", url.getHost());
        assertEquals("/path", url.getPath());
        assertEquals("query=value", url.getQuery());
    }

    @Test
    void testValidHttpsUrl() throws MalformedURLException {
        URL url = URLUtil.toURL("https://example.com/");
        assertNotNull(url);
        assertEquals("https", url.getProtocol());
    }

    @Test
    void testUrlWithPort() throws MalformedURLException {
        URL url = URLUtil.toURL("http://example.com:8080/path");
        assertEquals(8080, url.getPort());
    }

    @Test
    void testUrlWithFragment() throws MalformedURLException {
        URL url = URLUtil.toURL("http://example.com/page#section");
        assertNotNull(url);
        assertEquals("section", url.getRef());
    }

    @Test
    void testPipeFallbackSanitization() throws MalformedURLException {
        // Pipe is illegal in URIs per RFC 3986 but toURL sanitizes on fallback
        URL url = URLUtil.toURL("http://example.com?q=a|b");
        assertNotNull(url);
        assertEquals("example.com", url.getHost());
    }

    @Test
    void testBackslashFallbackSanitization() throws MalformedURLException {
        // Backslash is illegal in URIs but toURL sanitizes on fallback
        URL url = URLUtil.toURL("http://example.com/\\path");
        assertNotNull(url);
    }

    @Test
    void testSpaceFallbackSanitization() throws MalformedURLException {
        // Unencoded space is illegal in URIs but toURL sanitizes on fallback
        URL url = URLUtil.toURL("http://example.com/my path");
        assertNotNull(url);
    }

    @Test
    void testCompletelyInvalidString() {
        assertThrows(MalformedURLException.class, () -> URLUtil.toURL("not a url at all"));
    }

    @Test
    void testNonAbsoluteUri() {
        assertThrows(MalformedURLException.class, () -> URLUtil.toURL("just/a/path"));
    }

    @Test
    void testEncodedCharactersPreserved() throws MalformedURLException {
        URL url = URLUtil.toURL("http://example.com/path%20with%20spaces");
        assertNotNull(url);
        assertEquals("/path%20with%20spaces", url.getPath());
    }

    @Test
    void testUrlWithUserInfo() throws MalformedURLException {
        URL url = URLUtil.toURL("http://user:pass@example.com/path");
        assertNotNull(url);
        assertEquals("user:pass", url.getUserInfo());
    }

    @Test
    void testCauseIsPreserved() {
        MalformedURLException ex =
                assertThrows(MalformedURLException.class, () -> URLUtil.toURL("not a url at all"));
        assertNotNull(ex.getCause());
    }
}
