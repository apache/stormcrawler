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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Host aliases collapse to one url; different urls stay different. */
class URLUtilNormaliseHostTest {

    @Test
    void percentEscapedHostIsDecoded() {
        Assertions.assertEquals(
                "http://example.org/a?x=1", URLUtil.normaliseHost("http://exampl%65.org/a?x=1"));
        Assertions.assertEquals(
                "http://example.org/a?x=1", URLUtil.normaliseHost("http://%65xample.org/a?x=1"));
    }

    @Test
    void caseAndTrailingDotAreNormalised() {
        Assertions.assertEquals(
                "http://example.org/p", URLUtil.normaliseHost("http://EXAMPLE.org/p"));
        Assertions.assertEquals(
                "http://example.org/p", URLUtil.normaliseHost("http://example.org./p"));
    }

    /** The userinfo and the port are kept, only the host is replaced. */
    @Test
    void userInfoAndPortPreserved() {
        Assertions.assertEquals(
                "http://user:pw@example.org:8080/x",
                URLUtil.normaliseHost("http://user:pw@Exampl%65.org:8080/x"));
    }

    /** Already-canonical, hostless, IPv6 and non-hierarchical urls are unchanged. */
    @Test
    void leavesOtherUrlsUnchanged() {
        Assertions.assertEquals(
                "http://example.org/a", URLUtil.normaliseHost("http://example.org/a"));
        Assertions.assertEquals("file:///etc/passwd", URLUtil.normaliseHost("file:///etc/passwd"));
        Assertions.assertEquals("http://[::1]/x", URLUtil.normaliseHost("http://[::1]/x"));
        Assertions.assertEquals("mailto:a@b.c", URLUtil.normaliseHost("mailto:a@b.c"));
        Assertions.assertEquals("not a url", URLUtil.normaliseHost("not a url"));
    }

    /** A decoded escape which would corrupt the authority leaves the url alone. */
    @Test
    void escapesToAuthorityDelimitersAreIgnored() {
        // %3A decodes to a colon, which must not end up inside the host
        Assertions.assertEquals(
                "http://ex%3Aample.org/", URLUtil.normaliseHost("http://ex%3Aample.org/"));
    }

    @Test
    void normalisationIsIdempotent() {
        String once = URLUtil.normaliseHost("http://exampl%65.org./p");
        Assertions.assertEquals(once, URLUtil.normaliseHost(once));
    }

    /** A multi-byte escape decodes as UTF-8, not one character per octet. */
    @Test
    void multiByteEscapeDecodesAsUtf8() {
        // %C3%BC is the UTF-8 encoding of u-umlaut
        Assertions.assertEquals(
                "http://ünicode.example.org/",
                URLUtil.normaliseHost("http://%C3%BCnicode.example.org/"));
    }
}
