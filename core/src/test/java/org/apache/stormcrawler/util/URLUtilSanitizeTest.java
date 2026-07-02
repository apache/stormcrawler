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

import org.junit.jupiter.api.Test;

class URLUtilSanitizeTest {

    @Test
    void testSanitizePipe() {
        String input = "http://example.com?q=a|b";
        String result = URLUtil.sanitizeForURI(input);
        assertEquals("http://example.com?q=a%7Cb", result);
    }

    @Test
    void testSanitizeBackslash() {
        String input = "http://example.com/\\path\\file.pdf";
        String result = URLUtil.sanitizeForURI(input);
        assertEquals("http://example.com/%5Cpath%5Cfile.pdf", result);
    }

    @Test
    void testSanitizeSpace() {
        String input = "http://example.com/my path/file name.html";
        String result = URLUtil.sanitizeForURI(input);
        assertEquals("http://example.com/my%20path/file%20name.html", result);
    }

    @Test
    void testSanitizeCurlyBraces() {
        String input = "http://example.com/{id}/page";
        String result = URLUtil.sanitizeForURI(input);
        assertEquals("http://example.com/%7Bid%7D/page", result);
    }

    @Test
    void testSanitizeNonStandardPercentEncoding() {
        // %u011f is non-standard encoding for ğ (U+011F)
        // Should be converted to UTF-8 percent encoding: %C4%9F
        String input = "http://example.com/page?s=ni%u011fde";
        String result = URLUtil.sanitizeForURI(input);
        assertEquals("http://example.com/page?s=ni%C4%9Fde", result);
    }

    @Test
    void testSanitizeNonStandardPercentEncodingMultiple() {
        String input = "http://example.com/%u0041%u0042";
        String result = URLUtil.sanitizeForURI(input);
        // U+0041 = 'A' → %41, U+0042 = 'B' → %42
        assertEquals("http://example.com/%41%42", result);
    }

    @Test
    void testSanitizeValidUrlUnchanged() {
        String input = "http://example.com/path?query=value&other=123";
        String result = URLUtil.sanitizeForURI(input);
        assertEquals(input, result);
    }

    @Test
    void testSanitizeAlreadyEncodedUnchanged() {
        String input = "http://example.com/path%20with%7Cpipe";
        String result = URLUtil.sanitizeForURI(input);
        assertEquals(input, result);
    }

    @Test
    void testSanitizeMultiplePipes() {
        String input = "http://example.com?q=top-651451||1|60|1|2||||";
        String result = URLUtil.sanitizeForURI(input);
        assertEquals("http://example.com?q=top-651451%7C%7C1%7C60%7C1%7C2%7C%7C%7C%7C", result);
    }

    @Test
    void testSanitizeMixedIllegalChars() {
        String input = "http://example.com/path with|pipe";
        String result = URLUtil.sanitizeForURI(input);
        assertEquals("http://example.com/path%20with%7Cpipe", result);
    }
}
