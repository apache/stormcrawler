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
import java.net.URL;
import java.util.List;
import org.apache.http.cookie.Cookie;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class CookieConverterTest {

    private static String securedUrl = "https://someurl.com";

    private static String unsecuredUrl = "http://someurl.com";

    private static String dummyCookieHeader = "nice tasty test cookie header!";

    private static String dummyCookieValue = "nice tasty test cookie value!";

    @Test
    void testSimpleCookieAndUrl() {
        String[] cookiesStrings = new String[1];
        String dummyCookieString =
                buildCookieString(dummyCookieHeader, dummyCookieValue, null, null, null, null);
        cookiesStrings[0] = dummyCookieString;
        List<Cookie> result = CookieConverter.getCookies(cookiesStrings, getUrl(unsecuredUrl));
        Assertions.assertEquals(1, result.size(), "Should have 1 cookie");
        Assertions.assertEquals(
                dummyCookieHeader, result.get(0).getName(), "Cookie header should be as defined");
        Assertions.assertEquals(
                dummyCookieValue, result.get(0).getValue(), "Cookie value should be as defined");
    }

    @Test
    void testNotExpiredCookie() {
        String[] cookiesStrings = new String[1];
        String dummyCookieString =
                buildCookieString(
                        dummyCookieHeader,
                        dummyCookieValue,
                        null,
                        "Tue, 11 Apr 2117 07:13:39 -0000",
                        null,
                        null);
        cookiesStrings[0] = dummyCookieString;
        List<Cookie> result = CookieConverter.getCookies(cookiesStrings, getUrl(unsecuredUrl));
        Assertions.assertEquals(1, result.size(), "Should have 1 cookie");
        Assertions.assertEquals(
                dummyCookieHeader, result.get(0).getName(), "Cookie header should be as defined");
        Assertions.assertEquals(
                dummyCookieValue, result.get(0).getValue(), "Cookie value should be as defined");
    }

    @Test
    void testExpiredCookie() {
        String[] cookiesStrings = new String[1];
        String dummyCookieString =
                buildCookieString(
                        dummyCookieHeader,
                        dummyCookieValue,
                        null,
                        "Tue, 11 Apr 2016 07:13:39 -0000",
                        null,
                        null);
        cookiesStrings[0] = dummyCookieString;
        List<Cookie> result = CookieConverter.getCookies(cookiesStrings, getUrl(unsecuredUrl));
        Assertions.assertEquals(
                0, result.size(), "Should have 0 cookies, since cookie was expired");
    }

    @Test
    void testValidPath() {
        String[] cookiesStrings = new String[1];
        String dummyCookieString =
                buildCookieString(dummyCookieHeader, dummyCookieValue, null, null, "/", null);
        cookiesStrings[0] = dummyCookieString;
        List<Cookie> result =
                CookieConverter.getCookies(cookiesStrings, getUrl(unsecuredUrl + "/somepage"));
        Assertions.assertEquals(1, result.size(), "Should have 1 cookie");
        Assertions.assertEquals(
                dummyCookieHeader, result.get(0).getName(), "Cookie header should be as defined");
        Assertions.assertEquals(
                dummyCookieValue, result.get(0).getValue(), "Cookie value should be as defined");
    }

    @Test
    void testValidPath2() {
        String[] cookiesStrings = new String[1];
        String dummyCookieString =
                buildCookieString(dummyCookieHeader, dummyCookieValue, null, null, "/", null);
        cookiesStrings[0] = dummyCookieString;
        List<Cookie> result = CookieConverter.getCookies(cookiesStrings, getUrl(unsecuredUrl));
        Assertions.assertEquals(1, result.size(), "Should have 1 cookie");
        Assertions.assertEquals(
                dummyCookieHeader, result.get(0).getName(), "Cookie header should be as defined");
        Assertions.assertEquals(
                dummyCookieValue, result.get(0).getValue(), "Cookie value should be as defined");
    }

    @Test
    void testValidPath3() {
        String[] cookiesStrings = new String[1];
        String dummyCookieString =
                buildCookieString(
                        dummyCookieHeader, dummyCookieValue, null, null, "/someFolder", null);
        cookiesStrings[0] = dummyCookieString;
        List<Cookie> result =
                CookieConverter.getCookies(cookiesStrings, getUrl(unsecuredUrl + "/someFolder"));
        Assertions.assertEquals(1, result.size(), "Should have 1 cookie");
        Assertions.assertEquals(
                dummyCookieHeader, result.get(0).getName(), "Cookie header should be as defined");
        Assertions.assertEquals(
                dummyCookieValue, result.get(0).getValue(), "Cookie value should be as defined");
    }

    @Test
    void testValidPath4() {
        String[] cookiesStrings = new String[1];
        String dummyCookieString =
                buildCookieString(
                        dummyCookieHeader, dummyCookieValue, null, null, "/someFolder", null);
        cookiesStrings[0] = dummyCookieString;
        List<Cookie> result =
                CookieConverter.getCookies(
                        cookiesStrings, getUrl(unsecuredUrl + "/someFolder/SomeOtherFolder"));
        Assertions.assertEquals(1, result.size(), "Should have 1 cookie");
        Assertions.assertEquals(
                dummyCookieHeader, result.get(0).getName(), "Cookie header should be as defined");
        Assertions.assertEquals(
                dummyCookieValue, result.get(0).getValue(), "Cookie value should be as defined");
    }

    @Test
    void testInvalidPath() {
        String[] cookiesStrings = new String[1];
        String dummyCookieString =
                buildCookieString(
                        dummyCookieHeader, dummyCookieValue, null, null, "/someFolder", null);
        cookiesStrings[0] = dummyCookieString;
        List<Cookie> result =
                CookieConverter.getCookies(
                        cookiesStrings, getUrl(unsecuredUrl + "/someOtherFolder/SomeFolder"));
        Assertions.assertEquals(0, result.size(), "path mismatch, should have 0 cookies");
    }

    @Test
    void testValidDomain() {
        String[] cookiesStrings = new String[1];
        String dummyCookieString =
                buildCookieString(
                        dummyCookieHeader, dummyCookieValue, "someurl.com", null, null, null);
        cookiesStrings[0] = dummyCookieString;
        List<Cookie> result =
                CookieConverter.getCookies(
                        cookiesStrings, getUrl(unsecuredUrl + "/someFolder/SomeOtherFolder"));
        Assertions.assertEquals(1, result.size(), "Should have 1 cookie");
        Assertions.assertEquals(
                dummyCookieHeader, result.get(0).getName(), "Cookie header should be as defined");
        Assertions.assertEquals(
                dummyCookieValue, result.get(0).getValue(), "Cookie value should be as defined");
    }

    @Test
    void testInvalidDomain() {
        String[] cookiesStrings = new String[1];
        String dummyCookieString =
                buildCookieString(
                        dummyCookieHeader, dummyCookieValue, "someOtherUrl.com", null, null, null);
        cookiesStrings[0] = dummyCookieString;
        List<Cookie> result =
                CookieConverter.getCookies(
                        cookiesStrings, getUrl(unsecuredUrl + "/someFolder/SomeOtherFolder"));
        Assertions.assertEquals(0, result.size(), "Domain is not valid - Should have 0 cookies");
    }

    @Test
    void testSecurFlagHttp() {
        String[] cookiesStrings = new String[1];
        String dummyCookieString =
                buildCookieString(
                        dummyCookieHeader, dummyCookieValue, null, null, null, Boolean.TRUE);
        cookiesStrings[0] = dummyCookieString;
        List<Cookie> result =
                CookieConverter.getCookies(
                        cookiesStrings, getUrl(unsecuredUrl + "/someFolder/SomeOtherFolder"));
        Assertions.assertEquals(
                0, result.size(), "Target url is not secured - Should have 0 cookies");
    }

    @Test
    void testSecurFlagHttpS() {
        String[] cookiesStrings = new String[1];
        String dummyCookieString =
                buildCookieString(
                        dummyCookieHeader, dummyCookieValue, null, null, null, Boolean.TRUE);
        cookiesStrings[0] = dummyCookieString;
        List<Cookie> result =
                CookieConverter.getCookies(
                        cookiesStrings, getUrl(securedUrl + "/someFolder/SomeOtherFolder"));
        Assertions.assertEquals(1, result.size(), "Target url is  secured - Should have 1 cookie");
        Assertions.assertEquals(
                dummyCookieHeader, result.get(0).getName(), "Cookie header should be as defined");
        Assertions.assertEquals(
                dummyCookieValue, result.get(0).getValue(), "Cookie value should be as defined");
    }

    @Test
    void testFullCookie() {
        String[] cookiesStrings = new String[1];
        String dummyCookieString =
                buildCookieString(
                        dummyCookieHeader,
                        dummyCookieValue,
                        "someurl.com",
                        "Tue, 11 Apr 2117 07:13:39 -0000",
                        "/",
                        true);
        cookiesStrings[0] = dummyCookieString;
        List<Cookie> result =
                CookieConverter.getCookies(
                        cookiesStrings, getUrl(securedUrl + "/someFolder/SomeOtherFolder"));
        Assertions.assertEquals(1, result.size(), "Should have 1 cookie");
        Assertions.assertEquals(
                dummyCookieHeader, result.get(0).getName(), "Cookie header should be as defined");
        Assertions.assertEquals(
                dummyCookieValue, result.get(0).getValue(), "Cookie value should be as defined");
    }

    @Test
    void test2Cookies() {
        String[] cookiesStrings = new String[2];
        String dummyCookieString =
                buildCookieString(
                        dummyCookieHeader,
                        dummyCookieValue,
                        "someurl.com",
                        "Tue, 11 Apr 2117 07:13:39 -0000",
                        "/",
                        true);
        String dummyCookieString2 =
                buildCookieString(
                        dummyCookieHeader + "2",
                        dummyCookieValue + "2",
                        "someurl.com",
                        "Tue, 11 Apr 2117 07:13:39 -0000",
                        "/",
                        true);
        cookiesStrings[0] = dummyCookieString;
        cookiesStrings[1] = dummyCookieString2;
        List<Cookie> result =
                CookieConverter.getCookies(
                        cookiesStrings, getUrl(securedUrl + "/someFolder/SomeOtherFolder"));
        Assertions.assertEquals(2, result.size(), "Should have 2 cookies");
        Assertions.assertEquals(
                dummyCookieHeader, result.get(0).getName(), "Cookie header should be as defined");
        Assertions.assertEquals(
                dummyCookieValue, result.get(0).getValue(), "Cookie value should be as defined");
        Assertions.assertEquals(
                dummyCookieHeader + "2",
                result.get(1).getName(),
                "Cookie header should be as defined");
        Assertions.assertEquals(
                dummyCookieValue + "2",
                result.get(1).getValue(),
                "Cookie value should be as defined");
    }

    @Test
    void testDomainsChecker() {
        boolean result = CookieConverter.checkDomainMatchToUrl(".example.com", "www.example.com");
        Assertions.assertTrue(result, "domain is valid");
    }

    @Test
    void testDomainsChecker2() {
        boolean result = CookieConverter.checkDomainMatchToUrl(".example.com", "example.com");
        Assertions.assertTrue(result, "domain is valid");
    }

    @Test
    void testDomainsChecker3() {
        boolean result = CookieConverter.checkDomainMatchToUrl("example.com", "www.example.com");
        Assertions.assertTrue(result, "domain is valid");
    }

    @Test
    void testDomainsChecker4() {
        boolean result = CookieConverter.checkDomainMatchToUrl("example.com", "anotherexample.com");
        Assertions.assertFalse(result, "domain is not valid");
    }

    private URL getUrl(String urlString) {
        try {
            return URLUtil.toURL(urlString);
        } catch (MalformedURLException e) {
            return null;
        }
    }

    private String buildCookieString(
            String header,
            String value,
            String domain,
            String expires,
            String path,
            Boolean secure) {
        StringBuilder builder = new StringBuilder(buildCookiePart(header, value));
        if (domain != null) {
            builder.append(buildCookiePart("domain", domain));
        }
        if (expires != null) {
            builder.append(buildCookiePart("expires", expires));
        }
        if (path != null) {
            builder.append(buildCookiePart("path", path));
        }
        if (secure != null) {
            builder.append("secure;");
        }
        return builder.toString();
    }

    private String buildCookiePart(String partName, String partValue) {
        return partName + "=" + partValue + ";";
    }
}
