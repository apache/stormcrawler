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
        List<Cookie> result =
                CookieConverter.getCookies(
                        cookiesStrings, getUrl(unsecuredUrl), getUrl(unsecuredUrl));
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
        List<Cookie> result =
                CookieConverter.getCookies(
                        cookiesStrings, getUrl(unsecuredUrl), getUrl(unsecuredUrl));
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
        List<Cookie> result =
                CookieConverter.getCookies(
                        cookiesStrings, getUrl(unsecuredUrl), getUrl(unsecuredUrl));
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
                CookieConverter.getCookies(
                        cookiesStrings, getUrl(unsecuredUrl), getUrl(unsecuredUrl + "/somepage"));
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
        List<Cookie> result =
                CookieConverter.getCookies(
                        cookiesStrings, getUrl(unsecuredUrl), getUrl(unsecuredUrl));
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
                CookieConverter.getCookies(
                        cookiesStrings, getUrl(unsecuredUrl), getUrl(unsecuredUrl + "/someFolder"));
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
                        cookiesStrings,
                        getUrl(unsecuredUrl),
                        getUrl(unsecuredUrl + "/someFolder/SomeOtherFolder"));
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
                        cookiesStrings,
                        getUrl(unsecuredUrl),
                        getUrl(unsecuredUrl + "/someOtherFolder/SomeFolder"));
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
                        cookiesStrings,
                        getUrl(unsecuredUrl),
                        getUrl(unsecuredUrl + "/someFolder/SomeOtherFolder"));
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
                        cookiesStrings,
                        getUrl(unsecuredUrl),
                        getUrl(unsecuredUrl + "/someFolder/SomeOtherFolder"));
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
                        cookiesStrings,
                        getUrl(unsecuredUrl),
                        getUrl(unsecuredUrl + "/someFolder/SomeOtherFolder"));
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
                        cookiesStrings,
                        getUrl(unsecuredUrl),
                        getUrl(securedUrl + "/someFolder/SomeOtherFolder"));
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
                        cookiesStrings,
                        getUrl(unsecuredUrl),
                        getUrl(securedUrl + "/someFolder/SomeOtherFolder"));
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
                        cookiesStrings,
                        getUrl(unsecuredUrl),
                        getUrl(securedUrl + "/someFolder/SomeOtherFolder"));
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
    void cookieWithoutDomainIsSentToSameHost() {
        String[] cookiesStrings = new String[1];
        cookiesStrings[0] =
                buildCookieString(dummyCookieHeader, dummyCookieValue, null, null, null, null);
        List<Cookie> result =
                CookieConverter.getCookies(
                        cookiesStrings, getUrl(unsecuredUrl), getUrl(unsecuredUrl + "/somepage"));
        Assertions.assertEquals(1, result.size(), "Should have 1 cookie");
    }

    @Test
    void cookieWithoutDomainIsNotSentToOtherHost() {
        String[] cookiesStrings = new String[1];
        cookiesStrings[0] =
                buildCookieString(dummyCookieHeader, dummyCookieValue, null, null, null, null);
        List<Cookie> result =
                CookieConverter.getCookies(
                        cookiesStrings, getUrl(unsecuredUrl), getUrl("http://someotherurl.com"));
        Assertions.assertEquals(
                0, result.size(), "Cookie without domain is bound to the host which set it");
    }

    @Test
    void cookieWithoutDomainIsNotSentWhenOriginIsUnknown() {
        String[] cookiesStrings = new String[1];
        cookiesStrings[0] =
                buildCookieString(dummyCookieHeader, dummyCookieValue, null, null, null, null);
        List<Cookie> result =
                CookieConverter.getCookies(cookiesStrings, null, getUrl(unsecuredUrl));
        Assertions.assertEquals(
                0, result.size(), "Cookie without domain needs the host which set it");
    }

    @Test
    void cookieWithSingleLabelDomainIsNotSent() {
        String[] cookiesStrings = new String[1];
        cookiesStrings[0] =
                buildCookieString(dummyCookieHeader, dummyCookieValue, "com", null, null, null);
        List<Cookie> result =
                CookieConverter.getCookies(
                        cookiesStrings, getUrl(unsecuredUrl), getUrl(unsecuredUrl));
        Assertions.assertEquals(0, result.size(), "Domain com must not match someurl.com");
    }

    @Test
    void cookieWithDomainIsNotSentWhenOriginIsOutsideTheDomain() {
        String[] cookiesStrings = new String[1];
        cookiesStrings[0] =
                buildCookieString(
                        dummyCookieHeader, dummyCookieValue, "someurl.com", null, null, null);
        List<Cookie> result =
                CookieConverter.getCookies(
                        cookiesStrings,
                        getUrl("http://someotherurl.com"),
                        getUrl(unsecuredUrl + "/somepage"));
        Assertions.assertEquals(
                0, result.size(), "Domain does not cover the host which set the cookie");
    }

    @Test
    void cookieWithConsecutiveDotsInDomainIsNotSent() {
        String[] cookiesStrings = new String[1];
        cookiesStrings[0] =
                buildCookieString(dummyCookieHeader, dummyCookieValue, "com..", null, null, null);
        Assertions.assertEquals(
                0,
                CookieConverter.getCookies(
                                cookiesStrings,
                                getUrl("http://evil.com"),
                                getUrl("http://victim.com"))
                        .size(),
                "Domain com.. must not match every host under com");
        Assertions.assertEquals(
                0,
                CookieConverter.getCookies(
                                cookiesStrings, getUrl(unsecuredUrl), getUrl(unsecuredUrl))
                        .size(),
                "Domain com.. must not match someurl.com");
    }

    @Test
    void cookieWithTrailingDotsInDomainIsNotSent() {
        for (String domain : new String[] {"com...", ".com.."}) {
            String[] cookiesStrings = new String[1];
            cookiesStrings[0] =
                    buildCookieString(
                            dummyCookieHeader, dummyCookieValue, domain, null, null, null);
            Assertions.assertEquals(
                    0,
                    CookieConverter.getCookies(
                                    cookiesStrings, getUrl(unsecuredUrl), getUrl(unsecuredUrl))
                            .size(),
                    "Domain " + domain + " is malformed");
        }
    }

    @Test
    void cookieWithEmptyLabelInDomainIsNotSent() {
        String[] cookiesStrings = new String[1];
        cookiesStrings[0] =
                buildCookieString(
                        dummyCookieHeader, dummyCookieValue, "some..url.com", null, null, null);
        List<Cookie> result =
                CookieConverter.getCookies(
                        cookiesStrings, getUrl(unsecuredUrl), getUrl(unsecuredUrl));
        Assertions.assertEquals(0, result.size(), "Domain with an empty label is malformed");
    }

    @Test
    void cookieWithLeadingDoubleDotDomainIsNotSent() {
        String[] cookiesStrings = new String[1];
        cookiesStrings[0] =
                buildCookieString(
                        dummyCookieHeader, dummyCookieValue, "..someurl.com", null, null, null);
        List<Cookie> result =
                CookieConverter.getCookies(
                        cookiesStrings, getUrl(unsecuredUrl), getUrl(unsecuredUrl));
        Assertions.assertEquals(0, result.size(), "Only one leading dot is allowed");
    }

    @Test
    void cookieWithTrailingRootDotDomainIsSent() {
        String[] cookiesStrings = new String[1];
        cookiesStrings[0] =
                buildCookieString(
                        dummyCookieHeader, dummyCookieValue, "someurl.com.", null, null, null);
        List<Cookie> result =
                CookieConverter.getCookies(
                        cookiesStrings, getUrl(unsecuredUrl), getUrl(unsecuredUrl));
        Assertions.assertEquals(1, result.size(), "The root label is not part of the domain");
    }

    @Test
    void cookieWithInvalidCharactersInDomainIsNotSent() {
        for (String domain :
                new String[] {"some_url.com", "-someurl.com", "someurl-.com", "\"someurl.com\""}) {
            String[] cookiesStrings = new String[1];
            cookiesStrings[0] =
                    buildCookieString(
                            dummyCookieHeader, dummyCookieValue, domain, null, null, null);
            Assertions.assertEquals(
                    0,
                    CookieConverter.getCookies(
                                    cookiesStrings, getUrl(unsecuredUrl), getUrl(unsecuredUrl))
                            .size(),
                    "Domain " + domain + " is not a valid domain name");
        }
    }

    @Test
    void cookieWithOverlongLabelInDomainIsNotSent() {
        String label = "a".repeat(64);
        String[] cookiesStrings = new String[1];
        cookiesStrings[0] =
                buildCookieString(
                        dummyCookieHeader, dummyCookieValue, label + ".com", null, null, null);
        Assertions.assertEquals(
                0,
                CookieConverter.getCookies(
                                cookiesStrings,
                                getUrl("http://" + label + ".com"),
                                getUrl("http://" + label + ".com"))
                        .size(),
                "A label of more than 63 characters is not valid");

        String validLabel = "a".repeat(63);
        cookiesStrings[0] =
                buildCookieString(
                        dummyCookieHeader, dummyCookieValue, validLabel + ".com", null, null, null);
        Assertions.assertEquals(
                1,
                CookieConverter.getCookies(
                                cookiesStrings,
                                getUrl("http://" + validLabel + ".com"),
                                getUrl("http://" + validLabel + ".com"))
                        .size(),
                "A label of 63 characters is valid");
    }

    @Test
    void cookieWithEmptyDomainIsTreatedAsHostOnly() {
        String[] cookiesStrings = new String[1];
        cookiesStrings[0] =
                buildCookieString(dummyCookieHeader, dummyCookieValue, "", null, null, null);
        Assertions.assertEquals(
                1,
                CookieConverter.getCookies(
                                cookiesStrings, getUrl(unsecuredUrl), getUrl(unsecuredUrl))
                        .size(),
                "An empty domain attribute binds the cookie to the host which set it");
        Assertions.assertEquals(
                0,
                CookieConverter.getCookies(
                                cookiesStrings,
                                getUrl(unsecuredUrl),
                                getUrl("http://someotherurl.com"))
                        .size(),
                "An empty domain attribute does not cover another host");
    }

    @Test
    void cookieWithPublicSuffixDomainIsNotSent() {
        String[] cookiesStrings = new String[1];
        cookiesStrings[0] =
                buildCookieString(dummyCookieHeader, dummyCookieValue, "co.uk", null, null, null);
        List<Cookie> result =
                CookieConverter.getCookies(
                        cookiesStrings, getUrl("http://evil.co.uk"), getUrl("http://bank.co.uk"));
        Assertions.assertEquals(0, result.size(), "A public suffix is not usable as a scope");
    }

    @Test
    void cookieWithUpperCasePublicSuffixDomainIsNotSent() {
        String[] cookiesStrings = new String[1];
        cookiesStrings[0] =
                buildCookieString(dummyCookieHeader, dummyCookieValue, "CO.UK", null, null, null);
        List<Cookie> result =
                CookieConverter.getCookies(
                        cookiesStrings, getUrl("http://evil.co.uk"), getUrl("http://bank.co.uk"));
        Assertions.assertEquals(
                0, result.size(), "The public suffix lookup must not depend on the case");
    }

    @Test
    void cookieWithMultiLabelPublicSuffixDomainIsNotSent() {
        String[] cookiesStrings = new String[1];
        cookiesStrings[0] =
                buildCookieString(
                        dummyCookieHeader, dummyCookieValue, "pvt.k12.ma.us", null, null, null);
        List<Cookie> result =
                CookieConverter.getCookies(
                        cookiesStrings,
                        getUrl("http://evil.pvt.k12.ma.us"),
                        getUrl("http://victim.pvt.k12.ma.us"));
        Assertions.assertEquals(
                0, result.size(), "A multi label public suffix is not usable as a scope");
    }

    @Test
    void cookieWithPrivateSectionSuffixDomainIsNotSent() {
        String[] cookiesStrings = new String[1];
        cookiesStrings[0] =
                buildCookieString(
                        dummyCookieHeader, dummyCookieValue, "github.io", null, null, null);
        List<Cookie> result =
                CookieConverter.getCookies(
                        cookiesStrings,
                        getUrl("http://evil.github.io"),
                        getUrl("http://victim.github.io"));
        Assertions.assertEquals(
                0, result.size(), "A suffix of the private section is not usable as a scope");
    }

    @Test
    void cookieWithWildcardPublicSuffixDomainIsNotSent() {
        String[] cookiesStrings = new String[1];
        cookiesStrings[0] =
                buildCookieString(dummyCookieHeader, dummyCookieValue, "foo.ck", null, null, null);
        List<Cookie> result =
                CookieConverter.getCookies(
                        cookiesStrings, getUrl("http://a.foo.ck"), getUrl("http://b.foo.ck"));
        Assertions.assertEquals(
                0, result.size(), "A suffix matched by a wildcard rule is not usable as a scope");
    }

    @Test
    void cookieBelowPublicSuffixIsSent() {
        String[] cookiesStrings = new String[1];
        cookiesStrings[0] =
                buildCookieString(
                        dummyCookieHeader, dummyCookieValue, "example.co.uk", null, null, null);
        List<Cookie> result =
                CookieConverter.getCookies(
                        cookiesStrings,
                        getUrl("http://www.example.co.uk"),
                        getUrl("http://shop.example.co.uk"));
        Assertions.assertEquals(
                1, result.size(), "A domain below a public suffix covers its hosts");
    }

    @Test
    void cookieBelowPrivateSectionSuffixIsSent() {
        String[] cookiesStrings = new String[1];
        cookiesStrings[0] =
                buildCookieString(
                        dummyCookieHeader, dummyCookieValue, "user.github.io", null, null, null);
        List<Cookie> result =
                CookieConverter.getCookies(
                        cookiesStrings,
                        getUrl("http://www.user.github.io"),
                        getUrl("http://blog.user.github.io"));
        Assertions.assertEquals(
                1, result.size(), "A domain below a private suffix covers its hosts");
    }

    @Test
    void cookieWithUnlistedInternalDomainIsSent() {
        String[] cookiesStrings = new String[1];
        cookiesStrings[0] =
                buildCookieString(
                        dummyCookieHeader, dummyCookieValue, "intranet.local", null, null, null);
        List<Cookie> result =
                CookieConverter.getCookies(
                        cookiesStrings,
                        getUrl("http://host.intranet.local"),
                        getUrl("http://other.intranet.local"));
        Assertions.assertEquals(
                1, result.size(), "A domain unknown to the public suffix list keeps working");
    }

    @Test
    void cookieWithUnicodeDomainIsSent() {
        String[] cookiesStrings = new String[1];
        cookiesStrings[0] =
                buildCookieString(
                        dummyCookieHeader, dummyCookieValue, "münchen.de", null, null, null);
        List<Cookie> result =
                CookieConverter.getCookies(
                        cookiesStrings,
                        getUrl("http://münchen.de"),
                        getUrl("http://www.münchen.de"));
        Assertions.assertEquals(
                1, result.size(), "A domain attribute written in unicode keeps scoping its cookie");
    }

    @Test
    void cookieWithUnicodeDomainIsSentToThePunycodeHost() {
        String[] cookiesStrings = new String[1];
        cookiesStrings[0] =
                buildCookieString(
                        dummyCookieHeader, dummyCookieValue, "münchen.de", null, null, null);
        List<Cookie> result =
                CookieConverter.getCookies(
                        cookiesStrings,
                        getUrl("http://xn--mnchen-3ya.de"),
                        getUrl("http://www.xn--mnchen-3ya.de"));
        Assertions.assertEquals(
                1, result.size(), "The unicode and punycode forms are interchangeable");
    }

    @Test
    void cookieWithAddressDomainIsNotSentToAnotherAddress() {
        String[] cookiesStrings = new String[1];
        cookiesStrings[0] =
                buildCookieString(dummyCookieHeader, dummyCookieValue, "2.3.4", null, null, null);
        List<Cookie> result =
                CookieConverter.getCookies(
                        cookiesStrings, getUrl("http://1.2.3.4"), getUrl("http://9.2.3.4"));
        Assertions.assertEquals(0, result.size(), "An address does not cover anything below it");
    }

    @Test
    void cookieWithAddressDomainIsSentToThatAddress() {
        String[] cookiesStrings = new String[1];
        cookiesStrings[0] =
                buildCookieString(dummyCookieHeader, dummyCookieValue, "1.2.3.4", null, null, null);
        List<Cookie> result =
                CookieConverter.getCookies(
                        cookiesStrings, getUrl("http://1.2.3.4"), getUrl("http://1.2.3.4"));
        Assertions.assertEquals(1, result.size(), "An address is matched by itself");
    }

    @Test
    void cookieHeaderWithoutANameIsSkipped() {
        String[] cookiesStrings = new String[] {"novalue", "=novalue", "sid=ok"};
        List<Cookie> result =
                CookieConverter.getCookies(
                        cookiesStrings, getUrl(unsecuredUrl), getUrl(unsecuredUrl));
        Assertions.assertEquals(1, result.size(), "Only the usable cookie is kept");
        Assertions.assertEquals("sid", result.get(0).getName());
    }

    @Test
    void cookieWithUnlistedInternalDomainIsSent2() {
        String[] cookiesStrings = new String[1];
        cookiesStrings[0] =
                buildCookieString(
                        dummyCookieHeader, dummyCookieValue, "host.internal", null, null, null);
        List<Cookie> result =
                CookieConverter.getCookies(
                        cookiesStrings,
                        getUrl("http://a.host.internal"),
                        getUrl("http://b.host.internal"));
        Assertions.assertEquals(
                1, result.size(), "A domain unknown to the public suffix list keeps working");
    }

    @Test
    void cookieWithUnlistedDomainIsNotSentToLookalikeHost() {
        String[] cookiesStrings = new String[1];
        cookiesStrings[0] =
                buildCookieString(
                        dummyCookieHeader, dummyCookieValue, "intranet.local", null, null, null);
        List<Cookie> result =
                CookieConverter.getCookies(
                        cookiesStrings,
                        getUrl("http://host.intranet.local"),
                        getUrl("http://intranet.local.evil.com"));
        Assertions.assertEquals(
                0, result.size(), "The domain must be a suffix of the host, not a prefix");
    }

    @Test
    void cookieWithSingleLabelIntranetDomainIsSentToThatHostOnly() {
        String[] cookiesStrings = new String[1];
        cookiesStrings[0] =
                buildCookieString(
                        dummyCookieHeader, dummyCookieValue, "intranet", null, null, null);
        Assertions.assertEquals(
                1,
                CookieConverter.getCookies(
                                cookiesStrings,
                                getUrl("http://intranet"),
                                getUrl("http://intranet/somepage"))
                        .size(),
                "A single label domain binds the cookie to that host");
        Assertions.assertEquals(
                0,
                CookieConverter.getCookies(
                                cookiesStrings,
                                getUrl("http://intranet"),
                                getUrl("http://other.intranet"))
                        .size(),
                "A single label domain does not cover its subdomains");
    }

    @Test
    void domainsCheckerRejectsEmptyLabels() {
        Assertions.assertFalse(
                CookieConverter.checkDomainMatchToUrl("com..", "evil.com"),
                "an empty label must not be dropped");
        Assertions.assertFalse(
                CookieConverter.checkDomainMatchToUrl("..example.com", "www.example.com"),
                "an empty label must not be dropped");
    }

    @Test
    void domainsCheckerRejectsNullDomain() {
        Assertions.assertFalse(
                CookieConverter.checkDomainMatchToUrl(null, "example.com"),
                "domain can not be checked");
    }

    @Test
    void domainsCheckerIgnoresTheRootLabelOfTheHost() {
        Assertions.assertTrue(
                CookieConverter.checkDomainMatchToUrl("example.com", "www.example.com."),
                "the root label is not part of the host name");
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

    @Test
    void testDomainsChecker5() {
        boolean result = CookieConverter.checkDomainMatchToUrl("example.com", null);
        Assertions.assertFalse(result, "domain can not be checked");
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
