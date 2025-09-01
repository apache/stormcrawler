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

import java.net.URL;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import org.apache.http.cookie.Cookie;
import org.apache.http.impl.cookie.BasicClientCookie;

/** Helper to extract cookies from cookies string. */
public class CookieConverter {

    private static final SimpleDateFormat DATE_FORMAT =
            new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss zzz", Locale.ENGLISH);

    /**
     * Get a list of cookies based on the cookies string taken from response header and the target
     * url.
     *
     * @param cookiesStrings the value(s) of the http header for "Cookie" in the http response.
     * @param targetURL the url for which we wish to pass the cookies in the request.
     * @return List off cookies to add to the request.
     */
    public static List<Cookie> getCookies(String[] cookiesStrings, URL targetURL) {
        ArrayList<Cookie> list = new ArrayList<>();

        for (String cs : cookiesStrings) {
            String name = null;
            String value = null;

            String expires = null;
            String domain = null;
            String path = null;

            boolean secure = false;

            String[] tokens = cs.split(";");

            int equals = tokens[0].indexOf("=");
            name = tokens[0].substring(0, equals);
            value = tokens[0].substring(equals + 1);

            for (int i = 1; i < tokens.length; i++) {
                String ti = tokens[i].trim();
                if (ti.equalsIgnoreCase("secure")) secure = true;
                if (ti.toLowerCase(Locale.ROOT).startsWith("path=")) {
                    path = ti.substring(5);
                }
                if (ti.toLowerCase(Locale.ROOT).startsWith("domain=")) {
                    domain = ti.substring(7);
                }
                if (ti.toLowerCase(Locale.ROOT).startsWith("expires=")) {
                    expires = ti.substring(8);
                }
            }

            BasicClientCookie cookie = new BasicClientCookie(name, value);

            // check domain
            if (domain != null) {
                cookie.setDomain(domain);

                if (!checkDomainMatchToUrl(domain, targetURL.getHost())) continue;
            }

            // check path
            if (path != null) {
                cookie.setPath(path);

                if (!path.equals("") && !path.equals("/") && !targetURL.getPath().startsWith(path))
                    continue;
            }

            // check secure
            if (secure) {
                cookie.setSecure(secure);

                if (!targetURL.getProtocol().equalsIgnoreCase("https")) continue;
            }

            // check expiration
            if (expires != null) {
                try {
                    Date expirationDate = DATE_FORMAT.parse(expires);
                    cookie.setExpiryDate(expirationDate);

                    // check that it hasn't expired?
                    if (cookie.isExpired(new Date())) continue;

                    cookie.setExpiryDate(expirationDate);
                } catch (ParseException e) {
                    // ignore exceptions
                }
            }

            // attach additional infos to cookie
            list.add(cookie);
        }

        return list;
    }

    /**
     * Helper method to check if url matches a cookie domain.
     *
     * @param cookieDomain the domain in the cookie
     * @param urlHostName the host name of the url
     * @return does the cookie match the host name
     */
    public static boolean checkDomainMatchToUrl(String cookieDomain, String urlHostName) {
        try {
            if (cookieDomain.startsWith(".")) {
                cookieDomain = cookieDomain.substring(1);
            }
            String[] domainTokens = cookieDomain.split("\\.");
            String[] hostTokens = urlHostName.split("\\.");

            int tokenDif = hostTokens.length - domainTokens.length;
            if (tokenDif < 0) {
                return false;
            }

            for (int i = domainTokens.length - 1; i >= 0; i--) {
                if (!domainTokens[i].equalsIgnoreCase(hostTokens[i + tokenDif])) {
                    return false;
                }
            }
            return true;
        } catch (Exception e) {
            return true;
        }
    }
}
