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

import crawlercommons.domains.EffectiveTldFinder;
import java.net.IDN;
import java.net.URL;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import org.apache.http.cookie.Cookie;
import org.apache.http.impl.cookie.BasicClientCookie;

/** Helper to extract cookies from cookies string. */
public class CookieConverter {

    private static final org.slf4j.Logger LOG =
            org.slf4j.LoggerFactory.getLogger(CookieConverter.class);

    /** Maximum length of a domain name in ASCII characters, see RFC 1034. */
    private static final int MAX_DOMAIN_LENGTH = 253;

    /**
     * Get a list of cookies based on the cookies string taken from response header and the target
     * url. As the host which set the cookies is unknown, cookies without a domain attribute are
     * dropped instead of being sent to the target url.
     *
     * @param cookiesStrings the value(s) of the http header for "Cookie" in the http response.
     * @param targetURL the url for which we wish to pass the cookies in the request.
     * @return List off cookies to add to the request.
     * @deprecated use {@link #getCookies(String[], URL, URL)} instead
     */
    @Deprecated
    public static List<Cookie> getCookies(String[] cookiesStrings, URL targetURL) {
        return getCookies(cookiesStrings, null, targetURL);
    }

    /**
     * Get a list of cookies based on the cookies string taken from response header, the url which
     * set them and the target url.
     *
     * <p>A cookie without a domain attribute is only returned when the target url has the same host
     * as the url which set the cookie, as required by RFC 6265.
     *
     * <p>A cookie with a domain attribute is only returned when both urls match that domain.
     * Malformed domain attributes are rejected. A domain which is itself a public suffix, looked up
     * in the public suffix list shipped with crawler-commons and including its private section such
     * as "github.io", or which is made of a single label, does not cover subdomains: the cookie is
     * then bound to the host it was set on, as browsers do. Domains which the list does not know
     * about, such as internal names under ".local" or ".internal", keep the plain suffix match so
     * that they remain usable as a cookie scope. A public suffix missing from the list, whether
     * because the bundled list is a snapshot or because the suffix is genuinely internal, therefore
     * still behaves as a normal domain.
     *
     * @param cookiesStrings the value(s) of the http header for "Cookie" in the http response.
     * @param originURL the url whose response set the cookies, or null when it is unknown. The
     *     okhttp protocol takes it from the metadata written next to the cookies when a response
     *     carries them.
     * @param targetURL the url for which we wish to pass the cookies in the request.
     * @return List off cookies to add to the request.
     */
    public static List<Cookie> getCookies(String[] cookiesStrings, URL originURL, URL targetURL) {
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
            if (equals < 1) {
                // no name=value pair to take: the header is not usable
                LOG.debug("Skipping cookie: no name in {}", tokens[0]);
                continue;
            }
            name = tokens[0].substring(0, equals);
            value = tokens[0].substring(equals + 1);

            for (int i = 1; i < tokens.length; i++) {
                String ti = tokens[i].trim();
                if (ti.equalsIgnoreCase("secure")) {
                    secure = true;
                }
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
            if (domain != null && !domain.isBlank()) {
                final String scope = normaliseDomain(domain);
                if (scope == null) {
                    LOG.debug("Skipping cookie {}: malformed domain {}", name, domain);
                    continue;
                }

                cookie.setDomain(domain);

                // a domain which can not own subdomains, i.e. a public suffix such as
                // "com" or "co.uk" or a single label, binds the cookie to the host itself
                final boolean subdomains = allowsSubdomains(scope);

                if (!hostMatches(scope, targetURL.getHost(), subdomains)) {
                    LOG.debug(
                            "Skipping cookie {}: domain {} does not cover the target {}",
                            name,
                            domain,
                            targetURL);
                    continue;
                }

                // the host which set the cookie must be covered by the domain too
                if (originURL != null && !hostMatches(scope, originURL.getHost(), subdomains)) {
                    LOG.debug(
                            "Skipping cookie {}: domain {} does not cover {}, which set it",
                            name,
                            domain,
                            originURL);
                    continue;
                }
            } else {
                // host only cookie: valid for the host which set it and nothing else
                if (originURL == null) {
                    LOG.debug("Skipping cookie {}: the host which set it is unknown", name);
                    continue;
                }
                if (!originURL.getHost().equalsIgnoreCase(targetURL.getHost())) {
                    LOG.debug(
                            "Skipping cookie {}: set by {} and not valid for {}",
                            name,
                            originURL,
                            targetURL);
                    continue;
                }
            }

            // check path
            if (path != null) {
                cookie.setPath(path);

                if (!path.equals("")
                        && !path.equals("/")
                        && !targetURL.getPath().startsWith(path)) {
                    LOG.debug(
                            "Skipping cookie {}: path {} does not cover the target {}",
                            name,
                            path,
                            targetURL);
                    continue;
                }
            }

            // check secure
            if (secure) {
                cookie.setSecure(secure);

                if (!targetURL.getProtocol().equalsIgnoreCase("https")) {
                    LOG.debug("Skipping cookie {}: secure and {} is not https", name, targetURL);
                    continue;
                }
            }

            // check expiration
            if (expires != null) {
                try {
                    Date expirationDate = org.apache.http.client.utils.DateUtils.parseDate(expires);
                    if (expirationDate != null) {
                        cookie.setExpiryDate(expirationDate);

                        // check that it hasn't expired?
                        if (cookie.isExpired(new Date())) {
                            LOG.debug("Skipping cookie {}: expired on {}", name, expires);
                            continue;
                        }
                    }
                } catch (Exception e) {
                    LOG.debug("Could not parse cookie expiry date: {}", expires, e);
                }
            }

            // attach additional infos to cookie
            list.add(cookie);
        }

        return list;
    }

    /**
     * Normalises the value of a cookie domain attribute into the domain it scopes the cookie to.
     *
     * @param domain the value of the domain attribute
     * @return the lower cased domain, without the leading dot allowed by RFC 6265 and without the
     *     root label, or null when it is not a well formed domain name
     */
    private static String normaliseDomain(String domain) {
        String d = domain.trim().toLowerCase(Locale.ROOT);
        // RFC 6265 5.2.3 allows exactly one leading dot
        if (d.startsWith(".")) {
            d = d.substring(1);
        }
        d = toAscii(stripRootDot(d));
        if (d == null || d.length() > MAX_DOMAIN_LENGTH) {
            return null;
        }
        for (String label : d.split("\\.", -1)) {
            if (!isValidLabel(label)) {
                return null;
            }
        }
        return d;
    }

    /**
     * Converts a name to the ascii form in which the public suffix list and the hosts are compared,
     * so that a domain attribute written in unicode, e.g. "münchen.de", scopes its cookie to
     * "xn--mnchen-3ya.de" and the two forms are interchangeable.
     *
     * @param name a lower cased name, without its root label
     * @return the ascii name, or null when it is not a well formed domain name, e.g. "com.."
     */
    private static String toAscii(String name) {
        if (name.isEmpty()) {
            return null;
        }
        try {
            // rejects the empty labels which the default split drops, so that a domain such as
            // "com.." can not reach every host under "com"
            return IDN.toASCII(name, IDN.ALLOW_UNASSIGNED);
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    /** Checks that an ascii domain label is made of letters, digits and inner hyphens. */
    private static boolean isValidLabel(String label) {
        if (label.isEmpty() || label.length() > EffectiveTldFinder.MAX_DOMAIN_LENGTH_PART) {
            return false;
        }
        if (label.charAt(0) == '-' || label.charAt(label.length() - 1) == '-') {
            return false;
        }
        for (int i = 0; i < label.length(); i++) {
            final char c = label.charAt(i);
            if (c != '-' && (c < 'a' || c > 'z') && (c < '0' || c > '9')) {
                return false;
            }
        }
        return true;
    }

    /**
     * Checks whether a name is an address rather than a domain name. RFC 6265 5.1.3 only lets a
     * cookie go back to that exact address, so an address never covers anything below it.
     *
     * @param name a host name or a normalised cookie domain
     * @return true when the name is an ipv4 or ipv6 address
     */
    private static boolean isAddress(String name) {
        if (name.isEmpty()) {
            return false;
        }
        // an ipv6 literal, bracketed as URL.getHost() returns it or bare
        if (name.charAt(0) == '[' || name.indexOf(':') >= 0) {
            return true;
        }
        // a top level domain is never made of digits only, so this is the last part of an
        // address, e.g. "1.2.3.4" but also a truncation of it such as "2.3.4"
        final String last = name.substring(name.lastIndexOf('.') + 1);
        if (last.isEmpty()) {
            return false;
        }
        for (int i = 0; i < last.length(); i++) {
            final char c = last.charAt(i);
            if (c < '0' || c > '9') {
                return false;
            }
        }
        return true;
    }

    /**
     * Checks whether a cookie can be scoped to every host under a domain. The public suffix list
     * shipped with crawler-commons is used, its private section included, so that domains such as
     * "github.io" are treated as suffixes like browsers do. A domain which the list does not know
     * about, e.g. an internal name under ".local", falls back to the plain suffix match so that
     * unlisted domains keep working.
     *
     * @param domain a domain returned by {@link #normaliseDomain(String)}
     * @return true when hosts below the domain are covered by it
     */
    private static boolean allowsSubdomains(String domain) {
        if (domain.indexOf('.') < 0) {
            // a single label is either an intranet host name or a bare top level domain
            return false;
        }
        if (isAddress(domain)) {
            return false;
        }
        if (EffectiveTldFinder.getEffectiveTLD(domain, false) == null) {
            return true;
        }
        // the domain is covered by a public suffix: usable only when it sits below it
        return EffectiveTldFinder.getAssignedDomain(domain, true, false) != null;
    }

    /** Checks whether a host is covered by a cookie domain. */
    private static boolean hostMatches(String domain, String host, boolean subdomains) {
        if (host == null) {
            return false;
        }
        final String h = stripRootDot(host.toLowerCase(Locale.ROOT));
        if (isAddress(h)) {
            // RFC 6265 5.1.3: an address is only matched by itself
            return domain.equals(h);
        }
        final String ascii = toAscii(h);
        if (ascii == null) {
            return false;
        }
        if (subdomains) {
            return checkDomainMatchToUrl(domain, ascii);
        }
        // RFC 6265 5.3: a domain attribute which can not own subdomains is ignored and the
        // cookie is bound to the host it was set on
        return domain.equals(ascii);
    }

    /** Removes the root label from a domain name, e.g. "example.com." becomes "example.com". */
    private static String stripRootDot(String name) {
        return name.endsWith(".") ? name.substring(0, name.length() - 1) : name;
    }

    /**
     * Helper method to check if url matches a cookie domain. It only answers whether the host falls
     * under the domain and does not decide whether the domain may be used as a cookie scope, e.g.
     * "com" matches "example.com".
     *
     * @param cookieDomain the domain in the cookie
     * @param urlHostName the host name of the url
     * @return does the cookie match the host name
     */
    public static boolean checkDomainMatchToUrl(String cookieDomain, String urlHostName) {
        if (cookieDomain == null || urlHostName == null) {
            return false;
        }
        if (cookieDomain.startsWith(".")) {
            cookieDomain = cookieDomain.substring(1);
        }
        // the negative limit keeps the empty labels which the default split drops, so that
        // a domain such as "com.." can not match every host under "com"
        final String[] domainTokens = stripRootDot(cookieDomain).split("\\.", -1);
        final String[] hostTokens = stripRootDot(urlHostName).split("\\.", -1);

        final int tokenDif = hostTokens.length - domainTokens.length;
        if (tokenDif < 0) {
            return false;
        }

        for (int i = domainTokens.length - 1; i >= 0; i--) {
            if (domainTokens[i].isEmpty()
                    || !domainTokens[i].equalsIgnoreCase(hostTokens[i + tokenDif])) {
                return false;
            }
        }
        return true;
    }
}
