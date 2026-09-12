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

import java.net.IDN;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Utility class for URL analysis. */
public class URLUtil {

    private URLUtil() {}

    /** Pattern for non-standard {@code %uXXXX} percent encoding (e.g. from JavaScript escape()). */
    private static final Pattern ILLEGAL_ESCAPE_PATTERN = Pattern.compile("%u([0-9A-Fa-f]{4})");

    /**
     * Converts a URL string to a {@link URL} object using {@link URI} to avoid the deprecated
     * {@code new URL(String)} constructor. If strict RFC 3986 parsing fails, common illegal
     * characters are sanitized and parsing is retried, mimicking the leniency of the old {@code new
     * URL(String)} constructor.
     *
     * @param url the URL string to convert
     * @return the parsed URL
     * @throws MalformedURLException if the string is not a valid URL even after sanitization
     */
    public static URL toURL(String url) throws MalformedURLException {
        try {
            return toURI(url).toURL();
        } catch (IllegalArgumentException e) {
            throw (MalformedURLException) new MalformedURLException(e.getMessage()).initCause(e);
        }
    }

    /**
     * Converts a URL string to a {@link URI} object. If strict RFC 3986 parsing fails, common
     * illegal characters are sanitized and parsing is retried, mimicking the leniency of the old
     * {@code new URL(String)} constructor.
     *
     * @param url the URL string to convert
     * @return the parsed URI
     * @throws MalformedURLException if the string is not a valid URI even after sanitization
     */
    public static URI toURI(String url) throws MalformedURLException {
        try {
            return new URI(url);
        } catch (URISyntaxException e) {
            // strict parsing failed — try sanitizing common illegal characters
            try {
                return new URI(sanitizeForURI(url));
            } catch (URISyntaxException e2) {
                throw (MalformedURLException)
                        new MalformedURLException(e.getMessage()).initCause(e);
            }
        }
    }

    /**
     * Pre-sanitize a URL string by encoding characters that are illegal in URIs per RFC 3986 but
     * commonly found in URLs in the wild. Also converts non-standard {@code %uXXXX} percent
     * encoding to proper UTF-8 percent encoding.
     */
    public static String sanitizeForURI(String url) {
        // Handle non-standard %uXXXX encoding (e.g. from JavaScript escape())
        final Matcher matcher = ILLEGAL_ESCAPE_PATTERN.matcher(url);
        if (matcher.find()) {
            final StringBuilder sb = new StringBuilder();
            int end = 0;
            do {
                sb.append(url, end, matcher.start());
                final int codePoint = Integer.parseInt(matcher.group(1), 16);
                for (byte b :
                        new String(Character.toChars(codePoint)).getBytes(StandardCharsets.UTF_8)) {
                    sb.append(String.format(Locale.ROOT, "%%%02X", b & 0xFF));
                }
                end = matcher.end();
            } while (matcher.find());
            sb.append(url.substring(end));
            url = sb.toString();
        }

        // Encode characters that are illegal in URIs but commonly encountered
        final StringBuilder sb = new StringBuilder(url.length());
        for (int i = 0; i < url.length(); i++) {
            final char c = url.charAt(i);
            switch (c) {
                case '|':
                    sb.append("%7C");
                    break;
                case '\\':
                    sb.append("%5C");
                    break;
                case ' ':
                    sb.append("%20");
                    break;
                case '{':
                    sb.append("%7B");
                    break;
                case '}':
                    sb.append("%7D");
                    break;
                default:
                    sb.append(c);
            }
        }
        return sb.toString();
    }

    /**
     * Resolve relative URL-s and fix a few java.net.URL errors in handling of URLs with embedded
     * params and pure query targets.
     *
     * @param base base url
     * @param target target url (may be relative)
     * @return resolved absolute url.
     * @throws MalformedURLException
     */
    public static URL resolveUrl(URL base, String target) throws MalformedURLException {
        target = target.trim();

        if (target.startsWith("?")) {
            return fixPureQueryTargets(base, target);
        }

        return resolveURLInternal(base, target);
    }

    /**
     * Refactor deprecated URL constructor to use the URI class for resolving relative URLs.
     *
     * @param base the base URL
     * @param target the target URL (may be relative)
     * @return resolved absolute URL.
     * @throws MalformedURLException if the URL is not well formed
     */
    private static URL resolveURLInternal(URL base, String target) throws MalformedURLException {
        try {
            return base.toURI().resolve(target).toURL();
        } catch (Exception e) {
            throw (MalformedURLException) new MalformedURLException(e.getMessage()).initCause(e);
        }
    }

    /** Handle the case in RFC3986 section 5.4.1 example 7, and similar. */
    static URL fixPureQueryTargets(final URL base, String target) throws MalformedURLException {
        final String basePath = base.getPath();
        int baseRightMostIdx = basePath.lastIndexOf("/");
        if (baseRightMostIdx != -1) {
            final String baseRightMost = basePath.substring(baseRightMostIdx + 1);
            target = baseRightMost + target;
        }
        return resolveURLInternal(base, target);
    }

    /**
     * Handles cases where the url param information is encoded into the base url as opposed to the
     * target.
     *
     * <p>If the target contains params (i.e. ';xxxx') information then the target params
     * information is assumed to be correct and any base params information is ignored. If the base
     * contains params information but the target does not, then the params information is moved to
     * the target allowing it to be correctly determined by the java.net.URL class.
     *
     * @param base The base URL.
     * @param target The target path from the base URL.
     * @return URL A URL with the params information correctly encoded.
     * @throws MalformedURLException If the url is not a well formed URL.
     */
    private static URL fixEmbeddedParams(URL base, String target) throws MalformedURLException {

        // the target contains params information or the base doesn't then no
        // conversion necessary, return regular URL
        if (target.indexOf(';') >= 0 || base.toString().indexOf(';') == -1) {
            return resolveURLInternal(base, target);
        }

        // get the base url and it params information
        String baseUrl = base.toString();
        int startParams = baseUrl.indexOf(';');
        String params = baseUrl.substring(startParams);

        // if the target has a query string then put the params information
        // after
        // any path but before the query string, otherwise just append to the
        // path
        int startQueryString = target.indexOf('?');
        if (startQueryString >= 0) {
            target =
                    target.substring(0, startQueryString)
                            + params
                            + target.substring(startQueryString);
        } else {
            target += params;
        }

        return resolveURLInternal(base, target);
    }

    private static Pattern IP_PATTERN = Pattern.compile("(\\d{1,3}\\.){3}(\\d{1,3})");

    /** Partitions of the hostname of the url by ".". */
    public static String[] getHostSegments(URL url) {
        String host = url.getHost();
        // return whole hostname, if it is an ipv4
        // TODO: handle ipv6
        if (IP_PATTERN.matcher(host).matches()) {
            return new String[] {host};
        }
        return host.split("\\.");
    }

    /**
     * Partitions of the hostname of the url by ".".
     *
     * @throws MalformedURLException
     */
    public static String[] getHostSegments(String url) throws MalformedURLException {
        return getHostSegments(toURL(url));
    }

    /**
     * Returns the lowercased hostname for the url or null if the url is not well formed.
     *
     * @param url The url to check.
     * @return String The hostname for the url.
     */
    public static String getHost(String url) {
        try {
            return toURL(url).getHost().toLowerCase(Locale.ROOT);
        } catch (MalformedURLException e) {
            return null;
        }
    }

    /**
     * Returns the form of the host used to key politeness queues and the robots.txt cache: what
     * okhttp connects to, with the root label normalised away. Host strings which only differ in
     * escaping or case reach the same server, so both spellings must end up under one key,
     * otherwise one server is fetched under several queue ids and its robots.txt is downloaded once
     * per spelling.
     *
     * @param url The url to check.
     * @return String The canonical host for the url, or null if the url is not well formed or has
     *     no host.
     */
    public static String getCanonicalHost(URL url) {
        String host = url.getHost();
        if (host == null) {
            return null;
        }
        // okhttp percent-decodes the host when it parses the URL; do the same
        // so keys derived from the URL string agree with what it connects to.
        // The decoder never throws: crawled content is hostile input, and a
        // malformed escape falls back to the raw spelling rather than blowing
        // up the caller
        String decoded = percentDecodeHost(host);
        if (decoded.endsWith(".")) {
            decoded = decoded.substring(0, decoded.length() - 1);
        }
        return decoded.toLowerCase(Locale.ROOT);
    }

    /**
     * Percent-decodes a host string, leaving {@code +} alone and keeping malformed escapes as
     * literal characters. The decoded octets are interpreted as UTF-8, so a multi-byte sequence
     * like {@code %C3%BC} becomes one character ({@code ü}) rather than one character per octet.
     * Unlike {@link URLDecoder#decode}, this never throws.
     */
    private static String percentDecodeHost(String host) {
        if (!host.contains("%")) {
            return host;
        }
        java.io.ByteArrayOutputStream bytes = new java.io.ByteArrayOutputStream(host.length());
        for (int i = 0; i < host.length(); i++) {
            char c = host.charAt(i);
            if (c == '%' && i + 2 < host.length()) {
                int hi = Character.digit(host.charAt(i + 1), 16);
                int lo = Character.digit(host.charAt(i + 2), 16);
                if (hi != -1 && lo != -1) {
                    bytes.write((hi << 4) | lo);
                    i += 2;
                    continue;
                }
            }
            // a literal character: encode it as UTF-8 so it interleaves correctly
            // with the decoded octets (hosts are ASCII in practice, but a raw
            // non-ASCII label must not be mangled)
            byte[] encoded = String.valueOf(c).getBytes(StandardCharsets.UTF_8);
            bytes.write(encoded, 0, encoded.length);
        }
        return new String(bytes.toByteArray(), StandardCharsets.UTF_8);
    }

    /**
     * Returns the url with its host replaced by {@link #getCanonicalHost(URL)}: percent-escapes
     * decoded, lowercased and without a trailing dot. Only the authority changes; everything else
     * in the url is kept byte for byte. Host aliases of one server therefore end up as one record
     * in the status store, one politeness queue and one robots.txt cache entry, but two different
     * urls stay two different urls. The url is returned unchanged when it has no host, cannot be
     * parsed, or is already canonical.
     *
     * @param url The url to normalise.
     * @return String The url with a canonical host, or the input unchanged.
     */
    public static String normaliseHost(String url) {
        try {
            URL u = toURL(url);
            String host = u.getHost();
            if (host == null || host.isEmpty() || host.startsWith("[")) {
                // no host, or an IPv6 literal: nothing to collapse, leave as is
                return url;
            }
            String canonical = getCanonicalHost(u);
            if (canonical == null || canonical.equals(host)) {
                return url;
            }
            // the host never contains characters which would end the authority
            // (":", "/", "?", "#", "@" are all illegal in a host); a decoded
            // escape which produced one of them leaves the url unchanged
            if (canonical.indexOf(':') >= 0
                    || canonical.indexOf('/') >= 0
                    || canonical.indexOf('?') >= 0
                    || canonical.indexOf('#') >= 0
                    || canonical.indexOf('@') >= 0) {
                return url;
            }
            // splice the canonical host back into the original string, keeping
            // the scheme, any user info, the port and everything after the
            // authority exactly as they were
            int schemeEnd = url.indexOf("//");
            if (schemeEnd < 0) {
                return url;
            }
            int authorityStart = schemeEnd + 2;
            // the host part starts after the user info
            int at = url.lastIndexOf('@', indexOfAuthorityEnd(url, authorityStart));
            int hostStart = Math.max(at + 1, authorityStart);
            int hostEnd = indexOfHostEnd(url, hostStart);
            if (hostEnd < 0) {
                return url;
            }
            return url.substring(0, hostStart) + canonical + url.substring(hostEnd);
        } catch (MalformedURLException | IllegalArgumentException e) {
            return url;
        }
    }

    /** Index of the end of the authority of a URL string starting at pos. */
    private static int indexOfAuthorityEnd(String url, int pos) {
        for (int i = pos; i < url.length(); i++) {
            char c = url.charAt(i);
            if (c == '/' || c == '?' || c == '#') {
                return i;
            }
        }
        return url.length();
    }

    /**
     * Index just after the host part of an authority: hosts never contain ':', so the first colon
     * after the host starts the port, and "/", "?" or "#" end the authority.
     */
    private static int indexOfHostEnd(String url, int hostStart) {
        int authorityEnd = indexOfAuthorityEnd(url, hostStart);
        int colon = url.indexOf(':', hostStart);
        return colon == -1 || colon >= authorityEnd ? authorityEnd : colon;
    }

    /**
     * Returns the page for the url. The page consists of the protocol, host, and path, but does not
     * include the query string. The host is lowercased but the path is not.
     *
     * @param url The url to check.
     * @return String The page for the url.
     */
    public static String getPage(String url) {
        try {
            // get the full url, and replace the query string with and empty
            // string
            url = url.toLowerCase(Locale.ROOT);
            String queryStr = toURL(url).getQuery();
            return (queryStr != null) ? url.replace("?" + queryStr, "") : url;
        } catch (MalformedURLException e) {
            return null;
        }
    }

    public static String toASCII(String url) {
        try {
            URL u = toURL(url);
            URI p =
                    new URI(
                            u.getProtocol(),
                            null,
                            IDN.toASCII(u.getHost()),
                            u.getPort(),
                            u.getPath(),
                            u.getQuery(),
                            u.getRef());

            return p.toString();
        } catch (Exception e) {
            return null;
        }
    }

    public static String toUNICODE(String url) {
        try {
            URL u = toURL(url);
            URI p =
                    new URI(
                            u.getProtocol(),
                            null,
                            IDN.toUnicode(u.getHost()),
                            u.getPort(),
                            u.getPath(),
                            u.getQuery(),
                            u.getRef());

            return p.toString();
        } catch (Exception e) {
            return null;
        }
    }
}
