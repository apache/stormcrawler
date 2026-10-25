/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.stormcrawler.shade.httpcore5;

/**
 * Constants enumerating standard and common HTTP headers.
 *
 * <p>Copied from {@code org.apache.hc.core5.http.HttpHeaders} in Apache HttpComponents Core 5.4.3
 * (org.apache.httpcomponents.core5:httpcore5:5.4.3), so that StormCrawler does not depend on
 * HttpComponents (#2266).
 *
 * <p>For internal use only. Not part of StormCrawler's public API.
 */
public final class HttpHeaders {

    private HttpHeaders() {
        // Don't allow instantiation.
    }

    public static final String ACCEPT = "Accept";

    public static final String ACCEPT_CHARSET = "Accept-Charset";

    public static final String ACCEPT_ENCODING = "Accept-Encoding";

    public static final String ACCEPT_LANGUAGE = "Accept-Language";

    public static final String ACCEPT_RANGES = "Accept-Ranges";

    /** The CORS {@code Access-Control-Allow-Credentials} response header field name. */
    public static final String ACCESS_CONTROL_ALLOW_CREDENTIALS =
            "Access-Control-Allow-Credentials";

    /** The CORS {@code Access-Control-Allow-Headers} response header field name. */
    public static final String ACCESS_CONTROL_ALLOW_HEADERS = "Access-Control-Allow-Headers";

    /** The CORS {@code Access-Control-Allow-Methods} response header field name. */
    public static final String ACCESS_CONTROL_ALLOW_METHODS = "Access-Control-Allow-Methods";

    /** The CORS {@code Access-Control-Allow-Origin} response header field name. */
    public static final String ACCESS_CONTROL_ALLOW_ORIGIN = "Access-Control-Allow-Origin";

    /** The CORS {@code Access-Control-Expose-Headers} response header field name. */
    public static final String ACCESS_CONTROL_EXPOSE_HEADERS = "Access-Control-Expose-Headers";

    /** The CORS {@code Access-Control-Max-Age} response header field name. */
    public static final String ACCESS_CONTROL_MAX_AGE = "Access-Control-Max-Age";

    /** The CORS {@code Access-Control-Request-Headers} request header field name. */
    public static final String ACCESS_CONTROL_REQUEST_HEADERS = "Access-Control-Request-Headers";

    /** The CORS {@code Access-Control-Request-Method} request header field name. */
    public static final String ACCESS_CONTROL_REQUEST_METHOD = "Access-Control-Request-Method";

    public static final String AGE = "Age";

    public static final String ALLOW = "Allow";

    public static final String AUTHORIZATION = "Authorization";

    public static final String CACHE_CONTROL = "Cache-Control";

    public static final String CONNECTION = "Connection";

    public static final String CONTENT_ENCODING = "Content-Encoding";

    /** The HTTP {@code Content-Disposition} header field name. */
    public static final String CONTENT_DISPOSITION = "Content-Disposition";

    public static final String CONTENT_LANGUAGE = "Content-Language";

    public static final String CONTENT_LENGTH = "Content-Length";

    public static final String CONTENT_LOCATION = "Content-Location";

    public static final String CONTENT_MD5 = "Content-MD5";

    public static final String CONTENT_RANGE = "Content-Range";

    public static final String CONTENT_TYPE = "Content-Type";

    /** The HTTP {@code Cookie} header field name. */
    public static final String COOKIE = "Cookie";

    public static final String DATE = "Date";

    public static final String DAV = "Dav";

    public static final String DEPTH = "Depth";

    public static final String DESTINATION = "Destination";

    public static final String ETAG = "ETag";

    public static final String EXPECT = "Expect";

    public static final String EXPIRES = "Expires";

    public static final String FROM = "From";

    public static final String HOST = "Host";

    public static final String IF = "If";

    public static final String IF_MATCH = "If-Match";

    public static final String IF_MODIFIED_SINCE = "If-Modified-Since";

    public static final String IF_NONE_MATCH = "If-None-Match";

    public static final String IF_RANGE = "If-Range";

    public static final String IF_UNMODIFIED_SINCE = "If-Unmodified-Since";

    public static final String KEEP_ALIVE = "Keep-Alive";

    public static final String LAST_MODIFIED = "Last-Modified";

    /** The HTTP {@code Link} header field name. */
    public static final String LINK = "Link";

    public static final String LOCATION = "Location";

    public static final String LOCK_TOKEN = "Lock-Token";

    public static final String MAX_FORWARDS = "Max-Forwards";

    public static final String OVERWRITE = "Overwrite";

    public static final String PRAGMA = "Pragma";

    public static final String PROXY_AUTHENTICATE = "Proxy-Authenticate";

    public static final String PROXY_AUTHORIZATION = "Proxy-Authorization";

    public static final String PROXY_CONNECTION = "Proxy-Connection";

    public static final String RANGE = "Range";

    public static final String REFERER = "Referer";

    public static final String RETRY_AFTER = "Retry-After";

    public static final String SERVER = "Server";

    public static final String STATUS_URI = "Status-URI";

    /** The HTTP {@code Set-Cookie} header field name. */
    public static final String SET_COOKIE = "Set-Cookie";

    public static final String TE = "TE";

    public static final String TIMEOUT = "Timeout";

    public static final String TRAILER = "Trailer";

    public static final String TRANSFER_ENCODING = "Transfer-Encoding";

    public static final String UPGRADE = "Upgrade";

    public static final String USER_AGENT = "User-Agent";

    public static final String VARY = "Vary";

    public static final String VIA = "Via";

    public static final String WARNING = "Warning";

    public static final String WWW_AUTHENTICATE = "WWW-Authenticate";

    /**
     * RFC 7639 — ALPN HTTP header field used with CONNECT to advertise the application protocols
     * intended to run inside the tunnel (e.g. {@code h2}, {@code http/1.1}).
     *
     * @since 5.4
     */
    public static final String ALPN = "ALPN";

    /** The HTTP {@code Priority} header field name. */
    public static final String PRIORITY = "Priority";
}
