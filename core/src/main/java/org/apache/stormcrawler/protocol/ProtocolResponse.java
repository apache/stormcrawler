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

import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.stormcrawler.Metadata;

public class ProtocolResponse {

    /**
     * Key which holds the verbatim HTTP request headers in metadata (if supported by Protocol
     * implementation and if http.store.headers is true).
     */
    public static final String REQUEST_HEADERS_KEY = "_request.headers_";

    /** Key which holds the verbatim HTTP response headers in metadata. */
    public static final String RESPONSE_HEADERS_KEY = "_response.headers_";

    /**
     * Key which holds the IP address of the server the request was sent to (response received from)
     * in metadata.
     */
    public static final String RESPONSE_IP_KEY = "_response.ip_";

    /** Key which holds the request time (begin of request) in metadata. */
    public static final String REQUEST_TIME_KEY = "_request.time_";

    /**
     * Key which holds the protocol version(s) used for this request (for layered protocols this
     * field may hold multiple comma-separated values).
     */
    public static final String PROTOCOL_VERSIONS_KEY = "_protocol_versions_";

    /**
     * Key which holds the SSL/TLS cipher suite. Not set if the request was sent over an unencrypted
     * connection (http://).
     */
    public static final String CIPHER_SUITE_KEY = "_cipher_suite_";

    /**
     * Metadata key which holds a boolean value in metadata whether the response content is trimmed
     * or not.
     */
    public static final String TRIMMED_RESPONSE_KEY = "http.trimmed";

    /**
     * Metadata key which holds the reason why content has been trimmed, see {@link
     * TrimmedContentReason}.
     */
    public static final String TRIMMED_RESPONSE_REASON_KEY = "http.trimmed.reason";

    /**
     * Metadata key which holds the URL a followed redirect chain ended on, set when the content of
     * a response does not come from the URL it was requested with. Related to, but distinct from,
     * the {@code _redirTo} key the fetcher and parser bolts set on {@code Status.REDIRECTION}
     * tuples: that one names the immediate target of a redirect which was NOT followed, this one
     * names where an actually followed chain ended.
     */
    public static final String REDIRECTED_TO_KEY = "_redirectedTo";

    /**
     * @since 1.17
     * @see <a href="https://github.com/apache/stormcrawler/issues/776">Issue 776</a>
     */
    public static final String PROTOCOL_MD_PREFIX_PARAM = "protocol.md.prefix";

    /** Prefix of the metadata keys holding the timings a protocol reports about a fetch. */
    public static final String METRICS_PREFIX = "metrics.";

    /** Lowercased: metadata keys are normalised that way, and so are header names. */
    private static final Set<String> RESERVED_METADATA_KEYS =
            Stream.of(
                            REQUEST_HEADERS_KEY,
                            RESPONSE_HEADERS_KEY,
                            RESPONSE_IP_KEY,
                            REQUEST_TIME_KEY,
                            PROTOCOL_VERSIONS_KEY,
                            CIPHER_SUITE_KEY,
                            TRIMMED_RESPONSE_KEY,
                            TRIMMED_RESPONSE_REASON_KEY,
                            REDIRECTED_TO_KEY)
                    .map(key -> key.toLowerCase(Locale.ROOT))
                    .collect(Collectors.toUnmodifiableSet());

    /**
     * Whether a metadata key is one the crawler writes itself to record how a fetch went. The
     * protocols copy the response headers into the metadata, and a header name is not restricted to
     * anything: a value arriving from the wire must never populate one of these keys. The WARC
     * writer reads them as the crawler's own account of the request and the fetcher bolts read
     * {@value #METRICS_PREFIX} as their protocol timings.
     */
    public static boolean isReservedMetadataKey(String key) {
        final String normalised = key.toLowerCase(Locale.ROOT);
        return RESERVED_METADATA_KEYS.contains(normalised) || normalised.startsWith(METRICS_PREFIX);
    }

    /** Enum of reasons which may cause that protocol content is trimmed. */
    public enum TrimmedContentReason {
        NOT_TRIMMED,
        /** fetch exceeded configured http.content.limit. */
        LENGTH,
        /** fetch exceeded configured max. time for fetch */
        TIME,
        /** network disconnect or timeout during fetch. */
        DISCONNECT,
        /** implementation internal reason. */
        INTERNAL,
        /** unknown reason. */
        UNSPECIFIED
    }

    private final byte[] content;
    private final int statusCode;
    private final Metadata metadata;

    public ProtocolResponse(byte[] c, int s, Metadata md) {
        content = c;
        statusCode = s;
        metadata = md == null ? new Metadata() : md;
    }

    public byte[] getContent() {
        return content;
    }

    public int getStatusCode() {
        return statusCode;
    }

    public Metadata getMetadata() {
        return metadata;
    }
}
