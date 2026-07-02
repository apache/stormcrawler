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
     * @since 1.17
     * @see <a href="https://github.com/apache/stormcrawler/issues/776">Issue 776</a>
     */
    public static final String PROTOCOL_MD_PREFIX_PARAM = "protocol.md.prefix";

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
