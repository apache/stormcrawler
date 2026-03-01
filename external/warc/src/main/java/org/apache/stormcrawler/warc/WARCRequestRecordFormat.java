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

package org.apache.stormcrawler.warc;

import static org.apache.stormcrawler.protocol.ProtocolResponse.REQUEST_HEADERS_KEY;
import static org.apache.stormcrawler.protocol.ProtocolResponse.RESPONSE_IP_KEY;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.regex.Pattern;
import org.apache.commons.lang3.StringUtils;
import org.apache.storm.tuple.Tuple;
import org.apache.stormcrawler.Metadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Generate a byte representation of a WARC request record from a tuple if the request HTTP headers
 * are present. The request record ID is stored in the metadata so that a WARC response record
 * (created later) can refer to it.
 */
public class WARCRequestRecordFormat extends WARCRecordFormat {

    private static final Logger LOG = LoggerFactory.getLogger(WARCRequestRecordFormat.class);

    protected static final Pattern REQUEST_LINE_PATTERN =
            Pattern.compile("^\\S+ \\S+ HTTP/1\\.[01]$");

    public WARCRequestRecordFormat(String protocolMDprefix) {
        super(protocolMDprefix);
    }

    @Override
    public byte[] format(Tuple tuple) {

        String url = tuple.getStringByField("url");
        Metadata metadata = (Metadata) tuple.getValueByField("metadata");

        String headersVerbatim = metadata.getFirstValue(REQUEST_HEADERS_KEY, this.protocolMDprefix);
        if (StringUtils.isBlank(headersVerbatim)) {
            // no request header: return empty record
            LOG.warn("No request header for {}", url);
            return new byte[] {};
        }
        final byte[] httpheaders = fixHttpHeaders(headersVerbatim).getBytes(StandardCharsets.UTF_8);

        StringBuilder buffer = new StringBuilder();
        buffer.append(WARC_VERSION);
        buffer.append(CRLF);
        buffer.append("WARC-Type: ").append(WARC_TYPE_REQUEST).append(CRLF);

        // "WARC-IP-Address" if present
        String IP = metadata.getFirstValue(RESPONSE_IP_KEY, this.protocolMDprefix);
        if (StringUtils.isNotBlank(IP)) {
            buffer.append("WARC-IP-Address: ").append(IP).append(CRLF);
        }

        addRecordID(buffer);

        /*
         * The request record ID is stored in the metadata so that a WARC
         * response record can later refer to it. Deactivated because of
         * https://github.com/apache/stormcrawler/issues/721
         */
        // metadata.setValue("_request.warc_record_id_", mainID);

        int contentLength = httpheaders.length;
        buffer.append("Content-Length: ").append(Integer.toString(contentLength)).append(CRLF);

        String blockDigest = getDigestSha1(httpheaders);

        String captureTime = getCaptureTime(metadata);
        buffer.append("WARC-Date: ").append(captureTime).append(CRLF);

        // must be a valid URI
        try {
            addTargetURI(buffer, url);
        } catch (Exception e) {
            LOG.warn("Incorrect URI: {}", url);
            return new byte[0];
        }

        buffer.append("Content-Type: application/http; msgtype=request").append(CRLF);
        buffer.append("WARC-Block-Digest: ").append(blockDigest).append(CRLF);

        byte[] buffasbytes = buffer.toString().getBytes(StandardCharsets.UTF_8);

        int capacity = 6 + buffasbytes.length + httpheaders.length;

        ByteBuffer bytebuffer = ByteBuffer.allocate(capacity);
        bytebuffer.put(buffasbytes);
        bytebuffer.put(CRLF_BYTES);
        bytebuffer.put(httpheaders);
        bytebuffer.put(CRLF_BYTES);
        bytebuffer.put(CRLF_BYTES);

        return bytebuffer.array();
    }

    private static String fixHttpHeaders(String headers) {
        int start = 0;
        int lineEnd = 0;
        int last = 0;
        int trailingCrLf = 0;
        final StringBuilder replacement = new StringBuilder();
        while (start < headers.length()) {
            boolean valid = true;
            lineEnd = headers.indexOf(CRLF, start);
            trailingCrLf = 1;
            if (lineEnd == -1) {
                lineEnd = headers.length();
                trailingCrLf = 0;
            }
            if (start == 0) {
                String requestLine = headers.substring(0, lineEnd);
                if (!REQUEST_LINE_PATTERN.matcher(requestLine).matches()) {
                    String[] parts = WS_PATTERN.split(requestLine, 3);
                    if (parts.length < 2) {
                        LOG.warn(
                                "WARC parsers may fail on non-standard HTTP 1.0 / 1.1 request line: {}",
                                requestLine);
                    } else if (parts.length < 3
                            || !HTTP_VERSION_PATTERN.matcher(parts[2]).matches()) {
                        LOG.info(requestLine);
                        // append HTTP version string accepted by most WARC parsers
                        replacement.append(parts[0]);
                        replacement.append(' ');
                        replacement.append(parts[1]); // status code
                        replacement.append(' ');
                        replacement.append(HTTP_VERSION_FALLBACK);
                        replacement.append(CRLF);
                        last = lineEnd + 2 * trailingCrLf;
                    }
                }
            } else if ((lineEnd + 4) == headers.length() && headers.endsWith(CRLF + CRLF)) {
                // ok, trailing empty line
                trailingCrLf = 2;
            } else if (start == lineEnd) {
                // skip/remove empty line
                valid = false;
            } else {
                int colonPos = -1;
                for (int i = start; i < lineEnd; i++) {
                    if (headers.charAt(i) == ':') {
                        colonPos = i;
                        break;
                    }
                }
                if (colonPos == -1) {
                    LOG.warn("Invalid header line: {}", headers.substring(start, lineEnd));
                    valid = false;
                }
                // no headers to replace in requests
            }
            if (!valid) {
                if (last < start) {
                    replacement.append(headers.substring(last, start));
                }
                last = lineEnd + 2 * trailingCrLf;
            }
            start = lineEnd + 2 * trailingCrLf;
        }
        if (last > 0 || trailingCrLf != 2) {
            if (last < headers.length()) {
                // append trailing headers
                replacement.append(headers.substring(last));
            }
            while (trailingCrLf < 2) {
                replacement.append(CRLF);
                trailingCrLf++;
            }
            return replacement.toString();
        }
        return headers;
    }
}
