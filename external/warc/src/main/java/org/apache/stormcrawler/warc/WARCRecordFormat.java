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

import static org.apache.stormcrawler.protocol.ProtocolResponse.REQUEST_TIME_KEY;
import static org.apache.stormcrawler.protocol.ProtocolResponse.RESPONSE_HEADERS_KEY;
import static org.apache.stormcrawler.protocol.ProtocolResponse.RESPONSE_IP_KEY;

import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.DateTimeException;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.regex.Pattern;
import org.apache.commons.codec.binary.Base32;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpHeaders;
import org.apache.storm.hdfs.bolt.format.RecordFormat;
import org.apache.storm.tuple.Tuple;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.protocol.ProtocolResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Generate a byte representation of a WARC entry from a tuple * */
public class WARCRecordFormat implements RecordFormat {

    // WARC record types, cf.
    // http://iipc.github.io/warc-specifications/specifications/warc-format/warc-1.1/#warc-record-types
    /** WARC record type to hold a HTTP request */
    protected static final String WARC_TYPE_REQUEST = "request";

    /** WARC record type to hold a HTTP response */
    protected static final String WARC_TYPE_RESPONSE = "response";

    /**
     * WARC record type to hold any other resource, including a HTTP response with no HTTP headers
     * available
     */
    protected static final String WARC_TYPE_RESOURCE = "resource";

    protected static final String WARC_TYPE_WARCINFO = "warcinfo";

    protected static final String WARC_TYPE_METADATA = "metadata";

    protected static final String WARC_VERSION = "WARC/1.0";
    protected static final String CRLF = "\r\n";
    protected static final byte[] CRLF_BYTES = {13, 10};

    private static final Logger LOG = LoggerFactory.getLogger(WARCRecordFormat.class);

    /**
     * Date formatter format the WARC-Date.
     *
     * <p>Note: to meet the WARC 1.0 standard the precision is in seconds.
     */
    public static final DateTimeFormatter WARC_DF =
            new DateTimeFormatterBuilder().appendInstant(0).toFormatter(Locale.ROOT);

    protected static final Pattern STATUS_LINE_PATTERN =
            Pattern.compile("^HTTP/1\\.[01] [0-9]{3}(?: .*)?$");
    protected static final Pattern WS_PATTERN = Pattern.compile("\\s+");
    protected static final Pattern HTTP_VERSION_PATTERN = Pattern.compile("^HTTP/1\\.[01]$");
    protected static final Pattern HTTP_STATUS_CODE_PATTERN = Pattern.compile("^[0-9]{3}$");
    protected static final String HTTP_VERSION_FALLBACK = "HTTP/1.1";

    protected static final Pattern PROBLEMATIC_HEADERS =
            Pattern.compile("(?i)(?:Content-(?:Encoding|Length)|Transfer-Encoding)");
    protected static final String X_HIDE_HEADER = "X-Crawler-";

    private static final Base32 base32 = new Base32();
    private static final String digestNoContent = getDigestSha1(new byte[0]);

    protected final String protocolMDprefix;

    public WARCRecordFormat(String protocolMDprefix) {
        this.protocolMDprefix = protocolMDprefix;
    }

    public static String getDigestSha1(byte[] bytes) {
        return "sha1:" + base32.encodeAsString(DigestUtils.sha1(bytes));
    }

    public static String getDigestSha1(byte[] bytes1, byte[] bytes2) {
        MessageDigest sha1 = DigestUtils.getSha1Digest();
        sha1.update(bytes1);
        return "sha1:" + base32.encodeAsString(sha1.digest(bytes2));
    }

    /** Generates a WARC info entry which can be stored at the beginning of each WARC file. */
    public static byte[] generateWARCInfo(Map<String, String> fields) {
        StringBuffer buffer = new StringBuffer();
        buffer.append(WARC_VERSION);
        buffer.append(CRLF);

        buffer.append("WARC-Type: ").append(WARC_TYPE_WARCINFO).append(CRLF);

        String mainID = UUID.randomUUID().toString();

        // retrieve the date and filename from the map
        String date = fields.get("WARC-Date");
        buffer.append("WARC-Date: ").append(date).append(CRLF);

        String filename = fields.get("WARC-Filename");
        buffer.append("WARC-Filename: ").append(filename).append(CRLF);

        buffer.append("WARC-Record-ID")
                .append(": ")
                .append("<urn:uuid:")
                .append(mainID)
                .append(">")
                .append(CRLF);

        buffer.append("Content-Type").append(": ").append("application/warc-fields").append(CRLF);

        StringBuilder fieldsBuffer = new StringBuilder();

        // add WARC fields
        // http://bibnum.bnf.fr/warc/WARC_ISO_28500_version1_latestdraft.pdf
        Iterator<Entry<String, String>> iter = fields.entrySet().iterator();
        while (iter.hasNext()) {
            Entry<String, String> entry = iter.next();
            String key = entry.getKey();
            if (key.startsWith("WARC-")) continue;
            fieldsBuffer.append(key).append(": ").append(entry.getValue()).append(CRLF);
        }

        buffer.append("Content-Length")
                .append(": ")
                .append(fieldsBuffer.toString().getBytes(StandardCharsets.UTF_8).length)
                .append(CRLF);

        buffer.append(CRLF);

        buffer.append(fieldsBuffer.toString());

        buffer.append(CRLF);
        buffer.append(CRLF);

        return buffer.toString().getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Modify verbatim HTTP response headers: remove or replace headers <code>Content-Length</code>,
     * <code>Content-Encoding</code> and <code>Transfer-Encoding</code> which may confuse WARC
     * readers. Ensure that the header end with a single empty line (<code>\r\n\r\n</code>).
     *
     * @param headers HTTP 1.1 or 1.0 response header string, CR-LF-separated lines, first line is
     *     status line
     * @return safe HTTP response header
     */
    public static String fixHttpHeaders(String headers, int contentLength) {
        int start = 0, lineEnd = 0, last = 0, trailingCrLf = 0;
        final StringBuilder replacement = new StringBuilder();
        while (start < headers.length()) {
            lineEnd = headers.indexOf(CRLF, start);
            trailingCrLf = 1;
            if (lineEnd == -1) {
                lineEnd = headers.length();
                trailingCrLf = 0;
            }
            int colonPos = -1;
            for (int i = start; i < lineEnd; i++) {
                if (headers.charAt(i) == ':') {
                    colonPos = i;
                    break;
                }
            }
            if (colonPos == -1) {
                boolean valid = true;
                if (start == 0) {
                    // status line (without colon)
                    final String statusLine = headers.substring(0, lineEnd);
                    if (!STATUS_LINE_PATTERN.matcher(statusLine).matches()) {
                        final String[] parts = WS_PATTERN.split(headers.substring(0, lineEnd), 3);
                        if (parts.length < 2
                                || !HTTP_STATUS_CODE_PATTERN.matcher(parts[1]).matches()) {
                            // nothing we can do here, leave status line as is
                            LOG.warn(
                                    "WARC parsers may fail on non-standard HTTP 1.0 / 1.1 response status line: {}",
                                    statusLine);
                        } else {
                            if (HTTP_VERSION_PATTERN.matcher(parts[0]).matches()) {
                                replacement.append(parts[0]);
                            } else {
                                replacement.append(HTTP_VERSION_FALLBACK);
                            }
                            replacement.append(' ');
                            replacement.append(parts[1]); // status code
                            replacement.append(' ');
                            if (parts.length == 3) {
                                replacement.append(parts[2]); // message
                            }
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
                    LOG.warn("Invalid header line: {}", headers.substring(start, lineEnd));
                    valid = false;
                }
                if (!valid) {
                    if (last < start) {
                        replacement.append(headers.substring(last, start));
                    }
                    last = lineEnd + 2 * trailingCrLf;
                }
                start = lineEnd + 2 * trailingCrLf;
                /*
                 * skip over invalid header line or status line, no further check for problematic
                 * headers required
                 */
                continue;
            }
            String name = headers.substring(start, colonPos);
            if (PROBLEMATIC_HEADERS.matcher(name).matches()) {
                boolean needsFix = true;
                if (name.equalsIgnoreCase("content-length")) {
                    String value = headers.substring(colonPos + 1, lineEnd).trim();
                    try {
                        int l = Integer.parseInt(value);
                        if (l == contentLength) {
                            needsFix = false;
                        }
                    } catch (NumberFormatException e) {
                        // needs to be fixed
                    }
                }
                if (needsFix) {
                    if (last < start) {
                        replacement.append(headers.substring(last, start));
                    }
                    last = lineEnd + 2 * trailingCrLf;
                    replacement
                            .append(X_HIDE_HEADER)
                            .append(headers.substring(start, lineEnd + 2 * trailingCrLf));
                    if (trailingCrLf == 0) {
                        replacement.append(CRLF);
                        trailingCrLf = 1;
                    }
                    if (name.equalsIgnoreCase("content-length")) {
                        // add effective uncompressed and unchunked length of
                        // content
                        replacement
                                .append("Content-Length")
                                .append(": ")
                                .append(contentLength)
                                .append(CRLF);
                    }
                }
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

    /**
     * Get the actual fetch time from metadata and format it as required by the WARC-Date field. If
     * no fetch time is found in metadata (key {@link
     * org.apache.stormcrawler.protocol.ProtocolResponse#REQUEST_TIME_KEY REQUEST_TIME_KEY}), the
     * current time is taken.
     */
    protected String getCaptureTime(Metadata metadata) {
        String captureTimeMillis = metadata.getFirstValue(REQUEST_TIME_KEY, this.protocolMDprefix);
        Instant capturedAt = Instant.now();
        if (captureTimeMillis != null) {
            try {
                long millis = Long.parseLong(captureTimeMillis);
                capturedAt = Instant.ofEpochMilli(millis);
            } catch (NumberFormatException | DateTimeException e) {
                LOG.warn("Failed to parse capture time:", e);
            }
        }
        return WARC_DF.format(capturedAt);
    }

    @Override
    public byte[] format(Tuple tuple) {

        byte[] content = tuple.getBinaryByField("content");
        String url = tuple.getStringByField("url");
        Metadata metadata = (Metadata) tuple.getValueByField("metadata");

        // if HTTP headers have been stored verbatim,
        // generate a "response" record, otherwise a "resource" record
        String WARCTypeValue = WARC_TYPE_RESOURCE;

        // were the headers stored as is? Can write a response element then
        String headersVerbatim =
                metadata.getFirstValue(RESPONSE_HEADERS_KEY, this.protocolMDprefix);
        byte[] httpheaders = new byte[0];
        if (StringUtils.isNotBlank(headersVerbatim)) {
            WARCTypeValue = WARC_TYPE_RESPONSE;
            headersVerbatim = fixHttpHeaders(headersVerbatim, content.length);
            httpheaders = headersVerbatim.getBytes(StandardCharsets.UTF_8);
        }

        StringBuilder buffer = new StringBuilder();
        buffer.append(WARC_VERSION);
        buffer.append(CRLF);

        addRecordID(buffer);

        String warcRequestId = metadata.getFirstValue("_request.warc_record_id_");
        if (warcRequestId != null) {
            buffer.append("WARC-Concurrent-To")
                    .append(": ")
                    .append("<urn:uuid:")
                    .append(warcRequestId)
                    .append(">")
                    .append(CRLF);
        }

        int contentLength = 0;
        String payloadDigest = digestNoContent;
        String blockDigest = digestNoContent;
        if (content != null) {
            contentLength = content.length;
            payloadDigest = getDigestSha1(content);
            if (WARCTypeValue.equals(WARC_TYPE_RESPONSE)) {
                blockDigest = getDigestSha1(httpheaders, content);
            } else {
                blockDigest = payloadDigest;
            }
        } else if (WARCTypeValue.equals(WARC_TYPE_RESPONSE)) {
            blockDigest = getDigestSha1(httpheaders);
        }

        // add the length of the http header
        contentLength += httpheaders.length;

        buffer.append("Content-Length")
                .append(": ")
                .append(Integer.toString(contentLength))
                .append(CRLF);

        String captureTime = getCaptureTime(metadata);
        buffer.append("WARC-Date").append(": ").append(captureTime).append(CRLF);

        buffer.append("WARC-Type").append(": ").append(WARCTypeValue).append(CRLF);

        // "WARC-IP-Address" if present
        String IP = metadata.getFirstValue(RESPONSE_IP_KEY, this.protocolMDprefix);
        if (StringUtils.isNotBlank(IP)) {
            buffer.append("WARC-IP-Address").append(": ").append(IP).append(CRLF);
        }

        // must be a valid URI
        try {
            addTargetURI(buffer, url);
        } catch (Exception e) {
            LOG.warn("Incorrect URI: {}", url);
            return new byte[0];
        }

        // provide a ContentType if type response
        if (WARCTypeValue.equals(WARC_TYPE_RESPONSE)) {
            buffer.append("Content-Type: application/http; msgtype=response").append(CRLF);
        }
        // for resources just use the content type provided by the server if any
        else {
            String ct = metadata.getFirstValue(HttpHeaders.CONTENT_TYPE, this.protocolMDprefix);
            if (StringUtils.isBlank(ct)) {
                ct = "application/octet-stream";
            }
            buffer.append("Content-Type: ").append(ct).append(CRLF);
        }

        String truncated =
                metadata.getFirstValue(
                        ProtocolResponse.TRIMMED_RESPONSE_KEY, this.protocolMDprefix);
        if (truncated != null) {
            // content is truncated
            truncated =
                    metadata.getFirstValue(
                            ProtocolResponse.TRIMMED_RESPONSE_REASON_KEY, this.protocolMDprefix);
            if (truncated == null) {
                truncated =
                        ProtocolResponse.TrimmedContentReason.UNSPECIFIED
                                .toString()
                                .toLowerCase(Locale.ROOT);
            }
            buffer.append("WARC-Truncated").append(": ").append(truncated).append(CRLF);
        }
        final String protocolVersions =
                metadata.getFirstValue(
                        ProtocolResponse.PROTOCOL_VERSIONS_KEY, this.protocolMDprefix);
        if (protocolVersions != null) {
            buffer.append("WARC-Protocol: ").append(protocolVersions).append(CRLF);
        }

        buffer.append("WARC-Payload-Digest").append(": ").append(payloadDigest).append(CRLF);
        buffer.append("WARC-Block-Digest").append(": ").append(blockDigest).append(CRLF);

        byte[] buffasbytes = buffer.toString().getBytes(StandardCharsets.UTF_8);

        // work out the *exact* length of the bytebuffer - do not add any extra
        // bytes which are appended as trailing zero bytes causing invalid WARC
        // files
        int capacity = 6 + buffasbytes.length + httpheaders.length;
        if (content != null) {
            capacity += content.length;
        }

        ByteBuffer bytebuffer = ByteBuffer.allocate(capacity);
        bytebuffer.put(buffasbytes);
        bytebuffer.put(CRLF_BYTES);
        bytebuffer.put(httpheaders);
        // the binary content itself
        if (content != null) {
            bytebuffer.put(content);
        }
        bytebuffer.put(CRLF_BYTES);
        bytebuffer.put(CRLF_BYTES);

        return bytebuffer.array();
    }

    static void addTargetURI(StringBuilder builder, String url) {
        String normalised = url.replace(" ", "%20");
        String targetURI = URI.create(normalised).toASCIIString();
        builder.append("WARC-Target-URI").append(": ").append(targetURI).append(CRLF);
    }

    static void addRecordID(StringBuilder builder) {
        builder.append("WARC-Record-ID")
                .append(": ")
                .append("<urn:uuid:")
                .append(UUID.randomUUID().toString())
                .append(">")
                .append(CRLF);
    }
}
