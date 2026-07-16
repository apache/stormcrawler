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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import org.apache.http.HttpHeaders;
import org.apache.storm.tuple.Tuple;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.protocol.ProtocolResponse;
import org.junit.jupiter.api.Test;
import org.netpreserve.jwarc.WarcMetadata;
import org.netpreserve.jwarc.WarcReader;
import org.netpreserve.jwarc.WarcRecord;

class WARCRecordFormatTest {

    private static String protocolMDprefix = "http.";

    private static String[][] ipAddressTestData = {
        // { expected, input }
        // IPv4
        {"127.0.0.1", "127.0.0.1"},
        // IPv6
        {
            // IPv4-mapped IPv6 addresses
            "192.0.2.128", "::ffff:192.0.2.128"
        },
        {
            // IPv4-mapped IPv6 addresses (hex)
            "192.0.2.128", "::ffff:C000:0280"
        },
        // Tests below are from jwarc's InetAddressesTest
        {"2001:db8::1", "2001:db8:0:0:0:0:0:1"},
        {"2001:db8::1", "2001:0DB8:0:0:0:0:0:1"},
        {"::", "0:0:0:0:0:0:0:0"},
        {"::1", "0:0:0:0:0:0:0:1"},
        {"2001:db8:1:1:1:1:1:1", "2001:db8:1:1:1:1:1:1"},
        {"2001:0:0:1::1", "2001:0:0:1:0:0:0:1"},
        {"2001:db8:f::1", "2001:db8:000f:0:0:0:0:1"},
        {"2001:db8::1:0:0:1", "2001:0db8:0000:0000:0001:0000:0000:0001"},
        {"ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff", "ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff"},
        {"2001:200f::1", "2001:200f:0:0:0:0:0:1"},
        {
            // https://datatracker.ietf.org/doc/html/rfc5952#section-4.2.2
            // "The symbol "::" MUST NOT be used to shorten just one 16-bit 0 field."
            "2001:0:3:4:5:6:7:8", "2001:0:3:4:5:6:7:8"
        },
        {
            // shorten first of same-length consecutive 0 fields, also in initial position
            "::4:0:0:0:ffff", "0:0:0:4:0:0:0:ffff"
        },
    };

    @Test
    void testGetDigestSha1() {
        byte[] content = {'a', 'b', 'c', 'd', 'e', 'f'};
        String sha1str = "sha1:D6FMCDZDYW23YELHXWUEXAZ6LQCXU56S";
        assertEquals(sha1str, WARCRecordFormat.getDigestSha1(content), "Wrong sha1 digest");
    }

    @Test
    void testGetDigestSha1Empty() {
        byte[] content = {};
        String sha1str = "sha1:3I42H3S6NNFQ2MSVX7XZKYAYSCX5QBYJ";
        assertEquals(sha1str, WARCRecordFormat.getDigestSha1(content), "Wrong sha1 digest");
    }

    @Test
    void testGetDigestSha1TwoByteArrays() {
        byte[] content1 = {'a', 'b', 'c'};
        byte[] content2 = {'d', 'e', 'f'};
        String sha1str = "sha1:D6FMCDZDYW23YELHXWUEXAZ6LQCXU56S";
        assertEquals(
                sha1str, WARCRecordFormat.getDigestSha1(content1, content2), "Wrong sha1 digest");
    }

    @Test
    void testGetDigestSha1RobotsTxt() {
        // trivial robots.txt file, sha1 from WARC file written by Nutch
        String robotsTxt = "User-agent: *\r\nDisallow:";
        byte[] content = robotsTxt.getBytes(StandardCharsets.UTF_8);
        String sha1str = "sha1:DHBVNHAJABWFHIYUHNCKYYIB3OBPFX3Y";
        assertEquals(sha1str, WARCRecordFormat.getDigestSha1(content), "Wrong sha1 digest");
    }

    @Test
    void testCanonicalizeIpAddress() {
        Metadata metadata = new Metadata();
        for (String[] p : ipAddressTestData) {
            metadata.setValue(protocolMDprefix + ProtocolResponse.RESPONSE_IP_KEY, p[1]);
            Optional<String> ip = WARCRecordFormat.getIPAddress(metadata, protocolMDprefix);
            if (p[0] == null) {
                assertTrue(ip.isEmpty());
            } else {
                assertTrue(ip.isPresent(), "No canonical form for " + p[1]);
                assertEquals(p[0], ip.get());
            }
        }
    }

    @Test
    void testWarcRecord() {
        // test validity of WARC record
        String txt = "abcdef";
        byte[] content = txt.getBytes(StandardCharsets.UTF_8);
        String sha1str = "sha1:D6FMCDZDYW23YELHXWUEXAZ6LQCXU56S";
        Metadata metadata = new Metadata();
        metadata.addValue(
                protocolMDprefix + ProtocolResponse.RESPONSE_HEADERS_KEY,
                "HTTP/1.1 200 OK\r\nContent-Type: text/html\r\n");
        metadata.addValue(
                protocolMDprefix + ProtocolResponse.RESPONSE_IP_KEY, "2001:0DB8:0:0:0:0:0:1");
        Tuple tuple = mock(Tuple.class);
        when(tuple.getBinaryByField("content")).thenReturn(content);
        when(tuple.getStringByField("url")).thenReturn("https://www.example.org/");
        when(tuple.getValueByField("metadata")).thenReturn(metadata);
        WARCRecordFormat format = new WARCRecordFormat(protocolMDprefix);
        byte[] warcBytes = format.format(tuple);
        String warcString = new String(warcBytes, StandardCharsets.UTF_8);
        // the WARC record has the form:
        // WARC header
        // \r\n\r\n
        // HTTP header
        // \r\n\r\n
        // payload
        // \r\n\r\n
        assertTrue(
                warcString.startsWith("WARC/1.0"), "WARC record: incorrect format of WARC header");
        assertTrue(
                warcString.contains("\r\n\r\nHTTP/1.1 200 OK\r\n"),
                "WARC record: incorrect format of HTTP header");
        assertTrue(
                warcString.contains("Content-Type: text/html\r\n\r\nabcdef"),
                "WARC record: single empty line between HTTP header and payload");
        assertTrue(
                warcString.endsWith("\r\n\r\n"),
                "WARC record: record is required to end with \\r\\n\\r\\n");
        assertTrue(warcString.endsWith("\r\n\r\nabcdef\r\n\r\n"), "WARC record: payload mangled");
        assertTrue(
                warcString.contains("\r\nWARC-Payload-Digest: " + sha1str + "\r\n"),
                "WARC record: no or incorrect payload digest");
        assertTrue(
                warcString.contains("\r\nWARC-IP-Address: 2001:db8::1\r\n"),
                "WARC record: WARC-IP-Address not found (canonical form expected)");
    }

    @Test
    void testReplaceHeaders() {
        // test whether wrong/misleading HTTP headers are replaced
        // because payload is not saved in original Content-Encoding and
        // Transfer-Encoding
        String txt = "abcdef";
        byte[] content = txt.getBytes(StandardCharsets.UTF_8);
        String sha1str = "sha1:D6FMCDZDYW23YELHXWUEXAZ6LQCXU56S";
        Metadata metadata = new Metadata();
        metadata.addValue(
                protocolMDprefix + ProtocolResponse.RESPONSE_HEADERS_KEY,
                "HTTP/1.1 200 OK\r\n"
                        + "Content-Type: text/html\r\n"
                        + "Content-Encoding: gzip\r\n"
                        + "Content-Length: 26\r\n"
                        + "Connection: close");
        metadata.addValue(protocolMDprefix + ProtocolResponse.RESPONSE_IP_KEY, "123.123.123.123");
        Tuple tuple = mock(Tuple.class);
        when(tuple.getBinaryByField("content")).thenReturn(content);
        when(tuple.getStringByField("url")).thenReturn("https://www.example.org/");
        when(tuple.getValueByField("metadata")).thenReturn(metadata);
        WARCRecordFormat format = new WARCRecordFormat(protocolMDprefix);
        byte[] warcBytes = format.format(tuple);
        String warcString = new String(warcBytes, StandardCharsets.UTF_8);
        assertFalse(
                warcString.contains("\r\nContent-Encoding: gzip\r\n"),
                "WARC record: HTTP header Content-Encoding not replaced");
        assertFalse(
                warcString.contains("\r\nContent-Length: 26\r\n"),
                "WARC record: HTTP header Content-Length not replaced");
        // the correct Content-Length is 6 (txt = "abcdef")
        assertTrue(
                warcString.contains("\r\nContent-Length: 6\r\n"),
                "WARC record: HTTP header Content-Length does not match payload length");
        assertTrue(
                warcString.contains("\r\nConnection: close\r\n\r\nabcdef"),
                "WARC record: HTTP header does not end with \\r\\n\\r\\n");
    }

    @Test
    void testReplaceHttpVersion() {
        /*
         * Some WARC readers only accept "HTTP/1.0" or "HTTP/1.1" as HTTP protocol identifier in
         * HTTP request and status lines. Any other protocol versions should be replaced by one of
         * the mentioned commonly accepted protocol version strings.
         */
        String txt = "abcdef";
        byte[] content = txt.getBytes(StandardCharsets.UTF_8);
        Metadata metadata = new Metadata();
        metadata.addValue(
                protocolMDprefix + ProtocolResponse.RESPONSE_HEADERS_KEY,
                "HTTP/2 200 OK\r\n"
                        + "Content-Type: text/html\r\n"
                        + "Content-Encoding: gzip\r\n"
                        + "Content-Length: 26\r\n"
                        + "Connection: close");
        metadata.addValue(
                protocolMDprefix + ProtocolResponse.PROTOCOL_VERSIONS_KEY,
                "h2,TLS_1_3,TLS_AES_256_GCM_SHA384");
        metadata.addValue(protocolMDprefix + ProtocolResponse.RESPONSE_IP_KEY, "123.123.123.123");
        Tuple tuple = mock(Tuple.class);
        when(tuple.getBinaryByField("content")).thenReturn(content);
        when(tuple.getStringByField("url")).thenReturn("https://www.example.org/");
        when(tuple.getValueByField("metadata")).thenReturn(metadata);
        WARCRecordFormat format = new WARCRecordFormat(protocolMDprefix);
        byte[] warcBytes = format.format(tuple);
        String warcString = new String(warcBytes, StandardCharsets.UTF_8);
        String[] headersPayload = warcString.split("\r\n\r\n");
        assertEquals(
                3,
                headersPayload.length,
                "WARC response record must include WARC header, HTTP header and payload");
        String statusLine = headersPayload[1].split("\r\n", 2)[0];
        assertTrue(
                statusLine.matches("^HTTP/1\\.[01] .*"),
                "WARC response record: HTTP status line must start with HTTP/1.1 or HTTP/1.0");
        assertTrue(
                headersPayload[0].contains("\r\nWARC-Protocol: "),
                "WARC response record is expected to include WARC header \"WARC-Protocol\"");
        assertTrue(
                headersPayload[0].contains("\r\nWARC-IP-Address: "),
                "WARC response record is expected to include WARC header \"WARC-IP-Address\"");
    }

    @Test
    void testRequestHeader() {
        String txt = "abcdef";
        byte[] content = txt.getBytes(StandardCharsets.UTF_8);
        Metadata metadata = new Metadata();
        metadata.addValue(
                protocolMDprefix + ProtocolResponse.REQUEST_HEADERS_KEY,
                "GET / HTTP/2\r\n"
                        + "User-Agent: mybot\r\n"
                        + "Accept: text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8\r\n"
                        + "Accept-Language: en-us,en-gb,en;q=0.7,*;q=0.3\r\n"
                        + "Accept-Encoding: br,gzip\r\n"
                        + "Connection: Keep-Alive\r\n\r\n");
        metadata.addValue(protocolMDprefix + ProtocolResponse.RESPONSE_IP_KEY, "123.123.123.123");
        Tuple tuple = mock(Tuple.class);
        when(tuple.getBinaryByField("content")).thenReturn(content);
        when(tuple.getStringByField("url")).thenReturn("https://www.example.org/");
        when(tuple.getValueByField("metadata")).thenReturn(metadata);
        WARCRecordFormat format = new WARCRequestRecordFormat(protocolMDprefix);
        byte[] warcBytes = format.format(tuple);
        String warcString = new String(warcBytes, StandardCharsets.UTF_8);
        String[] headers = warcString.split("\r\n\r\n");
        assertEquals(2, headers.length, "WARC request record must include WARC and HTTP header");
        String requestLine = headers[1].split("\r\n", 2)[0];
        assertTrue(
                requestLine.matches(".* HTTP/1\\.[01]$"),
                "WARC request record: HTTP request line must end with HTTP/1.1 or HTTP/1.0");
    }

    @Test
    void testWarcDateFormat() {
        Metadata metadata = new Metadata();
        /*
         * To meet the WARC 1.0 standard the ISO date format with seconds
         * precision (not milliseconds) is expected. We pass epoch millisecond 1
         * to the formatter to ensure that the precision isn't increased on
         * demand (as by the ISO_INSTANT formatter):
         */
        metadata.addValue(protocolMDprefix + ProtocolResponse.REQUEST_TIME_KEY, "1");
        WARCRecordFormat format = new WARCRecordFormat(protocolMDprefix);
        assertEquals("1970-01-01T00:00:00Z", format.getCaptureTime(metadata));
    }

    @Test
    void testWarcResourceRecord() {
        // test writing of WARC resource record (no verbatim HTTP headers stored,
        // `http.store.headers: false`)
        String txt = "abcdef";
        byte[] content = txt.getBytes(StandardCharsets.UTF_8);
        String sha1str = "sha1:D6FMCDZDYW23YELHXWUEXAZ6LQCXU56S";
        Metadata metadata = new Metadata();
        metadata.addValue(protocolMDprefix + HttpHeaders.CONTENT_TYPE, "text/html");
        Tuple tuple = mock(Tuple.class);
        when(tuple.getBinaryByField("content")).thenReturn(content);
        when(tuple.getStringByField("url")).thenReturn("https://www.example.org/");
        when(tuple.getValueByField("metadata")).thenReturn(metadata);
        WARCRecordFormat format = new WARCRecordFormat(protocolMDprefix);
        byte[] warcBytes = format.format(tuple);
        String warcString = new String(warcBytes, StandardCharsets.UTF_8);
        assertTrue(
                warcString.startsWith("WARC/1.0"), "WARC record: incorrect format of WARC header");
        assertTrue(
                warcString.endsWith("\r\n\r\n"),
                "WARC record: record is required to end with \\r\\n\\r\\n");
        assertTrue(
                warcString.contains("WARC-Type: resource\r\n"),
                "WARC record: record type must be \"resource\"");
        assertTrue(
                warcString.contains("Content-Type: text/html\r\n"),
                "WARC record: Content-Type should be \"text/html\"");
        assertTrue(
                warcString.contains("\r\nWARC-Payload-Digest: " + sha1str + "\r\n"),
                "WARC record: no or incorrect payload digest");
        assertTrue(
                warcString.contains("\r\nWARC-Block-Digest: " + sha1str + "\r\n"),
                "WARC record: no or incorrect block, digest");
    }

    @Test
    void testWarcMetadataRecord() {
        Metadata metadata = new Metadata();
        metadata.addValue("source", "a source");
        metadata.addValues("another", List.of("several", "values"));
        Tuple tuple = mock(Tuple.class);
        when(tuple.getStringByField("url")).thenReturn("https://www.example.org/");
        when(tuple.getValueByField("metadata")).thenReturn(metadata);

        MetadataRecordFormat format = new MetadataRecordFormat(List.of("source", "another"));
        byte[] warcBytes = format.format(tuple);
        String warcString = new String(warcBytes, StandardCharsets.UTF_8);
        assertTrue(
                warcString.endsWith("\r\n\r\n"),
                "WARC record: record is required to end with \\r\\n\\r\\n");
        assertTrue(
                warcString.contains("WARC-Type: metadata\r\n"),
                "WARC record: record type must be \"metadata\"");
        assertTrue(
                warcString.contains("\r\nsource: a source\r\n"), "WARC record: missing metadata");
        assertTrue(
                warcString.contains("\r\nanother: several\r\n"), "WARC record: missing metadata");
        assertTrue(warcString.contains("\r\nanother: values\r\n"), "WARC record: missing metadata");

        // try to read it with Jwarc
        try (WarcReader reader = new WarcReader(new ByteArrayInputStream(warcBytes))) {
            for (WarcRecord record : reader) {
                assertTrue(record instanceof WarcMetadata, "Can't parse as WARCMetadata");
                WarcMetadata wmd = (WarcMetadata) record;
                org.netpreserve.jwarc.MessageHeaders fields = wmd.fields();
                assertTrue(fields.contains("source", "a source"));
                assertTrue(fields.contains("another", "several"));
                assertTrue(fields.contains("another", "values"));
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
