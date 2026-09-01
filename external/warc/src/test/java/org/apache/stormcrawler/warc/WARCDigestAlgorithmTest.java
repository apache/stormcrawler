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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import org.apache.commons.codec.binary.Base32;
import org.apache.commons.lang3.StringUtils;
import org.apache.storm.tuple.Tuple;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.protocol.ProtocolResponse;
import org.junit.jupiter.api.Test;

/**
 * Tests that the algorithm used for the WARC-Payload-Digest and WARC-Block-Digest fields is
 * configurable with {@link WARCRecordFormat#DIGEST_ALGORITHM_PARAM} and that SHA-1 remains the
 * default.
 */
class WARCDigestAlgorithmTest {

    private static final String URL = "https://www.example.org/";

    private static final byte[] CONTENT = "abcdef".getBytes(StandardCharsets.UTF_8);

    private static final String SHA1_ABCDEF = "sha1:D6FMCDZDYW23YELHXWUEXAZ6LQCXU56S";

    /*
     * The Base32 padding is omitted: the WARC digest fields define the digest value as a token,
     * which does not allow the padding character "=" (cf. ISO 28500 WARC 1.1).
     */
    private static final String SHA256_ABCDEF =
            "sha256:X32X5R7VHJWUBPVWICTYBJRZZA54FGWIVGAW6H6GYXDNZWJ4I4QQ";

    private static final String SHA256_EMPTY =
            "sha256:4OYMIQUY7QOBJGX36TEJS35ZEQT24QPEMSNZGTFESWMRW6CSXBKQ";

    /** Compute the expected digest independently of the code under test. */
    private static String expectedDigest(String jcaAlgorithm, String prefix, byte[]... byteArrays) {
        try {
            MessageDigest md = MessageDigest.getInstance(jcaAlgorithm);
            for (byte[] bytes : byteArrays) {
                md.update(bytes);
            }
            return prefix + StringUtils.stripEnd(new Base32().encodeAsString(md.digest()), "=");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * The bytes covered by the WARC-Block-Digest: everything between the end of the WARC header and
     * the final CRLF CRLF.
     */
    private static byte[] recordBlock(String warcString) {
        int start = warcString.indexOf("\r\n\r\n") + 4;
        return warcString
                .substring(start, warcString.length() - 4)
                .getBytes(StandardCharsets.UTF_8);
    }

    private static Tuple tupleWithContent(Metadata metadata) {
        Tuple tuple = mock(Tuple.class);
        when(tuple.getBinaryByField("content")).thenReturn(CONTENT);
        when(tuple.getStringByField("url")).thenReturn(URL);
        when(tuple.getValueByField("metadata")).thenReturn(metadata);
        return tuple;
    }

    @Test
    void testDigestDefaultsToSha1() {
        assertEquals(
                SHA1_ABCDEF,
                new WARCRecordFormat("").getDigest(CONTENT),
                "digest algorithm must default to SHA-1");
        // a null algorithm must be treated as the default
        assertEquals(SHA1_ABCDEF, new WARCRecordFormat("", null).getDigest(CONTENT));
    }

    @Test
    void testGetDigestSha256() {
        WARCRecordFormat format =
                new WARCRecordFormat("", WARCRecordFormat.DIGEST_ALGORITHM_SHA256);
        assertEquals(SHA256_ABCDEF, format.getDigest(CONTENT), "Wrong sha256 digest");
        assertEquals(SHA256_EMPTY, format.getDigest(new byte[0]), "Wrong sha256 digest");
    }

    @Test
    void testDigestValueContainsNoBase32Padding() {
        // the digest value is a token per the WARC 1.1 grammar and must not contain "="
        String sha256 =
                new WARCRecordFormat("", WARCRecordFormat.DIGEST_ALGORITHM_SHA256)
                        .getDigest(CONTENT);
        assertFalse(sha256.contains("="), "digest value must not contain Base32 padding");
        String sha1 = new WARCRecordFormat("").getDigest(CONTENT);
        assertFalse(sha1.contains("="), "digest value must not contain Base32 padding");
    }

    @Test
    void testGetDigestSha256TwoByteArrays() {
        WARCRecordFormat format =
                new WARCRecordFormat("", WARCRecordFormat.DIGEST_ALGORITHM_SHA256);
        byte[] content1 = "abc".getBytes(StandardCharsets.UTF_8);
        byte[] content2 = "def".getBytes(StandardCharsets.UTF_8);
        assertEquals(
                SHA256_ABCDEF,
                format.getDigest(content1, content2),
                "Wrong sha256 digest over concatenated byte arrays");
    }

    @Test
    void testDigestAlgorithmValueVariants() {
        // the value is matched case-insensitively, an optional hyphen is ignored and
        // surrounding whitespace is trimmed
        assertEquals(SHA256_ABCDEF, new WARCRecordFormat("", "SHA256").getDigest(CONTENT));
        assertEquals(SHA256_ABCDEF, new WARCRecordFormat("", "SHA-256").getDigest(CONTENT));
        assertEquals(SHA256_ABCDEF, new WARCRecordFormat("", " sha256 ").getDigest(CONTENT));
        assertEquals(SHA1_ABCDEF, new WARCRecordFormat("", "SHA-1").getDigest(CONTENT));
    }

    @Test
    void testUnsupportedDigestAlgorithm() {
        assertThrows(IllegalArgumentException.class, () -> new WARCRecordFormat("", "md5"));
        assertThrows(IllegalArgumentException.class, () -> new WARCRecordFormat("", "sha512"));
        assertThrows(
                IllegalArgumentException.class,
                () -> new MetadataRecordFormat(List.of("source"), "md5"));
        assertThrows(IllegalArgumentException.class, () -> new WARCRequestRecordFormat("", "md5"));
    }

    @Test
    void testResponseRecordDigestsSha256() {
        Metadata metadata = new Metadata();
        metadata.addValue(
                "protocol." + ProtocolResponse.RESPONSE_HEADERS_KEY,
                "HTTP/1.1 200 OK\r\nContent-Type: text/html\r\n");
        Tuple tuple = tupleWithContent(metadata);
        WARCRecordFormat format =
                new WARCRecordFormat("protocol.", WARCRecordFormat.DIGEST_ALGORITHM_SHA256);
        String warcString = new String(format.format(tuple), StandardCharsets.UTF_8);

        assertTrue(
                warcString.contains("\r\nWARC-Payload-Digest: " + SHA256_ABCDEF + "\r\n"),
                "WARC response record: payload digest must be SHA-256");
        String expectedBlockDigest = expectedDigest("SHA-256", "sha256:", recordBlock(warcString));
        assertTrue(
                warcString.contains("\r\nWARC-Block-Digest: " + expectedBlockDigest + "\r\n"),
                "WARC response record: block digest must be SHA-256 over HTTP headers and payload");
    }

    @Test
    void testResourceRecordDigestsSha256() {
        // no verbatim HTTP headers stored -> resource record, block digest equals payload digest
        Metadata metadata = new Metadata();
        Tuple tuple = tupleWithContent(metadata);
        WARCRecordFormat format =
                new WARCRecordFormat("", WARCRecordFormat.DIGEST_ALGORITHM_SHA256);
        String warcString = new String(format.format(tuple), StandardCharsets.UTF_8);
        assertTrue(warcString.contains("\r\nWARC-Type: resource\r\n"));
        assertTrue(
                warcString.contains("\r\nWARC-Payload-Digest: " + SHA256_ABCDEF + "\r\n"),
                "WARC resource record: payload digest must be SHA-256");
        assertTrue(
                warcString.contains("\r\nWARC-Block-Digest: " + SHA256_ABCDEF + "\r\n"),
                "WARC resource record: block digest must be SHA-256");
    }

    @Test
    void testRequestRecordBlockDigestSha256() {
        Metadata metadata = new Metadata();
        metadata.addValue(
                "protocol." + ProtocolResponse.REQUEST_HEADERS_KEY,
                "GET / HTTP/2\r\nUser-Agent: mybot\r\nConnection: Keep-Alive\r\n\r\n");
        Tuple tuple = mock(Tuple.class);
        when(tuple.getStringByField("url")).thenReturn(URL);
        when(tuple.getValueByField("metadata")).thenReturn(metadata);
        WARCRequestRecordFormat format =
                new WARCRequestRecordFormat("protocol.", WARCRecordFormat.DIGEST_ALGORITHM_SHA256);
        String warcString = new String(format.format(tuple), StandardCharsets.UTF_8);

        String expectedBlockDigest = expectedDigest("SHA-256", "sha256:", recordBlock(warcString));
        assertTrue(
                warcString.contains("\r\nWARC-Block-Digest: " + expectedBlockDigest + "\r\n"),
                "WARC request record: block digest must be SHA-256 over the request headers");
    }

    @Test
    void testMetadataRecordBlockDigestSha256() {
        Metadata metadata = new Metadata();
        metadata.addValue("source", "a source");
        Tuple tuple = mock(Tuple.class);
        when(tuple.getStringByField("url")).thenReturn(URL);
        when(tuple.getValueByField("metadata")).thenReturn(metadata);
        MetadataRecordFormat format =
                new MetadataRecordFormat(
                        List.of("source"), WARCRecordFormat.DIGEST_ALGORITHM_SHA256);
        String warcString = new String(format.format(tuple), StandardCharsets.UTF_8);

        // the payload of the metadata record are the metadata fields themselves
        byte[] payload = "source: a source\r\n".getBytes(StandardCharsets.UTF_8);
        String expectedBlockDigest = expectedDigest("SHA-256", "sha256:", payload);
        assertTrue(
                warcString.contains("\r\nWARC-Block-Digest: " + expectedBlockDigest + "\r\n"),
                "WARC metadata record: block digest must be SHA-256 over the metadata payload");
    }
}
