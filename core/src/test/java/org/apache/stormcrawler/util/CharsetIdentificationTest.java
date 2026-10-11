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

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.stormcrawler.Metadata;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class CharsetIdentificationTest {

    /** detect.charset.maxlength as set in crawler-default.yaml. */
    private static final int MAXLENGTH = 10000;

    /**
     * META_CHARSET_LOOKAHEAD in CharsetIdentification: bytes read past the window for the quote.
     */
    private static final int LOOKAHEAD = 64;

    /** A body which opens a meta charset declaration and never closes it. */
    private static byte[] unterminatedMetaCharset(int size) {
        byte[] content = new byte[size];
        Arrays.fill(content, (byte) 'A');
        byte[] prefix = "<meta charset=\"".getBytes(StandardCharsets.US_ASCII);
        System.arraycopy(prefix, 0, content, 0, prefix.length);
        return content;
    }

    /**
     * Runs the detection on a thread with a fixed stack so that the outcome does not depend on the
     * JVM defaults, and returns what it threw, if anything.
     */
    private static Throwable detectOnSmallStack(byte[] content, int maxlength)
            throws InterruptedException {
        AtomicReference<Throwable> thrown = new AtomicReference<>();
        Thread thread =
                new Thread(
                        null,
                        () -> {
                            try {
                                CharsetIdentification.getCharset(
                                        new Metadata(), content, maxlength);
                            } catch (Throwable t) {
                                thrown.set(t);
                            }
                        },
                        "charset-detection",
                        1024 * 1024);
        thread.start();
        thread.join();
        return thrown.get();
    }

    @Test
    void largeDocumentWithUnterminatedMetaCharsetDoesNotOverflowTheStack() throws Exception {
        // 400 KB is an ordinary page size, fetched whole under the default http.content.limit
        Throwable thrown = detectOnSmallStack(unterminatedMetaCharset(400_000), MAXLENGTH);
        Assertions.assertNull(thrown, "charset detection threw " + thrown);
    }

    @Test
    void smallDocumentWithUnterminatedMetaCharsetIsHandled() throws Exception {
        Throwable thrown = detectOnSmallStack(unterminatedMetaCharset(20_000), MAXLENGTH);
        Assertions.assertNull(thrown, "charset detection threw " + thrown);
    }

    @Test
    void unterminatedMetaCharsetIsHandledWithFullContentDetection() throws Exception {
        Throwable thrown = detectOnSmallStack(unterminatedMetaCharset(400_000), -1);
        Assertions.assertNull(thrown, "charset detection threw " + thrown);
    }

    /** ASCII content, no BOM and no HTTP header: only the meta tag can yield this charset. */
    private static final String DECLARED = "windows-1251";

    /** More than the look-ahead of body behind the tag, so that the look-ahead bounds the read. */
    private static final String BODY =
            "</head><body>" + "<p>text</p>".repeat(20) + "</body></html>";

    /** A declaration cut by the detection window is still read, see #870. */
    @Test
    void metaCharsetCutByTheDetectionWindowIsStillRead() {
        StringBuilder page = new StringBuilder("<html><head>");
        while (page.length() < MAXLENGTH) {
            page.append("<!-- padding -->");
        }
        // the window ends inside the charset name
        int cut = page.length() + "<meta charset=\"win".length();
        page.append("<meta charset=\"").append(DECLARED).append("\">").append(BODY);
        byte[] content = page.toString().getBytes(StandardCharsets.US_ASCII);

        String charset = CharsetIdentification.getCharsetFast(new Metadata(), content, cut);

        Assertions.assertEquals(DECLARED, charset);
    }

    /**
     * A page whose declaration opens on the last bytes of a window of {@link #MAXLENGTH} and closes
     * {@code quoteOffset} bytes after it. The name is padded with spaces, which the validation
     * trims, so the charset is used whenever the closing quote is read.
     */
    private static byte[] metaCharsetClosingAfterTheWindow(int quoteOffset) {
        StringBuilder page = new StringBuilder("<html><head>");
        while (page.length() < MAXLENGTH) {
            page.append("<!-- padding -->");
        }
        page.setLength(MAXLENGTH - "<meta charset=\"".length());
        page.append("<meta charset=\"");
        page.append(" ".repeat(quoteOffset - DECLARED.length())).append(DECLARED).append("\">");
        page.append(BODY);
        return page.toString().getBytes(StandardCharsets.US_ASCII);
    }

    /** The look-ahead is bounded: a quote on its last byte is read, one byte further is not. */
    @Test
    void lookAheadForTheClosingQuoteIsBounded() {
        String within =
                CharsetIdentification.getCharsetFast(
                        new Metadata(), metaCharsetClosingAfterTheWindow(LOOKAHEAD - 1), MAXLENGTH);
        String beyond =
                CharsetIdentification.getCharsetFast(
                        new Metadata(), metaCharsetClosingAfterTheWindow(LOOKAHEAD), MAXLENGTH);

        Assertions.assertEquals(DECLARED, within);
        Assertions.assertNotEquals(
                DECLARED, beyond, "a declaration closing beyond the look-ahead is not used");
    }
}
