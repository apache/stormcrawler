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
                                CharsetIdentification.getCharset(new Metadata(), content, maxlength);
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

    /** A declaration cut by the detection window is still read, see #870. */
    @Test
    void metaCharsetCutByTheDetectionWindowIsStillRead() {
        // ASCII content, no BOM and no HTTP header: only the meta tag can yield this charset
        String declaration = "<meta charset=\"windows-1251\">";
        StringBuilder page = new StringBuilder("<html><head>");
        while (page.length() < MAXLENGTH) {
            page.append("<!-- padding -->");
        }
        // the window ends inside the charset name, the buffer ends right after the tag
        int cut = page.length() + "<meta charset=\"win".length();
        page.append(declaration).append("</head><body></body></html>");
        byte[] content = page.toString().getBytes(StandardCharsets.US_ASCII);

        String charset = CharsetIdentification.getCharsetFast(new Metadata(), content, cut);

        Assertions.assertEquals("windows-1251", charset);
    }
}
