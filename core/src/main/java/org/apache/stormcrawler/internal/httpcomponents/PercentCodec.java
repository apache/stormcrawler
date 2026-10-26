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

package org.apache.stormcrawler.internal.httpcomponents;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.BitSet;

/**
 * Percent-encoding mechanism defined in RFC 3986.
 *
 * <p>Copied from {@code org.apache.hc.core5.net.PercentCodec} in Apache HttpComponents Core 5.4.3
 * (org.apache.httpcomponents.core5:httpcore5:5.4.3), so that StormCrawler does not depend on
 * HttpComponents (#2266). Only the encoding and decoding methods used by {@link URLEncodedUtils}
 * are kept, the character sets come from the caller.
 *
 * <p>For internal use only. Not part of StormCrawler's public API.
 */
final class PercentCodec {

    private static final int RADIX = 16;

    private PercentCodec() {}

    static void encode(
            final StringBuilder buf,
            final CharSequence content,
            final Charset charset,
            final BitSet safechars,
            final boolean blankAsPlus) {
        if (content == null) {
            return;
        }
        final CharBuffer cb = CharBuffer.wrap(content);
        final ByteBuffer bb = (charset != null ? charset : StandardCharsets.UTF_8).encode(cb);
        while (bb.hasRemaining()) {
            final int b = bb.get() & 0xff;
            if (safechars.get(b)) {
                buf.append((char) b);
            } else if (blankAsPlus && b == ' ') {
                buf.append("+");
            } else {
                buf.append("%");
                final char hex1 = Character.toUpperCase(Character.forDigit((b >> 4) & 0xF, RADIX));
                final char hex2 = Character.toUpperCase(Character.forDigit(b & 0xF, RADIX));
                buf.append(hex1);
                buf.append(hex2);
            }
        }
    }

    static String decode(
            final CharSequence content, final Charset charset, final boolean plusAsBlank) {
        if (content == null) {
            return null;
        }
        final ByteBuffer bb = ByteBuffer.allocate(content.length());
        final CharBuffer cb = CharBuffer.wrap(content);
        while (cb.hasRemaining()) {
            final char c = cb.get();
            if (c == '%' && cb.remaining() >= 2) {
                final char uc = cb.get();
                final char lc = cb.get();
                final int u = Character.digit(uc, RADIX);
                final int l = Character.digit(lc, RADIX);
                if (u != -1 && l != -1) {
                    bb.put((byte) ((u << 4) + l));
                } else {
                    bb.put((byte) '%');
                    bb.put((byte) uc);
                    bb.put((byte) lc);
                }
            } else if (plusAsBlank && c == '+') {
                bb.put((byte) ' ');
            } else {
                bb.put((byte) c);
            }
        }
        bb.flip();
        return (charset != null ? charset : StandardCharsets.UTF_8).decode(bb).toString();
    }
}
