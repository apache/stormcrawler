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

import java.util.BitSet;

/**
 * Tokenizer that can be used as a foundation for more complex parsing routines.
 *
 * <p>Copied from {@code org.apache.hc.core5.util.Tokenizer} in Apache HttpComponents Core 5.4.3
 * (org.apache.httpcomponents.core5:httpcore5:5.4.3), so that StormCrawler does not depend on
 * HttpComponents (#2266). Only the methods used by {@link URLEncodedUtils} are kept, the argument
 * checks of HttpCore's {@code Args} are inlined, and the deprecated {@code BitSet} variants are
 * used directly instead of delegating to a {@code Delimiter}.
 *
 * <p>This class is immutable and thread safe.
 *
 * <p>For internal use only. Not part of StormCrawler's public API.
 */
final class Tokenizer {

    static final class Cursor {

        private final int lowerBound;
        private final int upperBound;
        private int pos;

        Cursor(final int lowerBound, final int upperBound) {
            super();
            if (lowerBound < 0) {
                throw new IllegalArgumentException("lowerBound may not be negative");
            }
            if (lowerBound > upperBound) {
                throw new IllegalArgumentException("lowerBound cannot be greater than upperBound");
            }
            this.lowerBound = lowerBound;
            this.upperBound = upperBound;
            this.pos = lowerBound;
        }

        int getUpperBound() {
            return this.upperBound;
        }

        int getPos() {
            return this.pos;
        }

        void updatePos(final int pos) {
            if (pos < this.lowerBound) {
                throw new IllegalArgumentException(
                        "pos: " + pos + " < lowerBound: " + this.lowerBound);
            }
            if (pos > this.upperBound) {
                throw new IllegalArgumentException(
                        "pos: " + pos + " > upperBound: " + this.upperBound);
            }
            this.pos = pos;
        }

        boolean atEnd() {
            return this.pos >= this.upperBound;
        }

        @Override
        public String toString() {
            final StringBuilder buffer = new StringBuilder();
            buffer.append('[');
            buffer.append(this.lowerBound);
            buffer.append('>');
            buffer.append(this.pos);
            buffer.append('>');
            buffer.append(this.upperBound);
            buffer.append(']');
            return buffer.toString();
        }
    }

    static final int CR = 13; // <US-ASCII CR, carriage return (13)>
    static final int LF = 10; // <US-ASCII LF, linefeed (10)>
    static final int SP = 32; // <US-ASCII SP, space (32)>
    static final int HT = 9; // <US-ASCII HT, horizontal-tab (9)>

    static boolean isWhitespace(final char ch) {
        return ch == SP || ch == HT || ch == CR || ch == LF;
    }

    static final Tokenizer INSTANCE = new Tokenizer();

    /**
     * Extracts from the sequence of chars a token terminated with any of the given delimiters
     * discarding semantically insignificant whitespace characters.
     *
     * @param buf buffer with the sequence of chars to be parsed
     * @param cursor defines the bounds and current position of the buffer
     * @param delimiters set of delimiting characters. Can be {@code null} if the token is not
     *     delimited by any character.
     */
    String parseToken(final CharSequence buf, final Cursor cursor, final BitSet delimiters) {
        final StringBuilder dst = new StringBuilder();
        boolean whitespace = false;
        while (!cursor.atEnd()) {
            final char current = buf.charAt(cursor.getPos());
            if (delimiters != null && delimiters.get(current)) {
                break;
            } else if (isWhitespace(current)) {
                skipWhiteSpace(buf, cursor);
                whitespace = true;
            } else {
                if (whitespace && dst.length() > 0) {
                    dst.append(' ');
                }
                copyContent(buf, cursor, delimiters, dst);
                whitespace = false;
            }
        }
        return dst.toString();
    }

    /**
     * Skips semantically insignificant whitespace characters and moves the cursor to the closest
     * non-whitespace character.
     *
     * @param buf buffer with the sequence of chars to be parsed
     * @param cursor defines the bounds and current position of the buffer
     */
    void skipWhiteSpace(final CharSequence buf, final Cursor cursor) {
        int pos = cursor.getPos();
        final int indexFrom = cursor.getPos();
        final int indexTo = cursor.getUpperBound();
        for (int i = indexFrom; i < indexTo; i++) {
            final char current = buf.charAt(i);
            if (!isWhitespace(current)) {
                break;
            }
            pos++;
        }
        cursor.updatePos(pos);
    }

    /**
     * Transfers content into the destination buffer until a whitespace character or any of the
     * given delimiters is encountered.
     *
     * @param buf buffer with the sequence of chars to be parsed
     * @param cursor defines the bounds and current position of the buffer
     * @param delimiters set of delimiting characters. Can be {@code null} if the token is delimited
     *     by a whitespace only.
     * @param dst destination buffer
     */
    void copyContent(
            final CharSequence buf,
            final Cursor cursor,
            final BitSet delimiters,
            final StringBuilder dst) {
        int pos = cursor.getPos();
        final int indexFrom = cursor.getPos();
        final int indexTo = cursor.getUpperBound();
        for (int i = indexFrom; i < indexTo; i++) {
            final char current = buf.charAt(i);
            if (delimiters != null && delimiters.get(current) || isWhitespace(current)) {
                break;
            }
            pos++;
            dst.append(current);
        }
        cursor.updatePos(pos);
    }
}
