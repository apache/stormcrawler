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

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Objects;

/**
 * A collection of utilities for encoding URLs.
 *
 * <p>Copied from {@code org.apache.hc.core5.net.URLEncodedUtils} in Apache HttpComponents Core
 * 5.4.3 (org.apache.httpcomponents.core5:httpcore5:5.4.3), so that StormCrawler does not depend on
 * HttpComponents (#2266). Only {@code parse(CharSequence, Charset, char...)} and the {@code format}
 * methods are kept, unchanged; the {@code URI} and path segment methods are dropped.
 *
 * <p>For internal use only. Not part of StormCrawler's public API.
 */
public final class URLEncodedUtils {

    private static final char QP_SEP_A = '&';

    private URLEncodedUtils() {}

    /**
     * Returns a list of {@link NameValuePair}s parameters.
     *
     * @param s input text.
     * @param charset parameter charset.
     * @param separators parameter separators.
     * @return list of query parameters.
     */
    public static List<NameValuePair> parse(
            final CharSequence s, final Charset charset, final char... separators) {
        Objects.requireNonNull(s, "Char sequence");
        final Tokenizer tokenParser = Tokenizer.INSTANCE;
        final BitSet delimSet = new BitSet();
        for (final char separator : separators) {
            delimSet.set(separator);
        }
        final Tokenizer.Cursor cursor = new Tokenizer.Cursor(0, s.length());
        final List<NameValuePair> list = new ArrayList<>();
        while (!cursor.atEnd()) {
            delimSet.set('=');
            final String name = tokenParser.parseToken(s, cursor, delimSet);
            String value = null;
            if (!cursor.atEnd()) {
                final int delim = s.charAt(cursor.getPos());
                cursor.updatePos(cursor.getPos() + 1);
                if (delim == '=') {
                    delimSet.clear('=');
                    value = tokenParser.parseToken(s, cursor, delimSet);
                    if (!cursor.atEnd()) {
                        cursor.updatePos(cursor.getPos() + 1);
                    }
                }
            }
            if (!name.isEmpty()) {
                list.add(
                        new BasicNameValuePair(
                                PercentCodec.decode(name, charset, true),
                                PercentCodec.decode(value, charset, true)));
            }
        }
        return list;
    }

    /**
     * Returns a String that is suitable for use as an {@code application/x-www-form-urlencoded}
     * list of parameters in an HTTP PUT or HTTP POST.
     *
     * @param parameters The parameters to include.
     * @param parameterSeparator The parameter separator, by convention, {@code '&'} or {@code ';'}.
     * @param charset The encoding to use.
     * @return An {@code application/x-www-form-urlencoded} string
     */
    public static String format(
            final Iterable<? extends NameValuePair> parameters,
            final char parameterSeparator,
            final Charset charset) {
        Objects.requireNonNull(parameters, "Parameters");
        final StringBuilder buf = new StringBuilder();
        int i = 0;
        for (final NameValuePair parameter : parameters) {
            if (i > 0) {
                buf.append(parameterSeparator);
            }
            PercentCodec.encode(buf, parameter.getName(), charset, URL_ENCODER, true);
            if (parameter.getValue() != null) {
                buf.append('=');
                PercentCodec.encode(buf, parameter.getValue(), charset, URL_ENCODER, true);
            }
            i++;
        }
        return buf.toString();
    }

    /**
     * Returns a String that is suitable for use as an {@code application/x-www-form-urlencoded}
     * list of parameters in an HTTP PUT or HTTP POST.
     *
     * @param parameters The parameters to include.
     * @param charset The encoding to use.
     * @return An {@code application/x-www-form-urlencoded} string
     */
    public static String format(
            final Iterable<? extends NameValuePair> parameters, final Charset charset) {
        return format(parameters, QP_SEP_A, charset);
    }

    private static final BitSet URL_ENCODER = new BitSet(256);

    static {
        // unreserved chars
        // alpha characters
        for (int i = 'a'; i <= 'z'; i++) {
            URL_ENCODER.set(i);
        }
        for (int i = 'A'; i <= 'Z'; i++) {
            URL_ENCODER.set(i);
        }
        // numeric characters
        for (int i = '0'; i <= '9'; i++) {
            URL_ENCODER.set(i);
        }
        URL_ENCODER.set('_'); // these are the characters of the "mark" list
        URL_ENCODER.set('-');
        URL_ENCODER.set('.');
        URL_ENCODER.set('*');
    }
}
