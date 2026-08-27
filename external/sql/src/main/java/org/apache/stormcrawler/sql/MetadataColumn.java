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

package org.apache.stormcrawler.sql;

import org.apache.stormcrawler.Metadata;

/**
 * Encodes and decodes the content of the <code>metadata</code> column of the status table.
 *
 * <p>The keys and values are held in a single column as a list of <code>key=value</code> pairs
 * separated by tabulations. Since a value can contain any character, including the separators
 * themselves, the pairs written by {@link #encode(Metadata)} have the backslash, tabulation,
 * carriage return, line feed and equal sign characters escaped, so that the decoding is
 * unambiguous: a value can never introduce a key of its own.
 *
 * <p>An escaped column is made of {@link #FORMAT_MARKER} followed by the pairs, each preceded by a
 * tabulation. Columns written before the escaping was introduced start with a tabulation or are
 * empty; they are decoded verbatim, as they were previously, so that an existing table keeps
 * working.
 */
final class MetadataColumn {

    /** Marks a column whose keys and values are escaped. */
    static final String FORMAT_MARKER = "v1";

    private static final char SEPARATOR = '\t';

    private static final char KEY_VALUE_SEPARATOR = '=';

    private static final char ESCAPE = '\\';

    private MetadataColumn() {}

    /** Returns the representation of the metadata to store in the column. */
    static String encode(Metadata metadata) {
        final StringBuilder column = new StringBuilder(FORMAT_MARKER);
        for (String key : metadata.keySet()) {
            // a key holding no readable value, such as the empty key a column written before the
            // escaping can carry, is not written out
            final String[] values = metadata.getValues(key);
            if (values == null) {
                continue;
            }
            for (String value : values) {
                if (value == null) {
                    continue;
                }
                column.append(SEPARATOR)
                        .append(escape(key))
                        .append(KEY_VALUE_SEPARATOR)
                        .append(escape(value));
            }
        }
        return column.toString();
    }

    /** Rebuilds the metadata from the content of the column, which can be null or empty. */
    static Metadata decode(String column) {
        final Metadata metadata = new Metadata();
        if (column == null || column.isEmpty()) {
            return metadata;
        }
        // the marker is only one when it is on its own or followed by a separator, so that a
        // column hand written without the leading tabulation is not mistaken for an escaped one
        final boolean escaped =
                column.equals(FORMAT_MARKER) || column.startsWith(FORMAT_MARKER + SEPARATOR);
        final String pairs = escaped ? column.substring(FORMAT_MARKER.length()) : column;
        for (String pair : pairs.split("\t")) {
            if (pair.isEmpty()) {
                continue;
            }
            final int separator =
                    escaped ? separatorIndex(pair) : pair.indexOf(KEY_VALUE_SEPARATOR);
            if (separator == -1) {
                continue;
            }
            final String key = pair.substring(0, separator);
            final String value = pair.substring(separator + 1);
            if (escaped) {
                metadata.addValue(unescape(key), unescape(value));
            } else {
                metadata.addValue(key, value);
            }
        }
        return metadata;
    }

    /** Position of the first key / value separator which is not escaped, -1 if there is none. */
    private static int separatorIndex(String pair) {
        boolean escaping = false;
        for (int i = 0; i < pair.length(); i++) {
            final char c = pair.charAt(i);
            if (escaping) {
                escaping = false;
            } else if (c == ESCAPE) {
                escaping = true;
            } else if (c == KEY_VALUE_SEPARATOR) {
                return i;
            }
        }
        return -1;
    }

    private static String escape(String value) {
        final StringBuilder escaped = new StringBuilder(value.length());
        for (int i = 0; i < value.length(); i++) {
            final char c = value.charAt(i);
            switch (c) {
                case ESCAPE -> escaped.append("\\\\");
                case SEPARATOR -> escaped.append("\\t");
                case '\n' -> escaped.append("\\n");
                case '\r' -> escaped.append("\\r");
                case KEY_VALUE_SEPARATOR -> escaped.append("\\=");
                default -> escaped.append(c);
            }
        }
        return escaped.toString();
    }

    private static String unescape(String value) {
        final StringBuilder unescaped = new StringBuilder(value.length());
        for (int i = 0; i < value.length(); i++) {
            final char c = value.charAt(i);
            if (c != ESCAPE || i == value.length() - 1) {
                unescaped.append(c);
                continue;
            }
            final char next = value.charAt(++i);
            switch (next) {
                case '\\' -> unescaped.append(ESCAPE);
                case 't' -> unescaped.append(SEPARATOR);
                case 'n' -> unescaped.append('\n');
                case 'r' -> unescaped.append('\r');
                case '=' -> unescaped.append(KEY_VALUE_SEPARATOR);
                // not a sequence we produce: keep it as it is
                default -> unescaped.append(c).append(next);
            }
        }
        return unescaped.toString();
    }
}
