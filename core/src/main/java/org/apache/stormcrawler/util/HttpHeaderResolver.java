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

import java.util.Locale;
import org.apache.stormcrawler.Metadata;

/** Resolves separator variations of an expected HTTP response header. */
public final class HttpHeaderResolver {

    private HttpHeaderResolver() {}

    /**
     * Returns the first value for an expected HTTP header.
     *
     * <p>The exact, case-insensitive metadata key is checked first. Only when it is absent are
     * separator variations considered, for example {@code ContentType} or {@code content_type} for
     * {@code Content-Type}. This lookup is non-destructive and intentionally does not use fuzzy
     * matching, so unrelated extension headers such as {@code X-Location} are not treated as
     * {@code Location}.
     *
     * @param metadata metadata containing HTTP response headers
     * @param headerName expected HTTP header name
     * @return the first value, or {@code null} if the header or an unambiguous alias is absent
     */
    public static String getFirstValue(Metadata metadata, String headerName) {
        return getFirstValue(metadata, headerName, null);
    }

    /**
     * Returns the first value for an expected HTTP header stored with a metadata prefix.
     *
     * @param metadata metadata containing HTTP response headers
     * @param headerName expected HTTP header name
     * @param prefix optional metadata key prefix
     * @return the first value, or {@code null} if the header or an unambiguous alias is absent
     */
    public static String getFirstValue(Metadata metadata, String headerName, String prefix) {
        if (metadata == null || headerName == null || headerName.isEmpty()) {
            return null;
        }

        String normalizedPrefix = prefix == null ? "" : prefix.toLowerCase(Locale.ROOT);
        String exactKey = normalizedPrefix + headerName;
        if (metadata.containsKey(exactKey)) {
            return metadata.getFirstValue(exactKey);
        }

        String normalizedHeaderName = normalizeSeparators(headerName);
        String matchingKey = null;
        for (String key : metadata.keySet(normalizedPrefix)) {
            String candidate = key.substring(normalizedPrefix.length());
            if (!normalizeSeparators(candidate).equals(normalizedHeaderName)) {
                continue;
            }
            if (matchingKey != null) {
                // Multiple aliases are ambiguous. Do not let map iteration order choose a value.
                return null;
            }
            matchingKey = key;
        }
        return matchingKey == null ? null : metadata.getFirstValue(matchingKey);
    }

    private static String normalizeSeparators(String value) {
        StringBuilder normalized = new StringBuilder(value.length());
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (c == '-' || c == '_') {
                continue;
            }
            normalized.append(Character.toLowerCase(c));
        }
        return normalized.toString();
    }
}
