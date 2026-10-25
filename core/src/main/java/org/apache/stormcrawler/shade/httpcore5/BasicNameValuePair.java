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

import java.util.Locale;
import java.util.Objects;

/**
 * Basic implementation of {@link NameValuePair}.
 *
 * <p>Copied from {@code org.apache.hc.core5.http.message.BasicNameValuePair} in Apache
 * HttpComponents Core 5.4.3 (org.apache.httpcomponents.core5:httpcore5:5.4.3), so that StormCrawler
 * does not depend on HttpComponents (#2266). The argument checks use {@link Objects#requireNonNull}
 * instead of HttpCore's {@code Args}, {@code LangUtils} and {@code TextUtils} are inlined, and it
 * is no longer {@code Serializable}.
 *
 * <p>For internal use only. Not part of StormCrawler's public API.
 */
class BasicNameValuePair implements NameValuePair {

    private final String name;

    private final String value;

    /**
     * Default Constructor taking a name and a value. The value may be null.
     *
     * @param name The name.
     * @param value The value.
     */
    BasicNameValuePair(final String name, final String value) {
        super();
        this.name = Objects.requireNonNull(name, "Name");
        this.value = value;
    }

    @Override
    public String getName() {
        return this.name;
    }

    @Override
    public String getValue() {
        return this.value;
    }

    @Override
    public String toString() {
        if (this.value == null) {
            return name;
        }
        final int len = this.name.length() + 1 + this.value.length();
        final StringBuilder buffer = new StringBuilder(len);
        buffer.append(this.name);
        buffer.append("=");
        buffer.append(this.value);
        return buffer.toString();
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj instanceof BasicNameValuePair) {
            final BasicNameValuePair that = (BasicNameValuePair) obj;
            return this.name.equalsIgnoreCase(that.name) && Objects.equals(this.value, that.value);
        }
        return false;
    }

    @Override
    public int hashCode() {
        // LangUtils.hashCode(seed, obj) inlined: seed * 37 + hash, starting from 17
        int hash = 17;
        hash = hash * 37 + this.name.toLowerCase(Locale.ROOT).hashCode();
        hash = hash * 37 + (this.value != null ? this.value.hashCode() : 0);
        return hash;
    }
}
