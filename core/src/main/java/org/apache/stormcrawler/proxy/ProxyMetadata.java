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

package org.apache.stormcrawler.proxy;

import java.util.Locale;
import java.util.Optional;
import org.apache.stormcrawler.Metadata;

/** Utilities for resolving per-URL proxy settings from metadata. */
final class ProxyMetadata {

    static final String PROXY = "http.proxy";
    static final String PROXY_HOST = "http.proxy.host";
    static final String PROXY_PORT = "http.proxy.port";
    static final String PROXY_TYPE = "http.proxy.type";
    static final String PROXY_USER = "http.proxy.user";
    static final String PROXY_PASS = "http.proxy.pass";
    static final String PROXY_SKIP = "http.proxy.skip";

    private ProxyMetadata() {}

    static boolean shouldSkipProxy(Metadata metadata) {
        if (metadata == null || !metadata.containsKey(PROXY_SKIP)) {
            return false;
        }

        String skip = requiredValue(metadata, PROXY_SKIP);
        if ("true".equalsIgnoreCase(skip)) {
            return true;
        }
        if ("false".equalsIgnoreCase(skip)) {
            return false;
        }

        throw new IllegalArgumentException(
                "metadata key `" + PROXY_SKIP + "` must be `true` or `false`, got `" + skip + "`");
    }

    static Optional<SCProxy> getProxy(Metadata metadata) {
        if (metadata == null) {
            return Optional.empty();
        }

        if (metadata.containsKey(PROXY)) {
            String proxy = requiredValue(metadata, PROXY);
            try {
                return Optional.of(new SCProxy(proxy));
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException(
                        "metadata key `" + PROXY + "` must be a valid proxy connection string");
            }
        }

        if (!containsProxyField(metadata)) {
            return Optional.empty();
        }

        String host = requiredValue(metadata, PROXY_HOST);
        String type = valueOrDefault(metadata, PROXY_TYPE, "HTTP");
        String port = valueOrDefault(metadata, PROXY_PORT, "8080");
        String username = value(metadata, PROXY_USER);
        String password = value(metadata, PROXY_PASS);

        validatePort(port);
        if ((username == null) != (password == null)) {
            throw new IllegalArgumentException(
                    "metadata proxy authentication requires both `"
                            + PROXY_USER
                            + "` and `"
                            + PROXY_PASS
                            + "`");
        }

        return Optional.of(
                new SCProxy(
                        type.toLowerCase(Locale.ROOT),
                        host,
                        port,
                        username == null ? "" : username,
                        password == null ? "" : password,
                        "",
                        "",
                        "",
                        ""));
    }

    private static boolean containsProxyField(Metadata metadata) {
        return metadata.containsKey(PROXY_HOST)
                || metadata.containsKey(PROXY_PORT)
                || metadata.containsKey(PROXY_TYPE)
                || metadata.containsKey(PROXY_USER)
                || metadata.containsKey(PROXY_PASS);
    }

    private static String valueOrDefault(Metadata metadata, String key, String defaultValue) {
        if (!metadata.containsKey(key)) {
            return defaultValue;
        }
        return requiredValue(metadata, key);
    }

    private static String requiredValue(Metadata metadata, String key) {
        String value = value(metadata, key);
        if (value == null) {
            throw new IllegalArgumentException("metadata key `" + key + "` must not be blank");
        }
        return value;
    }

    private static String value(Metadata metadata, String key) {
        if (!metadata.containsKey(key)) {
            return null;
        }

        String value = metadata.getFirstValue(key);
        if (value == null || value.trim().isEmpty()) {
            return null;
        }
        return value.trim();
    }

    private static void validatePort(String port) {
        int parsedPort;
        try {
            parsedPort = Integer.parseInt(port);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    "metadata key `" + PROXY_PORT + "` must be an integer, got `" + port + "`", e);
        }

        ProxyUtils.validatePortRange(parsedPort, "metadata key `" + PROXY_PORT + "`", port);
    }
}
