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
import java.util.Objects;

final class ProxyUtils {

    static final int MIN_PORT = 1;
    static final int MAX_PORT = 65535;

    private ProxyUtils() {}

    static void validatePortRange(int port, String source, String value) {
        if (port < MIN_PORT || port > MAX_PORT) {
            throw new IllegalArgumentException(
                    source
                            + " must be between "
                            + MIN_PORT
                            + " and "
                            + MAX_PORT
                            + ", got `"
                            + value
                            + "`");
        }
    }

    static boolean isSameProxy(SCProxy proxy, SCProxy otherProxy) {
        return Objects.equals(normalize(proxy.getProtocol()), normalize(otherProxy.getProtocol()))
                && Objects.equals(normalize(proxy.getAddress()), normalize(otherProxy.getAddress()))
                && Objects.equals(proxy.getPort(), otherProxy.getPort())
                && Objects.equals(proxy.getUsername(), otherProxy.getUsername())
                && Objects.equals(proxy.getPassword(), otherProxy.getPassword());
    }

    private static String normalize(String value) {
        return value == null ? null : value.toLowerCase(Locale.ROOT);
    }
}
