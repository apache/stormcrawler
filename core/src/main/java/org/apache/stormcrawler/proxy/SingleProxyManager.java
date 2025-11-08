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
import org.apache.storm.Config;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.util.ConfUtils;

/** SingleProxyManager is a ProxyManager implementation for a single proxy endpoint. */
public class SingleProxyManager implements ProxyManager {
    private SCProxy proxy;

    public SingleProxyManager() {}

    public void configure(Config conf) {
        String proxyConf = ConfUtils.getString(conf, "http.proxy", null);
        // all configured as a single line
        if (proxyConf != null) {
            this.proxy = new SCProxy(proxyConf);
            return;
        }

        // values for single proxy
        String proxyHost = ConfUtils.getString(conf, "http.proxy.host", null);
        String proxyType = ConfUtils.getString(conf, "http.proxy.type", "HTTP");
        int proxyPort = ConfUtils.getInt(conf, "http.proxy.port", 8080);
        String proxyUsername = ConfUtils.getString(conf, "http.proxy.user", null);
        String proxyPassword = ConfUtils.getString(conf, "http.proxy.pass", null);

        // assemble proxy connection string
        String proxyString = proxyType.toLowerCase(Locale.ROOT) + "://";

        // conditionally append authentication info
        if (proxyUsername != null
                && !proxyUsername.isEmpty()
                && proxyPassword != null
                && !proxyPassword.isEmpty()) {
            proxyString += proxyUsername + ":" + proxyPassword + "@";
        }

        // complete proxy string and create proxy
        this.proxy =
                new SCProxy(
                        proxyString + String.format(Locale.ROOT, "%s:%d", proxyHost, proxyPort));
    }

    @Override
    public Optional<SCProxy> getProxy(Metadata metadata) {
        return Optional.of(proxy);
    }
}
