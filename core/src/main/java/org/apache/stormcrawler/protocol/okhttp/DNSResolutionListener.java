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

package org.apache.stormcrawler.protocol.okhttp;

import java.net.InetAddress;
import java.util.List;
import java.util.Map;
import okhttp3.Call;
import okhttp3.EventListener;
import org.slf4j.LoggerFactory;

public class DNSResolutionListener extends EventListener {

    private static final org.slf4j.Logger LOG =
            LoggerFactory.getLogger(DNSResolutionListener.class);

    private long dnsStartMillis;

    final Map<String, Long> times;

    public DNSResolutionListener(final Map<String, Long> times) {
        this.times = times;
    }

    @Override
    public void dnsEnd(Call call, String domainName, List<InetAddress> inetAddressList) {
        long timeSpent = System.currentTimeMillis() - dnsStartMillis;
        LOG.debug("DNS resolution for {} took {} millisecs", domainName, timeSpent);
        times.put(call.toString(), timeSpent);
    }

    @Override
    public void dnsStart(Call call, String domainName) {
        dnsStartMillis = System.currentTimeMillis();
    }
}
