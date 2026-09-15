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

package org.apache.stormcrawler.protocol;

import java.util.Map;
import org.apache.stormcrawler.Constants;
import org.apache.stormcrawler.util.ConfUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Reads {@code fetcher.thread.timeout} from the configuration, the same way for every user. */
public final class FetchTimeout {

    private static final Logger LOG = LoggerFactory.getLogger(FetchTimeout.class);

    private FetchTimeout() {}

    /**
     * The deadline in seconds, or -1 when disabled. Never larger than {@code
     * topology.message.timeout.secs}: Storm would fail the tuple first anyway, and with okhttp the
     * per-call deadline replaces the client-level call timeout derived from the message timeout,
     * which must not be loosened.
     */
    public static long secs(Map<String, Object> conf) {
        long timeout = ConfUtils.getLong(conf, Constants.FETCH_TIMEOUT_PARAM_KEY, -1);
        if (timeout <= 0) {
            return -1;
        }
        final long messageTimeout = ConfUtils.getLong(conf, "topology.message.timeout.secs", -1);
        if (messageTimeout > 0 && timeout > messageTimeout) {
            LOG.warn(
                    "{} ({}s) is larger than topology.message.timeout.secs ({}s): using the latter",
                    Constants.FETCH_TIMEOUT_PARAM_KEY,
                    timeout,
                    messageTimeout);
            timeout = messageTimeout;
        }
        return timeout;
    }
}
