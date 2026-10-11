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

import java.io.InterruptedIOException;

/**
 * Thrown when a protocol call did not complete within {@code fetcher.thread.timeout}: by a {@link
 * Protocol} which enforces the deadline itself (okhttp cancels the call), or by the fetcher bolts
 * when the call ran on a helper thread and was abandoned there.
 *
 * <p>It is an {@link InterruptedIOException} so that it is classified like any other timeout, but
 * can be told apart from the socket timeouts of the protocol ({@code http.timeout}).
 */
public class FetchTimeoutException extends InterruptedIOException {

    private final long timeoutSecs;

    public FetchTimeoutException(String url, long timeoutSecs) {
        this(url, timeoutSecs, null);
    }

    public FetchTimeoutException(String url, long timeoutSecs, Throwable cause) {
        super("Fetch timed out after " + timeoutSecs + "s fetching " + url);
        this.timeoutSecs = timeoutSecs;
        if (cause != null) {
            initCause(cause);
        }
    }

    /** The deadline that passed, in seconds. */
    public long getTimeoutSecs() {
        return timeoutSecs;
    }
}
