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

import java.time.DateTimeException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import java.util.regex.Pattern;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Parses the value of a <a
 * href="https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Retry-After">Retry-After</a> HTTP
 * response header, as sent by servers alongside a 429 (Too Many Requests) or 503 (Service
 * Unavailable) response to request a back-off.
 */
public final class RetryAfterParser {

    private static final Logger LOG = LoggerFactory.getLogger(RetryAfterParser.class);

    /**
     * Formatter for the HTTP-date form of the Retry-After header, e.g. {@code Wed, 21 Oct 2015
     * 07:28:00 GMT}. {@link DateTimeFormatter} is immutable and thread-safe, unlike {@code
     * SimpleDateFormat}, which matters as this is shared across threads.
     */
    private static final DateTimeFormatter HTTP_DATE_FORMATTER =
            DateTimeFormatter.ofPattern("EEE, dd MMM yyyy HH:mm:ss 'GMT'", Locale.ROOT)
                    .withZone(ZoneOffset.UTC);

    /** Pre-compiled matcher for the numeric (delta-seconds) form of the Retry-After header. */
    private static final Pattern RETRY_AFTER_SECONDS = Pattern.compile("[0-9]+");

    /**
     * Upper bound on the length of a Retry-After header value we will attempt to parse. A
     * delta-seconds value is at most 19 digits and an HTTP date ~29 chars; anything longer is
     * rejected to bound the work done on this attacker-controlled value.
     */
    private static final int MAX_RETRY_AFTER_VALUE_LENGTH = 64;

    private RetryAfterParser() {}

    /**
     * Parses a Retry-After header value into a delay expressed in milliseconds, relative to now.
     * The value is either a number of seconds (e.g. {@code 120}) or an HTTP date (e.g. {@code Wed,
     * 21 Oct 2015 07:28:00 GMT}).
     *
     * @return the delay in milliseconds (possibly {@code 0}), or {@code -1} if the header is
     *     absent, malformed, in the past or implausibly long.
     */
    public static long parseDelay(String retryAfter) {
        if (StringUtils.isBlank(retryAfter)) {
            return -1;
        }
        retryAfter = retryAfter.trim();
        // a valid Retry-After is either a delta-seconds value or an HTTP date;
        // reject anything implausibly long to bound the work done on this
        // attacker-controlled header value
        if (retryAfter.length() > MAX_RETRY_AFTER_VALUE_LENGTH) {
            return -1;
        }
        // delay expressed as a number of seconds
        if (RETRY_AFTER_SECONDS.matcher(retryAfter).matches()) {
            try {
                return Math.multiplyExact(Long.parseLong(retryAfter), 1000L);
            } catch (NumberFormatException | ArithmeticException e) {
                // value too large to fit in a long, or its conversion to ms
                // overflows - ignore
                return -1;
            }
        }
        // delay expressed as an HTTP date (IMF-fixdate form only, as produced by
        // virtually all servers; the legacy RFC 850 / asctime forms are not accepted)
        try {
            Instant date = Instant.from(HTTP_DATE_FORMATTER.parse(retryAfter));
            long delay = date.toEpochMilli() - System.currentTimeMillis();
            return delay > 0 ? delay : -1;
        } catch (DateTimeException e) {
            LOG.debug("Invalid Retry-After header value: {}", retryAfter);
            return -1;
        }
    }
}
