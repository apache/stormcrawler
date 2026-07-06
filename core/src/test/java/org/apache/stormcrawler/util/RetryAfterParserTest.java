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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import java.util.Random;
import org.junit.jupiter.api.Test;

/** Unit and randomised (fuzz) coverage for {@link RetryAfterParser#parseDelay(String)}. */
class RetryAfterParserTest {

    private static final DateTimeFormatter HTTP_DATE =
            DateTimeFormatter.ofPattern("EEE, dd MMM yyyy HH:mm:ss 'GMT'", Locale.ROOT)
                    .withZone(ZoneOffset.UTC);

    @Test
    void parseRetryAfterSeconds() {
        assertEquals(120_000L, RetryAfterParser.parseDelay("120"));
        // surrounding whitespace is tolerated
        assertEquals(120_000L, RetryAfterParser.parseDelay("  120 "));
        // zero is a valid (if pointless) delay and is not treated as "absent"
        assertEquals(0L, RetryAfterParser.parseDelay("0"));
    }

    @Test
    void parseRetryAfterFutureDate() {
        Instant future = Instant.now().plusSeconds(300);
        long delay = RetryAfterParser.parseDelay(HTTP_DATE.format(future));
        // allow a small tolerance for the time elapsed during the call
        assertTrue(delay > 250_000 && delay <= 300_000, "delay was " + delay);
    }

    @Test
    void parseRetryAfterPastDateReturnsMinusOne() {
        Instant past = Instant.now().minusSeconds(300);
        assertEquals(-1L, RetryAfterParser.parseDelay(HTTP_DATE.format(past)));
    }

    @Test
    void httpDateFormatterMatchesSpecAndJdkForTwoDigitDays() {
        // the local formatter mirrors the parser's pattern, so a round-trip
        // alone cannot catch a wrong pattern: cross-check it against the
        // IMF-fixdate example of RFC 7231 and against the JDK's RFC 1123
        // formatter (identical output for two-digit days)
        Instant fixed = Instant.parse("2015-10-21T07:28:00Z");
        assertEquals("Wed, 21 Oct 2015 07:28:00 GMT", HTTP_DATE.format(fixed));
        assertEquals(
                DateTimeFormatter.RFC_1123_DATE_TIME.format(fixed.atOffset(ZoneOffset.UTC)),
                HTTP_DATE.format(fixed));
    }

    @Test
    void parseRetryAfterRejectsUnpaddedSingleDigitDay() {
        // RFC 1123 allows a single-digit day (as produced by the JDK's
        // RFC_1123_DATE_TIME) but IMF-fixdate requires two digits; the parser
        // is deliberately strict and rejects the unpadded form, even for a
        // date in the future
        OffsetDateTime future = OffsetDateTime.now(ZoneOffset.UTC).plusYears(1).withDayOfMonth(3);
        String unpadded = DateTimeFormatter.RFC_1123_DATE_TIME.format(future);
        assertTrue(unpadded.matches("\\w{3}, 3 \\w{3}.*"), "unexpected format: " + unpadded);
        assertEquals(-1L, RetryAfterParser.parseDelay(unpadded));
    }

    @Test
    void parseRetryAfterBlankOrMalformedReturnsMinusOne() {
        assertEquals(-1L, RetryAfterParser.parseDelay(null));
        assertEquals(-1L, RetryAfterParser.parseDelay(""));
        assertEquals(-1L, RetryAfterParser.parseDelay("   "));
        assertEquals(-1L, RetryAfterParser.parseDelay("not-a-date"));
        assertEquals(-1L, RetryAfterParser.parseDelay("12.5"));
        assertEquals(-1L, RetryAfterParser.parseDelay("-5"));
    }

    @Test
    void parseRetryAfterOverlongValueReturnsMinusOne() {
        // an implausibly long header value is rejected without further parsing
        String tooLong = "1".repeat(65);
        assertEquals(-1L, RetryAfterParser.parseDelay(tooLong));
    }

    @Test
    void parseRetryAfterParseOverflowReturnsMinusOne() {
        // too large to fit in a long at all
        assertEquals(-1L, RetryAfterParser.parseDelay("999999999999999999999999999"));
    }

    @Test
    void parseRetryAfterMillisecondOverflowReturnsMinusOne() {
        // parses as a valid long, but * 1000 overflows a long (regression: was a
        // silent overflow yielding a negative delay)
        assertEquals(-1L, RetryAfterParser.parseDelay("3687950425865536959"));
        assertEquals(-1L, RetryAfterParser.parseDelay(Long.toString(Long.MAX_VALUE)));
    }

    @Test
    void parseRetryAfterRandomSecondsRoundTrip() {
        Random rnd = new Random(7);
        for (int i = 0; i < 1_000; i++) {
            long secs = rnd.nextInt(1_000_000);
            assertEquals(secs * 1000L, RetryAfterParser.parseDelay(Long.toString(secs)));
        }
    }

    @Test
    void parseRetryAfterNeverThrowsForRandomInput() {
        Random rnd = new Random(42);
        for (int i = 0; i < 10_000; i++) {
            String input = randomRetryAfterValue(rnd);
            long result = RetryAfterParser.parseDelay(input);
            // contract: never throws, never returns anything below -1
            assertTrue(result >= -1L, "value " + result + " for input <" + input + ">");
        }
    }

    /**
     * Produces a wide spectrum of inputs: digit runs (including values that overflow a long or
     * overflow when converted to ms), real HTTP dates (past and future), blanks, random printable
     * garbage and nulls.
     */
    private static String randomRetryAfterValue(Random rnd) {
        switch (rnd.nextInt(5)) {
            case 0:
                int len = 1 + rnd.nextInt(40);
                StringBuilder digits = new StringBuilder();
                for (int i = 0; i < len; i++) {
                    digits.append((char) ('0' + rnd.nextInt(10)));
                }
                return digits.toString();
            case 1:
                long offset = (rnd.nextBoolean() ? 1L : -1L) * rnd.nextInt(1_000_000);
                return HTTP_DATE.format(Instant.now().plusSeconds(offset));
            case 2:
                return "";
            case 3:
                int glen = rnd.nextInt(30);
                StringBuilder garbage = new StringBuilder();
                for (int i = 0; i < glen; i++) {
                    garbage.append((char) (32 + rnd.nextInt(95)));
                }
                return garbage.toString();
            default:
                return null;
        }
    }
}
