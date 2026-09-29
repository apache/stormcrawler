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

package org.apache.stormcrawler.bolt;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import org.apache.stormcrawler.Constants;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.metrics.ScopedCounter;
import org.apache.stormcrawler.persistence.Status;
import org.apache.stormcrawler.protocol.AbstractHttpProtocol;
import org.apache.stormcrawler.protocol.FetchTimeoutException;
import org.apache.stormcrawler.protocol.ProtocolResponse;
import org.junit.jupiter.api.Test;

class FetchOutcomesTest {

    private static final String URL = "http://a.test/page";

    private static final String PREFIX = "protocol.";

    private final Map<String, Long> counts = new HashMap<>();

    private final ScopedCounter counter = name -> n -> counts.merge(name, n, Long::sum);

    private final FetchOutcomes outcomes = new FetchOutcomes(PREFIX, counter);

    private static ProtocolResponse response(int code, String... headers) {
        Metadata metadata = new Metadata();
        for (int i = 0; i < headers.length; i += 2) {
            metadata.setValue(headers[i], headers[i + 1]);
        }
        return new ProtocolResponse(new byte[] {1, 2, 3}, code, metadata);
    }

    private FetchOutcomes.Outcome ofResponse(ProtocolResponse response, Metadata metadata) {
        return outcomes.ofResponse(response, metadata, null, 7, 11);
    }

    @Test
    void pageIsSentForParsingWithTheMergedMetadata() {
        Metadata source = new Metadata();
        source.setValue("depth", "1");

        FetchOutcomes.Outcome outcome =
                ofResponse(response(200, "Content-Type", "text/html"), source);
        assertEquals(Status.FETCHED, outcome.status());
        assertTrue(outcome.parse());
        assertNull(outcome.redirectTarget());
        Metadata md = outcome.metadata();
        assertNotSame(source, md);
        assertEquals(1, source.size());
        assertEquals("1", md.getFirstValue("depth"));
        assertEquals("text/html", md.getFirstValue(PREFIX + "Content-Type"));
        assertEquals("200", md.getFirstValue("fetch.statusCode"));
        assertEquals("3", md.getFirstValue("fetch.byteLength"));
        assertEquals("7", md.getFirstValue("fetch.loadingTime"));
        assertEquals("11", md.getFirstValue("fetch.timeInQueues"));
        assertEquals(Map.of("status_200", 1L), counts);
    }

    @Test
    void notModifiedIsNotParsed() {
        FetchOutcomes.Outcome outcome = ofResponse(response(304), new Metadata());
        assertEquals(Status.FETCHED, outcome.status());
        assertFalse(outcome.parse());
    }

    @Test
    void redirectCarriesItsTarget() {
        FetchOutcomes.Outcome outcome =
                ofResponse(response(301, "Location", "http://a.test/new"), new Metadata());
        assertEquals(Status.REDIRECTION, outcome.status());
        assertFalse(outcome.parse());
        assertEquals("http://a.test/new", outcome.redirectTarget());
        assertEquals("http://a.test/new", outcome.metadata().getFirstValue("_redirTo"));
        assertArrayEquals(new String[0], outcome.redirectKeyValues());
    }

    @Test
    void redirectWithoutLocationHasNoTarget() {
        FetchOutcomes.Outcome outcome = ofResponse(response(302), new Metadata());
        assertEquals(Status.REDIRECTION, outcome.status());
        assertNull(outcome.redirectTarget());
        assertNull(outcome.metadata().getFirstValue("_redirTo"));
    }

    @Test
    void locationOutsideARedirectIsIgnored() {
        FetchOutcomes.Outcome outcome =
                ofResponse(response(201, "Location", "http://a.test/new"), new Metadata());
        assertNull(outcome.redirectTarget());
        assertNull(outcome.metadata().getFirstValue("_redirTo"));
    }

    @Test
    void blankLocationIsNoTarget() {
        FetchOutcomes.Outcome outcome = ofResponse(response(301, "Location", " "), new Metadata());
        assertNull(outcome.redirectTarget());
        assertNull(outcome.metadata().getFirstValue("_redirTo"));
    }

    @Test
    void redirectedSitemapStaysASitemap() {
        Metadata source = new Metadata();
        source.setValue(SiteMapParserBolt.isSitemapKey, "true");

        FetchOutcomes.Outcome outcome =
                ofResponse(response(301, "Location", "http://a.test/sitemap_index.xml"), source);
        assertArrayEquals(
                new String[] {SiteMapParserBolt.isSitemapKey, "true"}, outcome.redirectKeyValues());
    }

    @Test
    void errorStatusIsAFetchError() {
        FetchOutcomes.Outcome outcome = ofResponse(response(404), new Metadata());
        assertEquals(Status.FETCH_ERROR, outcome.status());
        assertFalse(outcome.parse());
        assertEquals(Map.of("status_404", 1L), counts);
    }

    /**
     * The robots crawl delay key only ever carries the value parsed from robots.txt, and request
     * headers only ever come from the configuration, whatever the response says.
     */
    @Test
    void responseCannotSetControlKeys() {
        FetchOutcomes unprefixed = new FetchOutcomes("", counter);
        Metadata source = new Metadata();
        source.setValue(AbstractHttpProtocol.SET_HEADER_BY_REQUEST, "X-Configured=yes");
        ProtocolResponse response =
                response(
                        200,
                        Constants.ROBOTS_CRAWL_DELAY_KEY,
                        "120",
                        AbstractHttpProtocol.SET_HEADER_BY_REQUEST,
                        "X-Injected=yes");

        Metadata md = unprefixed.ofResponse(response, source, null, 0, 0).metadata();
        assertNull(md.getFirstValue(Constants.ROBOTS_CRAWL_DELAY_KEY));
        assertArrayEquals(
                new String[] {"X-Configured=yes"},
                md.getValues(AbstractHttpProtocol.SET_HEADER_BY_REQUEST));

        md = unprefixed.ofResponse(response, source, "60", 0, 0).metadata();
        assertEquals("60", md.getFirstValue(Constants.ROBOTS_CRAWL_DELAY_KEY));
    }

    @Test
    void fetchDeadlineIsASocketTimeout() {
        FetchOutcomes.Outcome outcome =
                outcomes.ofFailure(new FetchTimeoutException(URL, 5), URL, metadata());
        assertEquals(Status.FETCH_ERROR, outcome.status());
        assertEquals(
                "Socket timeout fetching", outcome.metadata().getFirstValue("fetch.exception"));
        assertEquals(Map.of("fetch.timeout", 1L, "fetch.deadline", 1L, "exception", 1L), counts);
    }

    @Test
    void socketTimeoutIsNotADeadline() {
        FetchOutcomes.Outcome outcome =
                outcomes.ofFailure(new SocketTimeoutException("read"), URL, metadata());
        assertEquals(
                "Socket timeout fetching", outcome.metadata().getFirstValue("fetch.exception"));
        assertEquals(Map.of("fetch.timeout", 1L, "exception", 1L), counts);
    }

    /** The protocols report some timeouts only in the message. */
    @Test
    void timedOutMessageIsASocketTimeout() {
        FetchOutcomes.Outcome outcome =
                outcomes.ofFailure(new IOException("connect timed out"), URL, metadata());
        assertEquals(
                "Socket timeout fetching", outcome.metadata().getFirstValue("fetch.exception"));
        assertEquals(Map.of("fetch.timeout", 1L, "exception", 1L), counts);
    }

    @Test
    void unknownHostIsRecognisedDirectlyOrAsCause() {
        FetchOutcomes.Outcome direct =
                outcomes.ofFailure(new UnknownHostException("a.test"), URL, metadata());
        FetchOutcomes.Outcome wrapped =
                outcomes.ofFailure(
                        new IOException(new UnknownHostException("a.test")), URL, metadata());
        assertEquals("Unknown host", direct.metadata().getFirstValue("fetch.exception"));
        assertEquals("Unknown host", wrapped.metadata().getFirstValue("fetch.exception"));
        assertEquals(Map.of("exception", 2L), counts);
    }

    @Test
    void otherFailureIsNamedByItsClass() {
        Metadata source = metadata();

        FetchOutcomes.Outcome outcome =
                outcomes.ofFailure(new IllegalArgumentException("bad proxy"), URL, source);
        assertSame(source, outcome.metadata());
        assertEquals(
                IllegalArgumentException.class.getName(),
                outcome.metadata().getFirstValue("fetch.exception"));
        assertEquals(Map.of("exception", 1L), counts);
    }

    /** Metadata.empty is read-only and shared, so the failure goes to a new instance. */
    @Test
    void failureNeverWritesToTheSharedEmptyMetadata() {
        FetchOutcomes.Outcome outcome =
                outcomes.ofFailure(new IllegalStateException(), URL, Metadata.empty);
        assertNotSame(Metadata.empty, outcome.metadata());
        assertEquals(
                IllegalStateException.class.getName(),
                outcome.metadata().getFirstValue("fetch.exception"));
        assertEquals(0, Metadata.empty.size());
    }

    private static Metadata metadata() {
        Metadata metadata = new Metadata();
        metadata.setValue("depth", "1");
        return metadata;
    }
}
