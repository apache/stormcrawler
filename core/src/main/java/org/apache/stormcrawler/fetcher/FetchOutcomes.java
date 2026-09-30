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

package org.apache.stormcrawler.fetcher;

import java.io.InterruptedIOException;
import java.net.UnknownHostException;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHeaders;
import org.apache.stormcrawler.Constants;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.bolt.FetcherBolt;
import org.apache.stormcrawler.bolt.SiteMapParserBolt;
import org.apache.stormcrawler.bolt.StatusEmitterBolt;
import org.apache.stormcrawler.metrics.ScopedCounter;
import org.apache.stormcrawler.persistence.Status;
import org.apache.stormcrawler.protocol.AbstractHttpProtocol;
import org.apache.stormcrawler.protocol.FetchTimeoutException;
import org.apache.stormcrawler.protocol.ProtocolResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Turns the response of a protocol for a URL, or any exception thrown while fetching it, into what
 * {@link FetcherBolt} reports for the URL. Counts each response in status_{code}, each failure in
 * exception, timeouts also in fetch.timeout and the fetcher.thread.timeout deadline also in
 * fetch.deadline. The bolt does the emitting.
 *
 * <p>For internal use only. Not part of StormCrawler's public API.
 */
public final class FetchOutcomes {

    // the bolt's category: log configurations for FetcherBolt keep covering these lines
    private static final Logger LOG = LoggerFactory.getLogger(FetcherBolt.class);

    private static final String[] NO_KEY_VALUES = {};

    /**
     * What to report for a URL.
     *
     * @param metadata the metadata to emit with the URL
     * @param parse whether the content goes to the default stream for parsing; otherwise the status
     *     goes to the status stream
     * @param redirectTarget the Location of a redirect, as sent and not resolved; null for anything
     *     but a redirect with a non-blank Location
     * @param redirectKeyValues custom key/values for the outlink to the redirect target, as {@link
     *     StatusEmitterBolt#emitOutlink} takes them
     */
    public record Outcome(
            Status status,
            Metadata metadata,
            boolean parse,
            String redirectTarget,
            String[] redirectKeyValues) {}

    private final String protocolMetadataPrefix;
    private final ScopedCounter eventCounter;

    public FetchOutcomes(String protocolMetadataPrefix, ScopedCounter eventCounter) {
        this.protocolMetadataPrefix = protocolMetadataPrefix;
        this.eventCounter = eventCounter;
    }

    /**
     * @param metadata the metadata the URL came with, left unchanged: the outcome has a merged copy
     * @param robotsCrawlDelaySecs the robots crawl delay to report, or null
     * @param timeFetching milliseconds spent fetching
     * @param timeInQueues milliseconds the URL waited in the fetch queues
     */
    public Outcome ofResponse(
            ProtocolResponse response,
            Metadata metadata,
            String robotsCrawlDelaySecs,
            long timeFetching,
            long timeInQueues) {
        // merges the original MD and the ones returned by the
        // protocol
        Metadata mergedMetadata = new Metadata();
        mergedMetadata.putAll(metadata);

        // add a prefix to avoid confusion, preserve protocol
        // metadata persisted or transferred from previous fetches
        mergedMetadata.putAll(response.getMetadata(), protocolMetadataPrefix);

        // Only the locally parsed robots.txt value may populate this control signal.
        // A colliding protocol prefix/header must not pace an unrelated queue.
        mergedMetadata.remove(Constants.ROBOTS_CRAWL_DELAY_KEY);

        // Request shaping comes from the configuration, never from a fetched page: a
        // response header named set-header lands on exactly the key the protocols read
        // to add headers to an outgoing request. Any value configured for this URL is
        // put back after the response metadata has been dropped.
        final String setHeaderKey =
                protocolMetadataPrefix + AbstractHttpProtocol.SET_HEADER_BY_REQUEST;
        mergedMetadata.remove(setHeaderKey);
        mergedMetadata.setValues(setHeaderKey, metadata.getValues(setHeaderKey));
        if (robotsCrawlDelaySecs != null) {
            mergedMetadata.setValue(Constants.ROBOTS_CRAWL_DELAY_KEY, robotsCrawlDelaySecs);
        }

        mergedMetadata.setValue("fetch.statusCode", Integer.toString(response.getStatusCode()));
        mergedMetadata.setValue("fetch.byteLength", Integer.toString(response.getContent().length));
        mergedMetadata.setValue("fetch.loadingTime", Long.toString(timeFetching));
        mergedMetadata.setValue("fetch.timeInQueues", Long.toString(timeInQueues));

        final Status status = Status.fromHTTPCode(response.getStatusCode());
        eventCounter.scope("status_" + response.getStatusCode()).incrBy(1);

        if (status == Status.FETCHED) {
            // a 304 is marked as fetched so that it gets rescheduled, but there is nothing to
            // parse or index
            boolean parse = response.getStatusCode() != 304;
            return new Outcome(status, mergedMetadata, parse, null, NO_KEY_VALUES);
        }

        String redirection =
                status == Status.REDIRECTION
                        ? response.getMetadata().getFirstValue(HttpHeaders.LOCATION)
                        : null;
        if (StringUtils.isBlank(redirection)) {
            return new Outcome(status, mergedMetadata, false, null, NO_KEY_VALUES);
        }

        // stores the URL it redirects to
        // used for debugging mainly - do not resolve the target
        // URL
        mergedMetadata.setValue("_redirTo", redirection);

        // a sitemap which redirects (e.g. /sitemap.xml to
        // /sitemap_index.xml) must stay a sitemap: the key
        // is persisted, not transferred to outlinks by
        // default, so carry it onto the redirect target
        String[] keyValues =
                Boolean.parseBoolean(mergedMetadata.getFirstValue(SiteMapParserBolt.isSitemapKey))
                        ? new String[] {SiteMapParserBolt.isSitemapKey, "true"}
                        : NO_KEY_VALUES;
        return new Outcome(status, mergedMetadata, false, redirection, keyValues);
    }

    /**
     * Records the failure as fetch.exception: "Socket timeout fetching", "Unknown host", or the
     * class name of the exception.
     *
     * @param url the URL, for the log
     * @param metadata the metadata the URL came with; the failure is written to it, or to a new
     *     instance when it is empty, since it may then be the read-only {@link Metadata#empty}
     */
    public Outcome ofFailure(Exception e, String url, Metadata metadata) {
        String message = e.getMessage();
        if (message == null) {
            message = "";
        }

        // common exceptions for which we log only a short message
        if (e instanceof InterruptedIOException || message.contains(" timed out")) {
            LOG.info("Socket timeout fetching {}", url);
            message = "Socket timeout fetching";
            eventCounter.scope("fetch.timeout").incrBy(1);
            if (e instanceof FetchTimeoutException) {
                // the hard deadline, as opposed to the protocol's socket timeouts
                eventCounter.scope("fetch.deadline").incrBy(1);
            }
        } else if (e.getCause() instanceof UnknownHostException
                || e instanceof UnknownHostException) {
            LOG.info("Unknown host {}", url);
            message = "Unknown host";
        } else {
            message = e.getClass().getName();
            if (LOG.isDebugEnabled()) {
                LOG.debug("Exception while fetching {}", url, e);
            } else {
                LOG.info("Exception while fetching {} -> {}", url, message);
            }
        }

        if (metadata.size() == 0) {
            metadata = new Metadata();
        }
        // add the reason of the failure in the metadata
        metadata.setValue("fetch.exception", message);
        eventCounter.scope("exception").incrBy(1);
        return new Outcome(Status.FETCH_ERROR, metadata, false, null, NO_KEY_VALUES);
    }
}
