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

package org.apache.stormcrawler.urlfrontier;

public final class Constants {
    private Constants() {}

    // General
    public static final String URLFRONTIER_ADDRESS_KEY = "urlfrontier.address";
    public static final String URLFRONTIER_HOST_KEY = "urlfrontier.host";
    public static final String URLFRONTIER_PORT_KEY = "urlfrontier.port";
    public static final String URLFRONTIER_DEFAULT_HOST = "localhost";
    public static final int URLFRONTIER_DEFAULT_PORT = 7071;

    // Spout
    public static final String URLFRONTIER_MAX_URLS_PER_BUCKET_KEY =
            "urlfrontier.max.urls.per.bucket";
    public static final String URLFRONTIER_MAX_BUCKETS_KEY = "urlfrontier.max.buckets";
    public static final String URLFRONTIER_DELAY_REQUESTABLE_KEY = "urlfrontier.delay.requestable";

    // StatusUpdater
    public static final String URLFRONTIER_CACHE_EXPIREAFTER_SEC_KEY =
            "urlfrontier.cache.expireafter.sec";
    public static final String URLFRONTIER_MAX_MESSAGES_IN_FLIGHT_KEY =
            "urlfrontier.max.messages.in.flight";
    public static final String URLFRONTIER_THROTTLING_TIME_MS_KEY =
            "urlfrontier.throttling.time.msec";
    public static final String URLFRONTIER_UPDATER_MAX_MESSAGES_KEY =
            "urlfrontier.updater.max.messages";
    public static final String URLFRONTIER_CRAWL_ID_KEY = "urlfrontier.crawlid";

    /**
     * Maximum delay in seconds honoured when a server requests a back-off via the Retry-After HTTP
     * response header. {@code -1} disables the cap. Defaults to 86400 (24h).
     */
    public static final String URLFRONTIER_MAX_RETRY_AFTER_KEY = "urlfrontier.max.retry.after";

    public static final int URLFRONTIER_MAX_RETRY_AFTER_DEFAULT = 86400;

    // QueueRegulatorBolt adaptive back-off (#867, #1106)

    /**
     * First block duration in seconds applied when a host rate-limits without a usable Retry-After
     * header. Defaults to 60; 0 disables the adaptive back-off while keeping the explicit
     * Retry-After handling.
     */
    public static final String URLFRONTIER_BACKOFF_BASE_KEY = "urlfrontier.backoff.base.secs";

    public static final int URLFRONTIER_BACKOFF_BASE_DEFAULT = 60;

    /**
     * Multiplicative increase applied to the block duration on each new pressure event for the same
     * host. Defaults to 2.
     */
    public static final String URLFRONTIER_BACKOFF_FACTOR_KEY = "urlfrontier.backoff.factor";

    public static final float URLFRONTIER_BACKOFF_FACTOR_DEFAULT = 2.0f;

    /**
     * Upper bound in seconds for the escalated block duration. Defaults to 86400 (24h), the same
     * spirit as {@link #URLFRONTIER_MAX_RETRY_AFTER_KEY} for the explicit-header case.
     */
    public static final String URLFRONTIER_BACKOFF_MAX_KEY = "urlfrontier.backoff.max.secs";

    public static final int URLFRONTIER_BACKOFF_MAX_DEFAULT = 86400;

    /**
     * Quiet window in seconds, counted from the end of the host's block, after which its escalation
     * is forgotten and the next pressure event starts again from the base duration. Defaults to
     * 1800, matching Nutch's {@code fetcher.exceptions.per.queue.clear.after}.
     */
    public static final String URLFRONTIER_BACKOFF_DECAY_KEY = "urlfrontier.backoff.decay.secs";

    public static final int URLFRONTIER_BACKOFF_DECAY_DEFAULT = 1800;

    /**
     * Whether fetch exceptions (timeouts, connection failures) count as pressure events and
     * escalate the host's block like a headerless rate-limit response. Defaults to false.
     */
    public static final String URLFRONTIER_BACKOFF_ON_EXCEPTIONS_KEY =
            "urlfrontier.backoff.on.exceptions";

    public static final boolean URLFRONTIER_BACKOFF_ON_EXCEPTIONS_DEFAULT = false;

    /**
     * Random extra fraction added to every computed block duration, so that hosts blocked on the
     * same schedule do not all expire together. Defaults to 0.1; 0 disables jitter.
     */
    public static final String URLFRONTIER_BACKOFF_JITTER_KEY = "urlfrontier.backoff.jitter";

    public static final float URLFRONTIER_BACKOFF_JITTER_DEFAULT = 0.1f;

    /**
     * HTTP status codes treated as a rate-limit signal by the {@link QueueRegulatorBolt}: with a
     * usable Retry-After header the requested delay is honoured, without one the adaptive back-off
     * escalates. Defaults to 429 and 503.
     */
    public static final String URLFRONTIER_BACKOFF_STATUS_CODES_KEY =
            "urlfrontier.backoff.status.codes";
}
