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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Expiry;
import com.github.benmanes.caffeine.cache.Ticker;
import java.time.Duration;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.protocol.ProtocolResponse;
import org.apache.stormcrawler.util.ConfUtils;
import org.apache.stormcrawler.util.RetryAfterParser;

/**
 * Per-host back-off state and decision logic for the {@link QueueRegulatorBolt}: given a
 * queue-stream tuple, decides whether and until when the host's frontier queue should be blocked.
 *
 * <p>Two kinds of signal are recognised, both derived from the tuple's metadata:
 *
 * <ul>
 *   <li><b>explicit</b> — a rate-limit status code with a usable Retry-After header: the requested
 *       delay is honoured, capped by {@code urlfrontier.max.retry.after};
 *   <li><b>pressure</b> — a rate-limit status code without a usable header (or, when {@code
 *       urlfrontier.backoff.on.exceptions} is set, a fetch exception): each pressure event blocks
 *       the host for a growing duration, {@code base × factor^n} capped at {@code max}, the same
 *       curve Nutch applies since 1.19 (NUTCH-2946). A host that stays quiet for {@code decay}
 *       seconds <b>after its block has expired</b> is forgiven and restarts from {@code base} — a
 *       blocked host is silent because it is blocked, so the quiet clock only starts once it may be
 *       fetched again.
 * </ul>
 *
 * <p>Two guards keep the escalation honest:
 *
 * <ul>
 *   <li><b>one event per window</b> — pressure signals arriving while the host's block is still
 *       active never escalate: URLs of that host already in flight when the block was raised fail
 *       one after the other, and counting each failure would let a single incident saturate the cap
 *       (multiple losses inside one RTT are one congestion event, RFC 5681);
 *   <li><b>only-extend</b> — {@code blockQueueUntil} overwrites, and signals race: a block is only
 *       ever replaced by a later one, never shrunk.
 * </ul>
 *
 * <p>Signals suppressed by either guard still return the <b>stored</b> block end, so the caller
 * re-asserts the same block at the frontier: the RPC is fire-and-forget, and a lost call would
 * otherwise leave the frontier unblocked while this state believes the host is covered. The
 * repeated call is idempotent and self-limiting — once the frontier applies the block, the host
 * stops being served and the signals stop.
 *
 * <p>A random fraction ({@code urlfrontier.backoff.jitter}) is added to every computed duration so
 * that hosts blocked on the same schedule do not all expire together.
 *
 * <p>All per-host state lives in a single cache entry that expires {@code decay} seconds after the
 * block end, which bounds the memory to the hosts blocked recently. State is soft: it restarts
 * empty, escalation resumes from {@code base} on the next signal. Not thread-safe; a bolt task
 * calls {@link #blockUntilFor} from a single thread.
 */
final class HostBackoff {

    /** Metadata key set by the FetcherBolt with the HTTP status code of the fetch. */
    static final String STATUS_CODE_KEY = "fetch.statusCode";

    /**
     * Metadata key set by the FetcherBolt with the reason of a fetch failure. The FetcherBolt
     * removes it before every fetch, so its presence means the last cycle failed — unlike {@link
     * #STATUS_CODE_KEY}, which lingers from the previous response and is stale on such tuples.
     */
    static final String EXCEPTION_KEY = "fetch.exception";

    /** Name of the Retry-After HTTP header, lower-cased as stored by the protocol layer. */
    static final String RETRY_AFTER_HEADER = "retry-after";

    /**
     * Queue key used by the status updaters when no partition key can be derived from a URL. The
     * queue is shared by unrelated URLs, so it is never blocked.
     */
    static final String DEFAULT_QUEUE_KEY = "_DEFAULT_";

    private static final Set<String> DEFAULT_STATUS_CODES = Set.of("429", "503");

    /**
     * Back-off state of one host: the escalated duration reached so far, the end of the current (or
     * last) block, and the entry's lifetime computed when it was written — block remainder plus the
     * decay window, so the escalation is forgotten {@code decay} seconds after the block ends, and
     * the block guard can never be evicted while the block runs.
     */
    private record HostState(long backoffSecs, long blockUntilSecs, long ttlSecs) {}

    private final Cache<String, HostState> hosts;

    /** Metadata key holding the Retry-After header, including the protocol prefix. */
    private final String retryAfterKey;

    /** Upper bound in ms for the honoured Retry-After delay; -1 means no cap. */
    private final long maxRetryAfterMs;

    private final Set<String> statusCodes;
    private final long baseSecs;
    private final float factor;
    private final long maxSecs;
    private final long decaySecs;
    private final boolean onExceptions;
    private final float jitter;
    private final Random jitterSource;

    HostBackoff(Map<String, Object> conf) {
        this(conf, Ticker.systemTicker(), new Random());
    }

    /** Visible for tests: a controllable ticker drives the expiry, a seeded source the jitter. */
    HostBackoff(Map<String, Object> conf, Ticker ticker, Random jitterSource) {
        // the protocol layer stores response headers in the metadata with this
        // prefix; the FetcherBolt merges them into the status metadata
        this.retryAfterKey =
                ConfUtils.getString(conf, ProtocolResponse.PROTOCOL_MD_PREFIX_PARAM, "")
                        + RETRY_AFTER_HEADER;
        long maxRetryAfterSecs =
                ConfUtils.getLong(
                        conf,
                        Constants.URLFRONTIER_MAX_RETRY_AFTER_KEY,
                        Constants.URLFRONTIER_MAX_RETRY_AFTER_DEFAULT);
        this.maxRetryAfterMs = maxRetryAfterSecs < 0 ? -1L : maxRetryAfterSecs * 1000L;
        this.statusCodes = statusCodesFromConf(conf);
        this.baseSecs =
                Math.max(
                        0L,
                        ConfUtils.getLong(
                                conf,
                                Constants.URLFRONTIER_BACKOFF_BASE_KEY,
                                Constants.URLFRONTIER_BACKOFF_BASE_DEFAULT));
        // a factor below 1 would de-escalate, negative jitter would shrink
        // blocks, a negative decay would evict a running block: clamp all
        // to avoid letting a typo break the invariants silently
        this.factor =
                Math.max(
                        1f,
                        ConfUtils.getFloat(
                                conf,
                                Constants.URLFRONTIER_BACKOFF_FACTOR_KEY,
                                Constants.URLFRONTIER_BACKOFF_FACTOR_DEFAULT));
        this.maxSecs =
                Math.max(
                        this.baseSecs,
                        ConfUtils.getLong(
                                conf,
                                Constants.URLFRONTIER_BACKOFF_MAX_KEY,
                                Constants.URLFRONTIER_BACKOFF_MAX_DEFAULT));
        this.decaySecs =
                Math.max(
                        0L,
                        ConfUtils.getLong(
                                conf,
                                Constants.URLFRONTIER_BACKOFF_DECAY_KEY,
                                Constants.URLFRONTIER_BACKOFF_DECAY_DEFAULT));
        this.onExceptions =
                ConfUtils.getBoolean(
                        conf,
                        Constants.URLFRONTIER_BACKOFF_ON_EXCEPTIONS_KEY,
                        Constants.URLFRONTIER_BACKOFF_ON_EXCEPTIONS_DEFAULT);
        this.jitter =
                Math.max(
                        0f,
                        ConfUtils.getFloat(
                                conf,
                                Constants.URLFRONTIER_BACKOFF_JITTER_KEY,
                                Constants.URLFRONTIER_BACKOFF_JITTER_DEFAULT));
        this.jitterSource = jitterSource;
        this.hosts =
                Caffeine.newBuilder()
                        .expireAfter(
                                Expiry.<String, HostState>writing(
                                        (key, state) -> Duration.ofSeconds(state.ttlSecs())))
                        .ticker(ticker)
                        .build();
    }

    /**
     * Decides whether a queue-stream tuple calls for blocking the host's frontier queue, updating
     * the per-host state accordingly.
     *
     * @return the absolute time to block the queue until, in epoch seconds — either a new (or
     *     extended) block, or the unchanged stored one re-asserted so a previously lost call
     *     converges — or {@code -1} if the tuple carries no back-off signal
     */
    long blockUntilFor(String key, Metadata metadata, long nowMs) {
        if (metadata == null || DEFAULT_QUEUE_KEY.equals(key)) {
            return -1L;
        }
        long explicitDelayMs = -1L;
        if (metadata.getFirstValue(EXCEPTION_KEY) != null) {
            // the last fetch failed with an exception: any status code (and
            // Retry-After header) in the metadata is left over from a
            // previous response, so only the pressure path may apply
            if (!onExceptions) {
                return -1L;
            }
        } else {
            final String statusCode = metadata.getFirstValue(STATUS_CODE_KEY);
            if (statusCode == null || !statusCodes.contains(statusCode)) {
                return -1L;
            }
            explicitDelayMs = RetryAfterParser.parseDelay(metadata.getFirstValue(retryAfterKey));
            if (explicitDelayMs > 0 && maxRetryAfterMs >= 0 && explicitDelayMs > maxRetryAfterMs) {
                explicitDelayMs = maxRetryAfterMs;
            }
        }

        final long nowSecs = nowMs / 1000L;
        final HostState state = hosts.getIfPresent(key);
        final long currentBlockEnd = state == null ? -1L : state.blockUntilSecs();
        final boolean blockActive = currentBlockEnd > nowSecs;

        final long delayMs;
        long backoffSecs = state == null ? 0L : state.backoffSecs();
        if (explicitDelayMs > 0) {
            // explicit server signal: always considered, only-extend decides
            delayMs = explicitDelayMs;
        } else {
            // pressure: while the block is active this is the same congestion
            // event — do not escalate, re-assert the stored block instead
            if (blockActive) {
                return currentBlockEnd;
            }
            backoffSecs =
                    backoffSecs == 0L
                            ? baseSecs
                            : (long) Math.min(backoffSecs * factor, (double) maxSecs);
            if (backoffSecs <= 0) {
                return -1L;
            }
            // clamp: an absurdly large configured duration must not wrap
            // negative and silently skip the block
            delayMs = backoffSecs > Long.MAX_VALUE / 1000L ? Long.MAX_VALUE : backoffSecs * 1000L;
        }

        final long candidate = jitteredBlockUntilSecs(delayMs, nowMs);
        // only-extend: never shrink an active block, whatever kind set it —
        // but re-assert it, in case the call that raised it was lost
        if (candidate <= currentBlockEnd) {
            return blockActive ? currentBlockEnd : -1L;
        }
        hosts.put(key, new HostState(backoffSecs, candidate, entryTtlSecs(candidate, nowSecs)));
        return candidate;
    }

    /** Applies the jitter fraction to the delay and turns it into an epoch-seconds block end. */
    private long jitteredBlockUntilSecs(long delayMs, long nowMs) {
        final double jittered = delayMs * (1.0d + jitter * jitterSource.nextDouble());
        final long delay = jittered >= (double) Long.MAX_VALUE ? Long.MAX_VALUE : (long) jittered;
        long blockUntilMs;
        try {
            blockUntilMs = Math.addExact(nowMs, delay);
        } catch (ArithmeticException e) {
            blockUntilMs = Long.MAX_VALUE;
        }
        return blockUntilMs / 1000L;
    }

    /** Entry lifetime: the remainder of the block plus the decay window, saturated. */
    private long entryTtlSecs(long blockUntilSecs, long nowSecs) {
        final long remainder = blockUntilSecs - nowSecs;
        final long ttl = remainder + decaySecs;
        return ttl < remainder ? Long.MAX_VALUE : ttl;
    }

    private static Set<String> statusCodesFromConf(Map<String, Object> conf) {
        final Object value = conf.get(Constants.URLFRONTIER_BACKOFF_STATUS_CODES_KEY);
        if (value == null) {
            return DEFAULT_STATUS_CODES;
        }
        final Set<String> codes = new HashSet<>();
        if (value instanceof Collection<?> values) {
            // the codes are usually parsed from YAML as numbers
            for (Object code : values) {
                codes.add(String.valueOf(code));
            }
        } else {
            codes.add(String.valueOf(value));
        }
        return Set.copyOf(codes);
    }

    String retryAfterKey() {
        return retryAfterKey;
    }

    boolean onExceptions() {
        return onExceptions;
    }
}
