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

package org.apache.stormcrawler.filtering;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.filtering.adaptive.AdaptiveURLNormalizer;
import org.apache.stormcrawler.filtering.adaptive.CanonicalRules;
import org.apache.stormcrawler.util.URLUtil;
import org.junit.jupiter.api.Test;

/**
 * <i>sid</i> plays the parameter a site drops from its canonical tags, <i>pid</i> the one it keeps.
 * Neither is in the protected list, unlike <code>id</code>.
 */
class AdaptiveURLNormalizerTest {

    private static final AtomicInteger STORE_COUNTER = new AtomicInteger();

    /** Rules are shared per JVM and per name, so every test needs a store of its own. */
    private CanonicalRules rules;

    private AdaptiveURLNormalizer createFilter(Map<String, Object> conf) {
        AdaptiveURLNormalizer filter = new AdaptiveURLNormalizer();
        ObjectNode filterParams = new ObjectNode(JsonNodeFactory.instance);
        String store = "test-" + STORE_COUNTER.incrementAndGet();
        filterParams.put("store", store);
        filter.configure(conf, filterParams);
        rules = CanonicalRules.getInstance(conf, store);
        return filter;
    }

    private AdaptiveURLNormalizer createFilter() {
        return createFilter(new HashMap<>());
    }

    /** Simulates the parsing of a page having the given canonical tag. */
    private String observe(AdaptiveURLNormalizer filter, String pageUrl, String canonicalValue)
            throws MalformedURLException {
        return observe(filter, pageUrl, "canonical", canonicalValue);
    }

    private String observe(
            AdaptiveURLNormalizer filter, String pageUrl, String metadataKey, String canonicalValue)
            throws MalformedURLException {
        Metadata metadata = new Metadata();
        if (canonicalValue != null) {
            metadata.setValue(metadataKey, canonicalValue);
        }
        return filter.filter(URLUtil.toURL(pageUrl), metadata, "http://example.com/seed");
    }

    /** Applies the rules learnt so far without providing any new evidence. */
    private String apply(AdaptiveURLNormalizer filter, String url) {
        return filter.filter(null, null, url);
    }

    /** Observes pages whose canonical drops <i>sid</i> but keeps <i>pid</i>. */
    private void observeSessionParam(AdaptiveURLNormalizer filter, int observations)
            throws MalformedURLException {
        for (int i = 0; i < observations; i++) {
            observe(
                    filter,
                    "http://example.com/page" + i + "?pid=" + i + "&sid=abc" + i,
                    "http://example.com/page" + i + "?pid=" + i);
        }
    }

    @Test
    void testUnchangedWithoutEvidence() {
        AdaptiveURLNormalizer filter = createFilter();
        assertEquals(
                "http://example.com/page?pid=1&sid=abc",
                apply(filter, "http://example.com/page?pid=1&sid=abc"));
    }

    @Test
    void testNoEvidenceWithoutCanonical() throws MalformedURLException {
        AdaptiveURLNormalizer filter = createFilter();
        for (int i = 0; i < 20; i++) {
            observe(filter, "http://example.com/page" + i + "?pid=" + i + "&sid=abc" + i, null);
        }
        assertEquals(
                "http://example.com/other?pid=9&sid=zzz",
                apply(filter, "http://example.com/other?pid=9&sid=zzz"));
    }

    @Test
    void testRemovesIrrelevantParameter() throws MalformedURLException {
        AdaptiveURLNormalizer filter = createFilter();
        observeSessionParam(filter, 5);
        assertEquals(
                "http://example.com/other?pid=9",
                apply(filter, "http://example.com/other?pid=9&sid=zzz"));
    }

    @Test
    void testNotAppliedBelowMinObservations() throws MalformedURLException {
        AdaptiveURLNormalizer filter = createFilter();
        observeSessionParam(filter, 4);
        assertEquals(
                "http://example.com/other?pid=9&sid=zzz",
                apply(filter, "http://example.com/other?pid=9&sid=zzz"));
    }

    @Test
    void testMinObservationsIsConfigurable() throws MalformedURLException {
        Map<String, Object> conf = new HashMap<>();
        conf.put(CanonicalRules.MIN_OBSERVATIONS_PARAM, 2);
        conf.put(CanonicalRules.MIN_DISTINCT_PATHS_PARAM, 2);
        AdaptiveURLNormalizer filter = createFilter(conf);
        observeSessionParam(filter, 2);
        assertEquals(
                "http://example.com/other?pid=9",
                apply(filter, "http://example.com/other?pid=9&sid=zzz"));
    }

    @Test
    void testParameterKeptByCanonicalIsNeverRemoved() throws MalformedURLException {
        AdaptiveURLNormalizer filter = createFilter();
        observeSessionParam(filter, 20);
        assertEquals(
                "http://example.com/other?pid=9", apply(filter, "http://example.com/other?pid=9"));
    }

    @Test
    void testCanonicalIdenticalToSourceKeepsEverything() throws MalformedURLException {
        AdaptiveURLNormalizer filter = createFilter();
        for (int i = 0; i < 10; i++) {
            String page = "http://example.com/page" + i + "?pid=" + i + "&sid=abc" + i;
            observe(filter, page, page);
        }
        assertEquals(
                "http://example.com/other?pid=9&sid=zzz",
                apply(filter, "http://example.com/other?pid=9&sid=zzz"));
    }

    @Test
    void testConfidenceThreshold() throws MalformedURLException {
        Map<String, Object> conf = new HashMap<>();
        conf.put(CanonicalRules.MIN_OBSERVATIONS_PARAM, 4);
        conf.put(CanonicalRules.MIN_DISTINCT_PATHS_PARAM, 3);
        conf.put(CanonicalRules.CONFIDENCE_PARAM, 0.75d);
        AdaptiveURLNormalizer filter = createFilter(conf);

        // sid: dropped 3 times out of 4 -> 0.75, at the threshold
        // ref: dropped 2 times out of 4 -> 0.5, below the threshold
        observe(filter, "http://example.com/a?sid=x&ref=1", "http://example.com/a");
        observe(filter, "http://example.com/b?sid=x&ref=1", "http://example.com/b");
        observe(filter, "http://example.com/c?sid=x&ref=1", "http://example.com/c?ref=1");
        observe(filter, "http://example.com/d?sid=x&ref=1", "http://example.com/d?sid=x&ref=1");

        assertEquals(
                "http://example.com/e?ref=3", apply(filter, "http://example.com/e?sid=x&ref=3"));
    }

    @Test
    void testEvidenceIsScopedToTheHost() throws MalformedURLException {
        AdaptiveURLNormalizer filter = createFilter();
        observeSessionParam(filter, 5);
        assertEquals(
                "http://another.com/other?pid=9&sid=zzz",
                apply(filter, "http://another.com/other?pid=9&sid=zzz"));
        assertEquals(
                "http://sub.example.com/other?pid=9&sid=zzz",
                apply(filter, "http://sub.example.com/other?pid=9&sid=zzz"));
    }

    @Test
    void testEvidenceCanBeScopedToTheDomain() throws MalformedURLException {
        Map<String, Object> conf = new HashMap<>();
        conf.put(CanonicalRules.SCOPE_PARAM, "domain");
        AdaptiveURLNormalizer filter = createFilter(conf);
        for (int i = 0; i < 5; i++) {
            observe(
                    filter,
                    "http://www.example.com/page" + i + "?pid=" + i + "&sid=abc" + i,
                    "http://www.example.com/page" + i + "?pid=" + i);
        }
        assertEquals(
                "http://shop.example.com/other?pid=9",
                apply(filter, "http://shop.example.com/other?pid=9&sid=zzz"));
        assertEquals(
                "http://another.com/other?pid=9&sid=zzz",
                apply(filter, "http://another.com/other?pid=9&sid=zzz"));
    }

    @Test
    void testHostIsCaseInsensitive() throws MalformedURLException {
        AdaptiveURLNormalizer filter = createFilter();
        observeSessionParam(filter, 5);
        assertEquals(
                "http://EXAMPLE.com/other?pid=9",
                apply(filter, "http://EXAMPLE.com/other?pid=9&sid=zzz"));
    }

    @Test
    void testCanonicalOnAnotherPathIsIgnored() throws MalformedURLException {
        AdaptiveURLNormalizer filter = createFilter();
        for (int i = 0; i < 10; i++) {
            observe(
                    filter,
                    "http://example.com/page" + i + "?pid=" + i + "&sid=abc" + i,
                    "http://example.com/canonical" + i);
        }
        assertEquals(
                "http://example.com/other?pid=9&sid=zzz",
                apply(filter, "http://example.com/other?pid=9&sid=zzz"));
    }

    @Test
    void testCanonicalOnAnotherHostIsIgnored() throws MalformedURLException {
        AdaptiveURLNormalizer filter = createFilter();
        for (int i = 0; i < 10; i++) {
            observe(
                    filter,
                    "http://example.com/page" + i + "?pid=" + i + "&sid=abc" + i,
                    "http://mirror.example.com/page" + i + "?pid=" + i);
        }
        assertEquals(
                "http://example.com/other?pid=9&sid=zzz",
                apply(filter, "http://example.com/other?pid=9&sid=zzz"));
    }

    @Test
    void testRelativeCanonicalIsResolved() throws MalformedURLException {
        AdaptiveURLNormalizer filter = createFilter();
        for (int i = 0; i < 5; i++) {
            observe(
                    filter,
                    "http://example.com/page" + i + "?pid=" + i + "&sid=abc" + i,
                    "/page" + i + "?pid=" + i);
        }
        assertEquals(
                "http://example.com/other?pid=9",
                apply(filter, "http://example.com/other?pid=9&sid=zzz"));
    }

    @Test
    void testPureQueryCanonicalIsResolved() throws MalformedURLException {
        AdaptiveURLNormalizer filter = createFilter();
        for (int i = 0; i < 5; i++) {
            observe(
                    filter,
                    "http://example.com/page" + i + "?pid=" + i + "&sid=abc" + i,
                    "?pid=" + i);
        }
        assertEquals(
                "http://example.com/other?pid=9",
                apply(filter, "http://example.com/other?pid=9&sid=zzz"));
    }

    @Test
    void testInvalidCanonicalIsIgnored() throws MalformedURLException {
        AdaptiveURLNormalizer filter = createFilter();
        for (int i = 0; i < 10; i++) {
            observe(filter, "http://example.com/page" + i + "?pid=" + i + "&sid=abc" + i, ":::");
        }
        assertEquals(
                "http://example.com/other?pid=9&sid=zzz",
                apply(filter, "http://example.com/other?pid=9&sid=zzz"));
    }

    @Test
    void testWholeQueryStringCanBeRemoved() throws MalformedURLException {
        AdaptiveURLNormalizer filter = createFilter();
        for (int i = 0; i < 5; i++) {
            observe(
                    filter,
                    "http://example.com/page" + i + "?sid=abc" + i,
                    "http://example.com/page" + i);
        }
        assertEquals("http://example.com/other", apply(filter, "http://example.com/other?sid=zzz"));
    }

    @Test
    void testFragmentAndEncodingArePreserved() throws MalformedURLException {
        AdaptiveURLNormalizer filter = createFilter();
        observeSessionParam(filter, 5);
        assertEquals(
                "http://example.com/other?pid=a%20b#top",
                apply(filter, "http://example.com/other?sid=zzz&pid=a%20b#top"));
    }

    @Test
    void testQuestionMarkInsideFragmentIsNotAQueryString() throws MalformedURLException {
        AdaptiveURLNormalizer filter = createFilter();
        observeSessionParam(filter, 5);
        assertEquals(
                "http://example.com/other#anchor?sid=zzz",
                apply(filter, "http://example.com/other#anchor?sid=zzz"));
    }

    @Test
    void testEmptyPathIsGivenASlash() throws MalformedURLException {
        AdaptiveURLNormalizer filter = createFilter();
        observeSessionParam(filter, 5);
        // BasicURLNormalizer would store this URL as http://example.com/?pid=2
        assertEquals("http://example.com/?pid=2", apply(filter, "http://example.com?sid=1&pid=2"));
    }

    @Test
    void testOnlyTheQueryStringIsRewritten() throws MalformedURLException {
        AdaptiveURLNormalizer filter = createFilter();
        observeSessionParam(filter, 5);
        // the space would be escaped by URLUtil.toURL: the rest must be left as it was
        assertEquals(
                "http://example.com/a b?pid=2", apply(filter, "http://example.com/a b?sid=1&pid=2"));
    }

    @Test
    void testNonDefaultPortIsPreserved() throws MalformedURLException {
        AdaptiveURLNormalizer filter = createFilter();
        for (int i = 0; i < 5; i++) {
            observe(
                    filter,
                    "http://example.com:8080/page" + i + "?pid=" + i + "&sid=abc" + i,
                    "http://example.com:8080/page" + i + "?pid=" + i);
        }
        assertEquals(
                "http://example.com:8080/other?pid=9",
                apply(filter, "http://example.com:8080/other?pid=9&sid=zzz"));
    }

    @Test
    void testCanonicalOnAnotherPortIsIgnored() throws MalformedURLException {
        AdaptiveURLNormalizer filter = createFilter();
        for (int i = 0; i < 10; i++) {
            observe(
                    filter,
                    "http://example.com/page" + i + "?pid=" + i + "&sid=abc" + i,
                    "http://example.com:8080/page" + i + "?pid=" + i);
        }
        assertEquals(
                "http://example.com/other?pid=9&sid=zzz",
                apply(filter, "http://example.com/other?pid=9&sid=zzz"));
    }

    @Test
    void testDefaultPortMatchesImplicitOne() throws MalformedURLException {
        AdaptiveURLNormalizer filter = createFilter();
        for (int i = 0; i < 5; i++) {
            observe(
                    filter,
                    "http://example.com:80/page" + i + "?pid=" + i + "&sid=abc" + i,
                    "http://example.com/page" + i + "?pid=" + i);
        }
        assertEquals(
                "http://example.com/other?pid=9",
                apply(filter, "http://example.com/other?pid=9&sid=zzz"));
    }

    @Test
    void testEncodedParameterNames() throws MalformedURLException {
        AdaptiveURLNormalizer filter = createFilter();
        for (int i = 0; i < 5; i++) {
            observe(
                    filter,
                    "http://example.com/page" + i + "?session%20id=abc" + i,
                    "http://example.com/page" + i);
        }
        assertEquals(
                "http://example.com/other",
                apply(filter, "http://example.com/other?session%20id=zzz"));
    }

    @Test
    void testAPageIsOnlyCountedOnce() throws MalformedURLException {
        AdaptiveURLNormalizer filter = createFilter();
        String page = "http://example.com/page?pid=1&sid=abc";
        String canonical = "http://example.com/page?pid=1";
        // the filter is called once per outlink of the same page
        for (int i = 0; i < 20; i++) {
            observe(filter, page, canonical);
        }
        assertEquals(
                "http://example.com/other?pid=9&sid=zzz",
                apply(filter, "http://example.com/other?pid=9&sid=zzz"));
    }

    @Test
    void testAPageIsOnlyCountedOnceAcrossRefetches() throws MalformedURLException {
        AdaptiveURLNormalizer filter = createFilter();
        // the same two pages seen again and again, as would happen over successive fetch cycles
        for (int round = 0; round < 10; round++) {
            for (int page = 0; page < 2; page++) {
                observe(
                        filter,
                        "http://example.com/page" + page + "?pid=1&sid=abc",
                        "http://example.com/page" + page + "?pid=1");
            }
        }
        assertEquals(
                "http://example.com/other?pid=9&sid=zzz",
                apply(filter, "http://example.com/other?pid=9&sid=zzz"));
    }

    @Test
    void testPaginationSurvivesSelfReferencingCanonicals() throws MalformedURLException {
        AdaptiveURLNormalizer filter = createFilter();
        // every page of a listing declaring the bare listing as canonical
        for (int i = 2; i < 30; i++) {
            observe(filter, "http://example.com/list?offset=" + i, "http://example.com/list");
        }
        assertEquals(
                "http://example.com/list?offset=7", apply(filter, "http://example.com/list?offset=7"));
    }

    @Test
    void testEvidenceFromASinglePathIsNotEnough() throws MalformedURLException {
        AdaptiveURLNormalizer filter = createFilter();
        // sid is not protected, but all the evidence comes from the same path
        for (int i = 0; i < 30; i++) {
            observe(filter, "http://example.com/list?sid=" + i, "http://example.com/list");
        }
        assertEquals(
                "http://example.com/list?sid=7", apply(filter, "http://example.com/list?sid=7"));
    }

    @Test
    void testDistinctPathsRequirementIsConfigurable() throws MalformedURLException {
        Map<String, Object> conf = new HashMap<>();
        conf.put(CanonicalRules.MIN_DISTINCT_PATHS_PARAM, 1);
        AdaptiveURLNormalizer filter = createFilter(conf);
        for (int i = 0; i < 5; i++) {
            observe(filter, "http://example.com/list?sid=" + i, "http://example.com/list");
        }
        assertEquals("http://example.com/list", apply(filter, "http://example.com/list?sid=7"));
    }

    @Test
    void testProtectedParametersAreNeverRemoved() throws MalformedURLException {
        AdaptiveURLNormalizer filter = createFilter();
        for (int i = 0; i < 20; i++) {
            observe(
                    filter,
                    "http://example.com/page" + i + "?page=2&sid=abc" + i,
                    "http://example.com/page" + i);
        }
        // sid was learnt, page is protected even though the evidence is identical
        assertEquals(
                "http://example.com/other?page=2",
                apply(filter, "http://example.com/other?page=2&sid=zzz"));
    }

    @Test
    void testProtectedParametersCanBeOverridden() throws MalformedURLException {
        Map<String, Object> conf = new HashMap<>();
        conf.put(CanonicalRules.PROTECTED_PARAMS_PARAM, Collections.emptyList());
        AdaptiveURLNormalizer filter = createFilter(conf);
        for (int i = 0; i < 5; i++) {
            observe(
                    filter,
                    "http://example.com/page" + i + "?page=2&sid=abc" + i,
                    "http://example.com/page" + i);
        }
        assertEquals("http://example.com/other", apply(filter, "http://example.com/other?page=2"));
    }

    @Test
    void testProtectedParametersCanBeListed() throws MalformedURLException {
        Map<String, Object> conf = new HashMap<>();
        conf.put(CanonicalRules.PROTECTED_PARAMS_PARAM, Arrays.asList("sid"));
        AdaptiveURLNormalizer filter = createFilter(conf);
        observeSessionParam(filter, 20);
        assertEquals(
                "http://example.com/other?pid=9&sid=zzz",
                apply(filter, "http://example.com/other?pid=9&sid=zzz"));
    }

    @Test
    void testAnEstablishedRuleIsNeverWithdrawn() throws MalformedURLException {
        AdaptiveURLNormalizer filter = createFilter();
        observeSessionParam(filter, 5);
        assertEquals(
                "http://example.com/other?pid=9",
                apply(filter, "http://example.com/other?pid=9&sid=zzz"));

        // enough contrary evidence to drop the ratio below the threshold: the rule must stand
        for (int i = 100; i < 200; i++) {
            String page = "http://example.com/page" + i + "?pid=" + i + "&sid=abc" + i;
            observe(filter, page, page);
        }
        assertEquals(
                "http://example.com/other?pid=9",
                apply(filter, "http://example.com/other?pid=9&sid=zzz"));
    }

    @Test
    void testNullSourceIsHandled() throws MalformedURLException {
        AdaptiveURLNormalizer filter = createFilter();
        assertEquals(
                "http://example.com/other?sid=zzz",
                filter.filter(null, new Metadata(), "http://example.com/other?sid=zzz"));
        assertEquals(
                "http://example.com/other?sid=zzz",
                filter.filter(
                        URLUtil.toURL("http://example.com/page?sid=1"),
                        null,
                        "http://example.com/other?sid=zzz"));
    }

    @Test
    void testMalformedURLIsLeftUntouched() throws MalformedURLException {
        AdaptiveURLNormalizer filter = createFilter();
        observeSessionParam(filter, 5);
        String malformed = "this is not a URL?sid=1";
        assertEquals(malformed, apply(filter, malformed));
        assertEquals("mailto:someone@example.com", apply(filter, "mailto:someone@example.com"));
    }

    @Test
    void testCustomCanonicalMetadataKey() throws MalformedURLException {
        Map<String, Object> conf = new HashMap<>();
        conf.put(CanonicalRules.CANONICAL_KEY_PARAM, "parse.canonical");
        AdaptiveURLNormalizer filter = createFilter(conf);
        for (int i = 0; i < 5; i++) {
            observe(
                    filter,
                    "http://example.com/page" + i + "?pid=" + i + "&sid=abc" + i,
                    "parse.canonical",
                    "http://example.com/page" + i + "?pid=" + i);
        }
        assertEquals(
                "http://example.com/other?pid=9",
                apply(filter, "http://example.com/other?pid=9&sid=zzz"));
    }

    @Test
    void testWeakestParameterIsDiscardedWhenFull() throws MalformedURLException {
        Map<String, Object> conf = new HashMap<>();
        conf.put(CanonicalRules.MAX_PARAMS_PARAM, 2);
        AdaptiveURLNormalizer filter = createFilter(conf);

        // per-page tokens as parameter names would otherwise fill the slots for good
        for (int i = 0; i < 20; i++) {
            observe(
                    filter,
                    "http://example.com/junk" + i + "?token" + i + "=x&nonce" + i + "=y",
                    "http://example.com/junk" + i);
        }
        for (int i = 0; i < 5; i++) {
            observe(
                    filter,
                    "http://example.com/page" + i + "?sid=abc" + i,
                    "http://example.com/page" + i);
        }
        assertEquals("http://example.com/other", apply(filter, "http://example.com/other?sid=zzz"));
    }

    @Test
    void testNumberOfTrackedSitesIsBounded() throws MalformedURLException {
        Map<String, Object> conf = new HashMap<>();
        conf.put(CanonicalRules.MAX_SCOPES_PARAM, 1);
        AdaptiveURLNormalizer filter = createFilter(conf);

        observeSessionParam(filter, 5);
        for (int i = 0; i < 5; i++) {
            observe(
                    filter,
                    "http://another.com/page" + i + "?sid=abc" + i,
                    "http://another.com/page" + i);
        }

        assertEquals(1L, rules.getTrackedScopes(), "the second host should have evicted the first");

        boolean firstStillKnown =
                "http://example.com/other?pid=9"
                        .equals(apply(filter, "http://example.com/other?pid=9&sid=zzz"));
        boolean secondKnown =
                "http://another.com/other".equals(apply(filter, "http://another.com/other?sid=zzz"));
        assertFalse(
                firstStillKnown && secondKnown, "only one host should be retained with maxScopes 1");
        assertTrue(firstStillKnown || secondKnown, "the surviving host should still have its rule");
    }

    @Test
    void testInvalidConfidenceFallsBackToTheDefault() throws MalformedURLException {
        Map<String, Object> conf = new HashMap<>();
        conf.put(CanonicalRules.CONFIDENCE_PARAM, 1.5d);
        AdaptiveURLNormalizer filter = createFilter(conf);

        observeSessionParam(filter, 4);
        assertEquals(
                "http://example.com/other?pid=9&sid=zzz",
                apply(filter, "http://example.com/other?pid=9&sid=zzz"));

        observeSessionParam(filter, 5);
        assertEquals(
                "http://example.com/other?pid=9",
                apply(filter, "http://example.com/other?pid=9&sid=zzz"));
    }

    @Test
    void testSharedByConcurrentThreads() throws Exception {
        // the URL filters of a FetcherBolt are shared by all of its fetcher threads
        AdaptiveURLNormalizer filter = createFilter();
        final int threads = 4;
        final int pagesPerThread = 100;
        final ExecutorService pool = Executors.newFixedThreadPool(threads);
        final CountDownLatch start = new CountDownLatch(1);
        final AtomicReference<Throwable> failure = new AtomicReference<>();
        final List<Runnable> tasks = new ArrayList<>();

        for (int t = 0; t < threads; t++) {
            final int thread = t;
            tasks.add(
                    () -> {
                        try {
                            start.await();
                            for (int i = 0; i < pagesPerThread; i++) {
                                String page =
                                        "http://example.com/t" + thread + "p" + i + "?sid=abc" + i;
                                observe(filter, page, "http://example.com/t" + thread + "p" + i);
                                apply(filter, "http://example.com/other?sid=zzz");
                            }
                        } catch (Throwable e) {
                            failure.compareAndSet(null, e);
                        }
                    });
        }

        tasks.forEach(pool::execute);
        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS), "the threads should have finished");
        if (failure.get() != null) {
            throw new AssertionError("a thread failed", failure.get());
        }
        assertEquals("http://example.com/other", apply(filter, "http://example.com/other?sid=zzz"));
    }
}
