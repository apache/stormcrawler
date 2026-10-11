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
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.filtering.adaptive.AdaptiveURLNormalizer;
import org.apache.stormcrawler.filtering.adaptive.CanonicalRules;
import org.apache.stormcrawler.parse.ParseResult;
import org.apache.stormcrawler.parse.filter.CanonicalParamLearner;
import org.apache.stormcrawler.util.URLUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * <i>sid</i> plays the parameter a site drops from its canonical tags, <i>pid</i> the one it keeps.
 * Neither is in the protected list, unlike <code>id</code>. The evidence is fed through the
 * learner, as in a topology: the filter itself never learns.
 */
class AdaptiveURLNormalizerTest {

    private static final String HOST = "example.com";

    private final List<AdaptiveURLNormalizer> filters = new ArrayList<>();

    private final List<CanonicalParamLearner> learners = new ArrayList<>();

    /** The learner created along with the last filter. */
    private CanonicalParamLearner learner;

    private CanonicalRules rules;

    /** The rules are shared by the JVM: released after each test so that the next starts afresh. */
    @AfterEach
    void releaseRules() {
        filters.forEach(AdaptiveURLNormalizer::cleanup);
        learners.forEach(CanonicalParamLearner::cleanup);
        if (rules != null) {
            rules.release();
            rules = null;
        }
    }

    /** A filter and the learner feeding it, both on the given configuration. */
    private AdaptiveURLNormalizer createFilter(Map<String, Object> conf) {
        AdaptiveURLNormalizer filter = new AdaptiveURLNormalizer();
        filter.configure(conf, new ObjectNode(JsonNodeFactory.instance));
        filters.add(filter);
        learner = new CanonicalParamLearner();
        learner.configure(conf, new ObjectNode(JsonNodeFactory.instance));
        learners.add(learner);
        return filter;
    }

    private AdaptiveURLNormalizer createFilter() {
        return createFilter(new HashMap<>());
    }

    /** Simulates the parsing of a page having the given canonical tag. */
    private void observe(String pageUrl, String canonicalValue) {
        observe(pageUrl, "canonical", canonicalValue);
    }

    private void observe(String pageUrl, String metadataKey, String canonicalValue) {
        ParseResult parse = new ParseResult();
        Metadata metadata = new Metadata();
        if (canonicalValue != null) {
            metadata.setValue(metadataKey, canonicalValue);
        }
        parse.set(pageUrl, metadata);
        learner.filter(pageUrl, new byte[0], null, parse);
    }

    /** Applies the rules learnt so far. */
    private String apply(AdaptiveURLNormalizer filter, String url) {
        return filter.filter(null, null, url);
    }

    /** Observes pages whose canonical drops <i>sid</i> but keeps <i>pid</i>. */
    private void observeSessionParam(int observations) {
        for (int i = 0; i < observations; i++) {
            observe(
                    "http://example.com/page" + i + "?pid=" + i + "&sid=abc" + i,
                    "http://example.com/page" + i + "?pid=" + i);
        }
    }

    /**
     * sid dropped by 5 pages out of 6, ref by 3 out of 6, both on at least 3 distinct paths. The
     * pages keeping ref are interleaved: promotion is checked after every page, so the ratio of ref
     * must stay below 0.75 at every point, not just at the end.
     */
    private void observeMixedEvidence() {
        observe("http://example.com/a?sid=x&ref=1", "http://example.com/a");
        observe("http://example.com/d?sid=x&ref=1", "http://example.com/d?ref=1");
        observe("http://example.com/b?sid=x&ref=1", "http://example.com/b");
        observe("http://example.com/e?sid=x&ref=1", "http://example.com/e?ref=1");
        observe("http://example.com/c?sid=x&ref=1", "http://example.com/c");
        observe("http://example.com/f?sid=x&ref=1", "http://example.com/f?sid=x&ref=1");
    }

    @Test
    void testUnchangedWithoutEvidence() {
        AdaptiveURLNormalizer filter = createFilter();
        assertEquals(
                "http://example.com/page?pid=1&sid=abc",
                apply(filter, "http://example.com/page?pid=1&sid=abc"));
    }

    @Test
    void testNoEvidenceWithoutCanonical() {
        AdaptiveURLNormalizer filter = createFilter();
        for (int i = 0; i < 20; i++) {
            observe("http://example.com/page" + i + "?pid=" + i + "&sid=abc" + i, null);
        }
        assertEquals(
                "http://example.com/other?pid=9&sid=zzz",
                apply(filter, "http://example.com/other?pid=9&sid=zzz"));
    }

    @Test
    void testRemovesIrrelevantParameter() {
        AdaptiveURLNormalizer filter = createFilter();
        observeSessionParam(5);
        assertEquals(
                "http://example.com/other?pid=9",
                apply(filter, "http://example.com/other?pid=9&sid=zzz"));
    }

    @Test
    void testNotAppliedBelowMinObservations() {
        AdaptiveURLNormalizer filter = createFilter();
        observeSessionParam(4);
        assertEquals(
                "http://example.com/other?pid=9&sid=zzz",
                apply(filter, "http://example.com/other?pid=9&sid=zzz"));
    }

    @Test
    void testMinObservationsIsConfigurable() {
        Map<String, Object> conf = new HashMap<>();
        conf.put(CanonicalRules.MIN_OBSERVATIONS_PARAM, 2);
        conf.put(CanonicalRules.MIN_DISTINCT_PATHS_PARAM, 2);
        AdaptiveURLNormalizer filter = createFilter(conf);
        observeSessionParam(2);
        assertEquals(
                "http://example.com/other?pid=9",
                apply(filter, "http://example.com/other?pid=9&sid=zzz"));
    }

    @Test
    void testParameterKeptByCanonicalIsNeverRemoved() {
        AdaptiveURLNormalizer filter = createFilter();
        observeSessionParam(20);
        assertEquals(
                "http://example.com/other?pid=9", apply(filter, "http://example.com/other?pid=9"));
    }

    @Test
    void testCanonicalIdenticalToSourceKeepsEverything() {
        AdaptiveURLNormalizer filter = createFilter();
        for (int i = 0; i < 10; i++) {
            String page = "http://example.com/page" + i + "?pid=" + i + "&sid=abc" + i;
            observe(page, page);
        }
        assertEquals(
                "http://example.com/other?pid=9&sid=zzz",
                apply(filter, "http://example.com/other?pid=9&sid=zzz"));
    }

    @Test
    void testConfidenceThresholdRejectsAParameterKeptTooOften() {
        Map<String, Object> conf = new HashMap<>();
        conf.put(CanonicalRules.MIN_OBSERVATIONS_PARAM, 4);
        conf.put(CanonicalRules.MIN_DISTINCT_PATHS_PARAM, 3);
        conf.put(CanonicalRules.CONFIDENCE_PARAM, 0.75d);
        AdaptiveURLNormalizer filter = createFilter(conf);
        observeMixedEvidence();
        // ref meets the observations and the paths requirements: only its ratio holds it back
        assertEquals(
                "http://example.com/g?ref=3", apply(filter, "http://example.com/g?sid=x&ref=3"));
    }

    @Test
    void testConfidenceThresholdCanBeLowered() {
        Map<String, Object> conf = new HashMap<>();
        conf.put(CanonicalRules.MIN_OBSERVATIONS_PARAM, 4);
        conf.put(CanonicalRules.MIN_DISTINCT_PATHS_PARAM, 3);
        conf.put(CanonicalRules.CONFIDENCE_PARAM, 0.5d);
        AdaptiveURLNormalizer filter = createFilter(conf);
        observeMixedEvidence();
        assertEquals("http://example.com/g", apply(filter, "http://example.com/g?sid=x&ref=3"));
    }

    @Test
    void testEvidenceIsScopedToTheHost() {
        AdaptiveURLNormalizer filter = createFilter();
        observeSessionParam(5);
        assertEquals(
                "http://another.com/other?pid=9&sid=zzz",
                apply(filter, "http://another.com/other?pid=9&sid=zzz"));
        assertEquals(
                "http://sub.example.com/other?pid=9&sid=zzz",
                apply(filter, "http://sub.example.com/other?pid=9&sid=zzz"));
    }

    @Test
    void testEvidenceCanBeScopedToTheDomain() {
        Map<String, Object> conf = new HashMap<>();
        conf.put(CanonicalRules.SCOPE_PARAM, "domain");
        AdaptiveURLNormalizer filter = createFilter(conf);
        for (int i = 0; i < 5; i++) {
            observe(
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
    void testDomainScopeHonoursPrivateSuffixes() {
        Map<String, Object> conf = new HashMap<>();
        conf.put(CanonicalRules.SCOPE_PARAM, "domain");
        AdaptiveURLNormalizer filter = createFilter(conf);
        for (int i = 0; i < 5; i++) {
            observe(
                    "http://alice.github.io/page" + i + "?pid=" + i + "&sid=abc" + i,
                    "http://alice.github.io/page" + i + "?pid=" + i);
        }
        // github.io is a private suffix: these are two different sites
        assertEquals(
                "http://bob.github.io/other?pid=9&sid=zzz",
                apply(filter, "http://bob.github.io/other?pid=9&sid=zzz"));
        assertEquals(
                "http://alice.github.io/other?pid=9",
                apply(filter, "http://alice.github.io/other?pid=9&sid=zzz"));
    }

    @Test
    void testHostIsCaseInsensitive() {
        AdaptiveURLNormalizer filter = createFilter();
        observeSessionParam(5);
        assertEquals(
                "http://EXAMPLE.com/other?pid=9",
                apply(filter, "http://EXAMPLE.com/other?pid=9&sid=zzz"));
    }

    @Test
    void testCanonicalOnAnotherPathIsIgnored() {
        AdaptiveURLNormalizer filter = createFilter();
        for (int i = 0; i < 10; i++) {
            observe(
                    "http://example.com/page" + i + "?pid=" + i + "&sid=abc" + i,
                    "http://example.com/canonical" + i);
        }
        assertEquals(
                "http://example.com/other?pid=9&sid=zzz",
                apply(filter, "http://example.com/other?pid=9&sid=zzz"));
    }

    @Test
    void testCanonicalOnAnotherHostIsIgnored() {
        AdaptiveURLNormalizer filter = createFilter();
        for (int i = 0; i < 10; i++) {
            observe(
                    "http://example.com/page" + i + "?pid=" + i + "&sid=abc" + i,
                    "http://mirror.example.com/page" + i + "?pid=" + i);
        }
        assertEquals(
                "http://example.com/other?pid=9&sid=zzz",
                apply(filter, "http://example.com/other?pid=9&sid=zzz"));
    }

    @Test
    void testRelativeCanonicalIsResolved() {
        AdaptiveURLNormalizer filter = createFilter();
        for (int i = 0; i < 5; i++) {
            observe(
                    "http://example.com/page" + i + "?pid=" + i + "&sid=abc" + i,
                    "/page" + i + "?pid=" + i);
        }
        assertEquals(
                "http://example.com/other?pid=9",
                apply(filter, "http://example.com/other?pid=9&sid=zzz"));
    }

    @Test
    void testPureQueryCanonicalIsResolved() {
        AdaptiveURLNormalizer filter = createFilter();
        for (int i = 0; i < 5; i++) {
            observe("http://example.com/page" + i + "?pid=" + i + "&sid=abc" + i, "?pid=" + i);
        }
        assertEquals(
                "http://example.com/other?pid=9",
                apply(filter, "http://example.com/other?pid=9&sid=zzz"));
    }

    @Test
    void testInvalidCanonicalIsIgnored() {
        AdaptiveURLNormalizer filter = createFilter();
        for (int i = 0; i < 10; i++) {
            observe("http://example.com/page" + i + "?pid=" + i + "&sid=abc" + i, ":::");
        }
        assertEquals(
                "http://example.com/other?pid=9&sid=zzz",
                apply(filter, "http://example.com/other?pid=9&sid=zzz"));
    }

    @Test
    void testWholeQueryStringCanBeRemoved() {
        AdaptiveURLNormalizer filter = createFilter();
        for (int i = 0; i < 5; i++) {
            observe("http://example.com/page" + i + "?sid=abc" + i, "http://example.com/page" + i);
        }
        assertEquals("http://example.com/other", apply(filter, "http://example.com/other?sid=zzz"));
    }

    @Test
    void testFragmentAndEncodingArePreserved() {
        AdaptiveURLNormalizer filter = createFilter();
        observeSessionParam(5);
        assertEquals(
                "http://example.com/other?pid=a%20b#top",
                apply(filter, "http://example.com/other?sid=zzz&pid=a%20b#top"));
    }

    @Test
    void testQuestionMarkInsideFragmentIsNotAQueryString() {
        AdaptiveURLNormalizer filter = createFilter();
        observeSessionParam(5);
        assertEquals(
                "http://example.com/other#anchor?sid=zzz",
                apply(filter, "http://example.com/other#anchor?sid=zzz"));
    }

    @Test
    void testEmptyPathIsGivenASlash() {
        AdaptiveURLNormalizer filter = createFilter();
        observeSessionParam(5);
        // BasicURLNormalizer would store this URL as http://example.com/?pid=2
        assertEquals("http://example.com/?pid=2", apply(filter, "http://example.com?sid=1&pid=2"));
    }

    @Test
    void testOnlyTheQueryStringIsRewritten() {
        AdaptiveURLNormalizer filter = createFilter();
        observeSessionParam(5);
        // the space would be escaped by URLUtil.toURL: the rest must be left as it was
        assertEquals(
                "http://example.com/a b?pid=2",
                apply(filter, "http://example.com/a b?sid=1&pid=2"));
    }

    @Test
    void testSemicolonSeparatedParametersAreLearnt() {
        AdaptiveURLNormalizer filter = createFilter();
        for (int i = 0; i < 5; i++) {
            observe(
                    "http://example.com/page" + i + "?sid=abc" + i + ";pid=" + i,
                    "http://example.com/page" + i + "?pid=" + i);
        }
        assertEquals(
                "http://example.com/other?pid=9",
                apply(filter, "http://example.com/other?sid=zzz;pid=9"));
    }

    @Test
    void testSemicolonSeparatorsArePreserved() {
        AdaptiveURLNormalizer filter = createFilter();
        observeSessionParam(5);
        assertEquals(
                "http://example.com/x?item=7",
                apply(filter, "http://example.com/x?sid=abc;item=7"));
        assertEquals(
                "http://example.com/x?item=7",
                apply(filter, "http://example.com/x?item=7;sid=abc"));
        // each surviving pair keeps the separator which preceded it
        assertEquals(
                "http://example.com/x?a=1&b=3",
                apply(filter, "http://example.com/x?a=1;sid=2&b=3"));
        assertEquals(
                "http://example.com/x?a=1;b=3",
                apply(filter, "http://example.com/x?a=1&sid=2;b=3"));
    }

    @Test
    void testNonDefaultPortIsPreserved() {
        AdaptiveURLNormalizer filter = createFilter();
        for (int i = 0; i < 5; i++) {
            observe(
                    "http://example.com:8080/page" + i + "?pid=" + i + "&sid=abc" + i,
                    "http://example.com:8080/page" + i + "?pid=" + i);
        }
        assertEquals(
                "http://example.com:8080/other?pid=9",
                apply(filter, "http://example.com:8080/other?pid=9&sid=zzz"));
    }

    @Test
    void testCanonicalOnAnotherPortIsIgnored() {
        AdaptiveURLNormalizer filter = createFilter();
        for (int i = 0; i < 10; i++) {
            observe(
                    "http://example.com/page" + i + "?pid=" + i + "&sid=abc" + i,
                    "http://example.com:8080/page" + i + "?pid=" + i);
        }
        assertEquals(
                "http://example.com/other?pid=9&sid=zzz",
                apply(filter, "http://example.com/other?pid=9&sid=zzz"));
    }

    @Test
    void testDefaultPortMatchesImplicitOne() {
        AdaptiveURLNormalizer filter = createFilter();
        for (int i = 0; i < 5; i++) {
            observe(
                    "http://example.com:80/page" + i + "?pid=" + i + "&sid=abc" + i,
                    "http://example.com/page" + i + "?pid=" + i);
        }
        assertEquals(
                "http://example.com/other?pid=9",
                apply(filter, "http://example.com/other?pid=9&sid=zzz"));
    }

    @Test
    void testEncodedParameterNames() {
        AdaptiveURLNormalizer filter = createFilter();
        for (int i = 0; i < 5; i++) {
            observe(
                    "http://example.com/page" + i + "?session%20id=abc" + i,
                    "http://example.com/page" + i);
        }
        assertEquals(
                "http://example.com/other",
                apply(filter, "http://example.com/other?session%20id=zzz"));
    }

    @Test
    void testLongParameterNamesAreNotTracked() {
        Map<String, Object> conf = new HashMap<>();
        AdaptiveURLNormalizer filter = createFilter(conf);
        rules = CanonicalRules.getInstance(conf);
        String token = "x".repeat(CanonicalRules.MAX_PARAM_NAME_LENGTH + 1);
        for (int i = 0; i < 10; i++) {
            observe(
                    "http://example.com/page" + i + "?" + token + "=1",
                    "http://example.com/page" + i);
        }
        assertEquals(0, rules.getTrackedParams(HOST));
        assertEquals(
                "http://example.com/other?" + token + "=1",
                apply(filter, "http://example.com/other?" + token + "=1"));
    }

    @Test
    void testAPageIsOnlyCountedOnce() {
        AdaptiveURLNormalizer filter = createFilter();
        String page = "http://example.com/page?pid=1&sid=abc";
        String canonical = "http://example.com/page?pid=1";
        for (int i = 0; i < 20; i++) {
            observe(page, canonical);
        }
        assertEquals(
                "http://example.com/other?pid=9&sid=zzz",
                apply(filter, "http://example.com/other?pid=9&sid=zzz"));
    }

    @Test
    void testAPageIsOnlyCountedOnceAcrossRefetches() {
        AdaptiveURLNormalizer filter = createFilter();
        // the same two pages seen again and again, as would happen over successive fetch cycles
        for (int round = 0; round < 10; round++) {
            for (int page = 0; page < 2; page++) {
                observe(
                        "http://example.com/page" + page + "?pid=1&sid=abc",
                        "http://example.com/page" + page + "?pid=1");
            }
        }
        assertEquals(
                "http://example.com/other?pid=9&sid=zzz",
                apply(filter, "http://example.com/other?pid=9&sid=zzz"));
    }

    @Test
    void testPaginationSurvivesSelfReferencingCanonicals() {
        AdaptiveURLNormalizer filter = createFilter();
        // every page of a listing declaring the bare listing as canonical
        for (int i = 2; i < 30; i++) {
            observe("http://example.com/list?offset=" + i, "http://example.com/list");
        }
        assertEquals(
                "http://example.com/list?offset=7",
                apply(filter, "http://example.com/list?offset=7"));
    }

    @Test
    void testEvidenceFromASinglePathIsNotEnough() {
        AdaptiveURLNormalizer filter = createFilter();
        // sid is not protected, but all the evidence comes from the same path
        for (int i = 0; i < 30; i++) {
            observe("http://example.com/list?sid=" + i, "http://example.com/list");
        }
        assertEquals(
                "http://example.com/list?sid=7", apply(filter, "http://example.com/list?sid=7"));
    }

    @Test
    void testDistinctPathsRequirementIsConfigurable() {
        Map<String, Object> conf = new HashMap<>();
        conf.put(CanonicalRules.MIN_DISTINCT_PATHS_PARAM, 1);
        AdaptiveURLNormalizer filter = createFilter(conf);
        for (int i = 0; i < 5; i++) {
            observe("http://example.com/list?sid=" + i, "http://example.com/list");
        }
        assertEquals("http://example.com/list", apply(filter, "http://example.com/list?sid=7"));
    }

    @Test
    void testProtectedParametersAreNeverRemoved() {
        AdaptiveURLNormalizer filter = createFilter();
        for (int i = 0; i < 20; i++) {
            observe(
                    "http://example.com/page" + i + "?page=2&sid=abc" + i,
                    "http://example.com/page" + i);
        }
        // sid was learnt, page is protected even though the evidence is identical
        assertEquals(
                "http://example.com/other?page=2",
                apply(filter, "http://example.com/other?page=2&sid=zzz"));
    }

    @Test
    void testProtectedParametersAreMatchedRegardlessOfCase() {
        AdaptiveURLNormalizer filter = createFilter();
        for (int i = 0; i < 20; i++) {
            observe(
                    "http://example.com/page" + i + "?PageNo=2&Seite=3&sid=abc" + i,
                    "http://example.com/page" + i);
        }
        assertEquals(
                "http://example.com/other?PageNo=2&Seite=3",
                apply(filter, "http://example.com/other?PageNo=2&Seite=3&sid=zzz"));
    }

    @Test
    void testProtectedParametersCanBeOverridden() {
        Map<String, Object> conf = new HashMap<>();
        conf.put(CanonicalRules.PROTECTED_PARAMS_PARAM, Collections.emptyList());
        AdaptiveURLNormalizer filter = createFilter(conf);
        for (int i = 0; i < 5; i++) {
            observe(
                    "http://example.com/page" + i + "?page=2&sid=abc" + i,
                    "http://example.com/page" + i);
        }
        assertEquals("http://example.com/other", apply(filter, "http://example.com/other?page=2"));
    }

    @Test
    void testProtectedParametersCanBeListed() {
        Map<String, Object> conf = new HashMap<>();
        conf.put(CanonicalRules.PROTECTED_PARAMS_PARAM, Arrays.asList("SID"));
        AdaptiveURLNormalizer filter = createFilter(conf);
        observeSessionParam(20);
        assertEquals(
                "http://example.com/other?pid=9&sid=zzz",
                apply(filter, "http://example.com/other?pid=9&sid=zzz"));
    }

    @Test
    void testParameterKeptOnAPathIsNotRemovedThere() {
        AdaptiveURLNormalizer filter = createFilter();
        // the canonical of a listing keeps sid before the rule is established...
        observe("http://example.com/list?sid=keep", "http://example.com/list?sid=keep");
        // ...which takes enough pages for that one to weigh less than 10%...
        observeSessionParam(20);
        // ...and the one of the cart after
        observe("http://example.com/cart?sid=keep", "http://example.com/cart?sid=keep");

        assertEquals(
                "http://example.com/list?sid=zzz",
                apply(filter, "http://example.com/list?sid=zzz"));
        assertEquals(
                "http://example.com/cart?sid=zzz",
                apply(filter, "http://example.com/cart?sid=zzz"));
        // the rule stands everywhere else
        assertEquals("http://example.com/other", apply(filter, "http://example.com/other?sid=zzz"));
        assertEquals(
                "http://example.com/other?pid=9",
                apply(filter, "http://example.com/other?pid=9&sid=zzz"));
    }

    @Test
    void testParameterKeptOnManyPathsIsNeverRemoved() {
        Map<String, Object> conf = new HashMap<>();
        conf.put(CanonicalRules.CONFIDENCE_PARAM, 0.5d);
        AdaptiveURLNormalizer filter = createFilter(conf);
        observeSessionParam(10);
        assertEquals("http://example.com/other", apply(filter, "http://example.com/other?sid=zzz"));

        // kept on more paths than the rule remembers: too content-bearing to be trusted anywhere
        for (int i = 0; i <= CanonicalRules.MAX_KEPT_PATHS; i++) {
            observe(
                    "http://example.com/keep" + i + "?sid=x",
                    "http://example.com/keep" + i + "?sid=x");
        }
        assertEquals(
                "http://example.com/other?sid=zzz",
                apply(filter, "http://example.com/other?sid=zzz"));
    }

    @Test
    void testContraryEvidenceOnTheRatioDoesNotWithdrawARule() {
        AdaptiveURLNormalizer filter = createFilter();
        observeSessionParam(5);
        // a few pages keeping sid drop the ratio below the threshold: the rule stands elsewhere
        for (int i = 0; i < CanonicalRules.MAX_KEPT_PATHS; i++) {
            String page = "http://example.com/keep" + i + "?pid=" + i + "&sid=abc" + i;
            observe(page, page);
        }
        assertEquals(
                "http://example.com/other?pid=9",
                apply(filter, "http://example.com/other?pid=9&sid=zzz"));
    }

    @Test
    void testFilterDoesNotLearnFromTheMetadataItIsGiven() throws MalformedURLException {
        AdaptiveURLNormalizer filter = createFilter();
        // what the filter sees at best is the canonical of a previous fetch, in metadata.persist
        for (int i = 0; i < 10; i++) {
            Metadata metadata = new Metadata();
            metadata.setValue("canonical", "http://example.com/page" + i + "?pid=" + i);
            filter.filter(
                    URLUtil.toURL("http://example.com/page" + i + "?pid=" + i + "&sid=abc" + i),
                    metadata,
                    "http://example.com/seed");
        }
        assertEquals(
                "http://example.com/other?pid=9&sid=zzz",
                apply(filter, "http://example.com/other?pid=9&sid=zzz"));
    }

    @Test
    void testTheLearnerIsNotPreemptedByTheFilter() throws MalformedURLException {
        AdaptiveURLNormalizer filter = createFilter();
        for (int i = 0; i < 5; i++) {
            String page = "http://example.com/page" + i + "?pid=" + i + "&sid=abc" + i;
            // a stale canonical identical to the page is in the metadata when the outlinks
            // are filtered, the current one only reaches the learner afterwards
            Metadata metadata = new Metadata();
            metadata.setValue("canonical", page);
            filter.filter(URLUtil.toURL(page), metadata, "http://example.com/seed");
            observe(page, "http://example.com/page" + i + "?pid=" + i);
        }
        assertEquals(
                "http://example.com/other?pid=9",
                apply(filter, "http://example.com/other?pid=9&sid=zzz"));
    }

    @Test
    void testRedirectionToItselfWithAParameterIsLeftAlone() throws MalformedURLException {
        AdaptiveURLNormalizer filter = createFilter();
        observeSessionParam(5);
        Metadata redirection = new Metadata();
        redirection.setValue("_redirTo", "http://example.com/c?sid=zzz");

        // normalised, /c?sid=zzz would be /c again, i.e. dropped: the content needs the param
        assertEquals(
                "http://example.com/c?sid=zzz",
                filter.filter(
                        URLUtil.toURL("http://example.com/c"),
                        redirection,
                        "http://example.com/c?sid=zzz"));
        assertEquals(
                "http://example.com/c?sid=zzz#top",
                filter.filter(
                        URLUtil.toURL("http://example.com/c"),
                        redirection,
                        "http://example.com/c?sid=zzz#top"));
        assertEquals(
                "http://example.com/c?pid=1&sid=zzz",
                filter.filter(
                        URLUtil.toURL("http://example.com/c?pid=1"),
                        redirection,
                        "http://example.com/c?pid=1&sid=zzz"));
        // a redirection elsewhere is normalised as usual
        assertEquals(
                "http://example.com/d",
                filter.filter(
                        URLUtil.toURL("http://example.com/c"),
                        redirection,
                        "http://example.com/d?sid=zzz"));
    }

    @Test
    void testLinkToItselfWithAParameterIsNormalised() throws MalformedURLException {
        AdaptiveURLNormalizer filter = createFilter();
        observeSessionParam(5);
        // not a redirection: /c becomes a link to itself, for the SelfURLFilter to drop
        assertEquals(
                "http://example.com/c",
                filter.filter(
                        URLUtil.toURL("http://example.com/c"),
                        new Metadata(),
                        "http://example.com/c?sid=zzz"));
    }

    @Test
    void testNullSourceIsHandled() throws MalformedURLException {
        AdaptiveURLNormalizer filter = createFilter();
        observeSessionParam(5);
        assertEquals(
                "http://example.com/other",
                filter.filter(null, new Metadata(), "http://example.com/other?sid=zzz"));
        assertEquals(
                "http://example.com/other",
                filter.filter(
                        URLUtil.toURL("http://example.com/page?sid=1"),
                        null,
                        "http://example.com/other?sid=zzz"));
    }

    @Test
    void testMalformedURLIsLeftUntouched() {
        AdaptiveURLNormalizer filter = createFilter();
        observeSessionParam(5);
        String malformed = "this is not a URL?sid=1";
        assertEquals(malformed, apply(filter, malformed));
        assertEquals("mailto:someone@example.com", apply(filter, "mailto:someone@example.com"));
    }

    @Test
    void testCustomCanonicalMetadataKey() {
        Map<String, Object> conf = new HashMap<>();
        conf.put(CanonicalRules.CANONICAL_KEY_PARAM, "parse.canonical");
        AdaptiveURLNormalizer filter = createFilter(conf);
        for (int i = 0; i < 5; i++) {
            observe(
                    "http://example.com/page" + i + "?pid=" + i + "&sid=abc" + i,
                    "parse.canonical",
                    "http://example.com/page" + i + "?pid=" + i);
        }
        assertEquals(
                "http://example.com/other?pid=9",
                apply(filter, "http://example.com/other?pid=9&sid=zzz"));
    }

    @Test
    void testWeakestParameterIsDiscardedWhenFull() {
        Map<String, Object> conf = new HashMap<>();
        conf.put(CanonicalRules.MAX_PARAMS_PARAM, 2);
        AdaptiveURLNormalizer filter = createFilter(conf);
        rules = CanonicalRules.getInstance(conf);

        // per-page tokens as parameter names would otherwise fill the slots for good
        for (int i = 0; i < 20; i++) {
            observe(
                    "http://example.com/junk" + i + "?token" + i + "=x&nonce" + i + "=y",
                    "http://example.com/junk" + i);
        }
        assertEquals(2, rules.getTrackedParams(HOST));
        for (int i = 0; i < 5; i++) {
            observe("http://example.com/page" + i + "?sid=abc" + i, "http://example.com/page" + i);
        }
        assertEquals(2, rules.getTrackedParams(HOST));
        assertEquals("http://example.com/other", apply(filter, "http://example.com/other?sid=zzz"));
    }

    @Test
    void testEstablishedParameterSurvivesTheCap() {
        Map<String, Object> conf = new HashMap<>();
        conf.put(CanonicalRules.MAX_PARAMS_PARAM, 2);
        AdaptiveURLNormalizer filter = createFilter(conf);
        rules = CanonicalRules.getInstance(conf);

        observeSessionParam(5);
        for (int i = 0; i < 20; i++) {
            observe(
                    "http://example.com/junk" + i + "?token" + i + "=x",
                    "http://example.com/junk" + i);
        }
        assertEquals(2, rules.getTrackedParams(HOST));
        // only the parameters still gathering evidence make room, never an established one
        assertEquals("http://example.com/other", apply(filter, "http://example.com/other?sid=zzz"));
    }

    @Test
    void testNumberOfTrackedSitesIsBounded() {
        Map<String, Object> conf = new HashMap<>();
        conf.put(CanonicalRules.MAX_SCOPES_PARAM, 1);
        AdaptiveURLNormalizer filter = createFilter(conf);
        rules = CanonicalRules.getInstance(conf);

        observeSessionParam(5);
        for (int i = 0; i < 5; i++) {
            observe("http://another.com/page" + i + "?sid=abc" + i, "http://another.com/page" + i);
        }

        assertEquals(1L, rules.getTrackedScopes(), "the second host should have evicted the first");

        boolean firstStillKnown =
                "http://example.com/other?pid=9"
                        .equals(apply(filter, "http://example.com/other?pid=9&sid=zzz"));
        boolean secondKnown =
                "http://another.com/other"
                        .equals(apply(filter, "http://another.com/other?sid=zzz"));
        assertFalse(
                firstStillKnown && secondKnown,
                "only one host should be retained with maxScopes 1");
        assertTrue(firstStillKnown || secondKnown, "the surviving host should still have its rule");
    }

    @Test
    void testInvalidConfidenceFallsBackToTheDefault() {
        Map<String, Object> conf = new HashMap<>();
        conf.put(CanonicalRules.CONFIDENCE_PARAM, 1.5d);
        AdaptiveURLNormalizer filter = createFilter(conf);

        observeSessionParam(4);
        assertEquals(
                "http://example.com/other?pid=9&sid=zzz",
                apply(filter, "http://example.com/other?pid=9&sid=zzz"));

        observeSessionParam(5);
        assertEquals(
                "http://example.com/other?pid=9",
                apply(filter, "http://example.com/other?pid=9&sid=zzz"));
    }

    @Test
    void testRulesAreSharedByTheComponentsOfTheJVM() {
        Map<String, Object> lenient = new HashMap<>();
        lenient.put(CanonicalRules.MIN_OBSERVATIONS_PARAM, 2);
        lenient.put(CanonicalRules.MIN_DISTINCT_PATHS_PARAM, 2);
        createFilter(lenient);
        // configured differently, as could happen in local mode: the settings of the first win
        AdaptiveURLNormalizer second = createFilter(new HashMap<>());

        observeSessionParam(2);
        assertEquals(
                "http://example.com/other?pid=9",
                apply(second, "http://example.com/other?pid=9&sid=zzz"));
    }

    @Test
    void testRulesAreDiscardedOnceReleasedByEveryComponent() {
        AdaptiveURLNormalizer first = createFilter();
        CanonicalParamLearner firstLearner = learner;
        AdaptiveURLNormalizer second = createFilter();
        CanonicalParamLearner secondLearner = learner;
        observeSessionParam(5);

        first.cleanup();
        firstLearner.cleanup();
        assertEquals(
                "http://example.com/other?pid=9",
                apply(second, "http://example.com/other?pid=9&sid=zzz"));

        second.cleanup();
        secondLearner.cleanup();
        AdaptiveURLNormalizer fresh = createFilter();
        assertEquals(
                "http://example.com/other?pid=9&sid=zzz",
                apply(fresh, "http://example.com/other?pid=9&sid=zzz"));
    }

    @Test
    void testSharedByConcurrentThreads() throws Exception {
        // the URL filters of a FetcherBolt are shared by all of its fetcher threads
        Map<String, Object> conf = new HashMap<>();
        conf.put(CanonicalRules.MAX_PARAMS_PARAM, 4);
        AdaptiveURLNormalizer filter = createFilter(conf);
        rules = CanonicalRules.getInstance(conf);

        runConcurrently(
                (thread, i) -> {
                    String page = "http://example.com/t" + thread + "p" + i;
                    observe(page + "?sid=abc" + i, page);
                    apply(filter, "http://example.com/other?sid=zzz");
                });
        assertEquals("http://example.com/other", apply(filter, "http://example.com/other?sid=zzz"));

        // a flood of distinct parameters competing for the slots, with the rule in place
        runConcurrently(
                (thread, i) -> {
                    String page = "http://example.com/j" + thread + "p" + i;
                    observe(page + "?junk" + thread + "x" + i + "=1", page);
                    apply(filter, "http://example.com/other?sid=zzz");
                });
        assertTrue(rules.getTrackedParams(HOST) <= 4, "the cap must hold under contention");
        assertEquals("http://example.com/other", apply(filter, "http://example.com/other?sid=zzz"));
    }

    /** Runs the task for 100 values of <i>i</i> on each of 4 threads started together. */
    private static void runConcurrently(BiConsumer<Integer, Integer> task) throws Exception {
        final int threads = 4;
        final ExecutorService pool = Executors.newFixedThreadPool(threads);
        final CountDownLatch start = new CountDownLatch(1);
        final AtomicReference<Throwable> failure = new AtomicReference<>();
        for (int t = 0; t < threads; t++) {
            final int thread = t;
            pool.execute(
                    () -> {
                        try {
                            start.await();
                            for (int i = 0; i < 100; i++) {
                                task.accept(thread, i);
                            }
                        } catch (Throwable e) {
                            failure.compareAndSet(null, e);
                        }
                    });
        }
        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS), "the threads should have finished");
        if (failure.get() != null) {
            throw new AssertionError("a thread failed", failure.get());
        }
    }
}
