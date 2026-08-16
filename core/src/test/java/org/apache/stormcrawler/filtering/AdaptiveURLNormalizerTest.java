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

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.net.MalformedURLException;
import java.util.HashMap;
import java.util.Map;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.filtering.adaptive.AdaptiveURLNormalizer;
import org.apache.stormcrawler.util.URLUtil;
import org.junit.jupiter.api.Test;

/**
 * Tests the learning of the query parameters which can be removed from the URLs of a site, based on
 * the canonical tags found in its pages.
 */
class AdaptiveURLNormalizerTest {

    private static final String CANONICAL = "canonical";

    private AdaptiveURLNormalizer createFilter() {
        return createFilter(new ObjectNode(JsonNodeFactory.instance));
    }

    private AdaptiveURLNormalizer createFilter(ObjectNode filterParams) {
        AdaptiveURLNormalizer filter = new AdaptiveURLNormalizer();
        Map<String, Object> conf = new HashMap<>();
        filter.configure(conf, filterParams);
        return filter;
    }

    private static ObjectNode params() {
        return new ObjectNode(JsonNodeFactory.instance);
    }

    /** Simulates the parsing of a page having the given canonical tag. */
    private String observe(AdaptiveURLNormalizer filter, String pageUrl, String canonicalValue)
            throws MalformedURLException {
        return observe(filter, pageUrl, CANONICAL, canonicalValue);
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

    /**
     * Observes pages whose canonical drops the <i>sid</i> parameter but keeps the <i>id</i> one.
     */
    private void observeSessionParam(AdaptiveURLNormalizer filter, int observations)
            throws MalformedURLException {
        for (int i = 0; i < observations; i++) {
            observe(
                    filter,
                    "http://example.com/page" + i + "?id=" + i + "&sid=abc" + i,
                    "http://example.com/page" + i + "?id=" + i);
        }
    }

    @Test
    void testUnchangedWithoutEvidence() {
        AdaptiveURLNormalizer filter = createFilter();
        assertEquals(
                "http://example.com/page?id=1&sid=abc",
                apply(filter, "http://example.com/page?id=1&sid=abc"));
    }

    @Test
    void testNoEvidenceWithoutCanonical() throws MalformedURLException {
        AdaptiveURLNormalizer filter = createFilter();
        for (int i = 0; i < 20; i++) {
            observe(filter, "http://example.com/page" + i + "?id=" + i + "&sid=abc" + i, null);
        }
        assertEquals(
                "http://example.com/other?id=9&sid=zzz",
                apply(filter, "http://example.com/other?id=9&sid=zzz"));
    }

    @Test
    void testRemovesIrrelevantParameter() throws MalformedURLException {
        AdaptiveURLNormalizer filter = createFilter();
        observeSessionParam(filter, 5);
        assertEquals(
                "http://example.com/other?id=9",
                apply(filter, "http://example.com/other?id=9&sid=zzz"));
    }

    @Test
    void testNotAppliedBelowMinObservations() throws MalformedURLException {
        AdaptiveURLNormalizer filter = createFilter();
        observeSessionParam(filter, 4);
        assertEquals(
                "http://example.com/other?id=9&sid=zzz",
                apply(filter, "http://example.com/other?id=9&sid=zzz"));
    }

    @Test
    void testMinObservationsIsConfigurable() throws MalformedURLException {
        ObjectNode filterParams = params();
        filterParams.put("minObservations", 2);
        AdaptiveURLNormalizer filter = createFilter(filterParams);
        observeSessionParam(filter, 2);
        assertEquals(
                "http://example.com/other?id=9",
                apply(filter, "http://example.com/other?id=9&sid=zzz"));
    }

    @Test
    void testParameterKeptByCanonicalIsNeverRemoved() throws MalformedURLException {
        AdaptiveURLNormalizer filter = createFilter();
        observeSessionParam(filter, 20);
        assertEquals(
                "http://example.com/other?id=9", apply(filter, "http://example.com/other?id=9"));
    }

    @Test
    void testCanonicalIdenticalToSourceKeepsEverything() throws MalformedURLException {
        AdaptiveURLNormalizer filter = createFilter();
        for (int i = 0; i < 10; i++) {
            String page = "http://example.com/page" + i + "?id=" + i + "&sid=abc" + i;
            observe(filter, page, page);
        }
        assertEquals(
                "http://example.com/other?id=9&sid=zzz",
                apply(filter, "http://example.com/other?id=9&sid=zzz"));
    }

    @Test
    void testConfidenceThreshold() throws MalformedURLException {
        ObjectNode filterParams = params();
        filterParams.put("minObservations", 4);
        filterParams.put("confidenceThreshold", 0.75d);
        AdaptiveURLNormalizer filter = createFilter(filterParams);

        // sort: dropped 3 times out of 4 -> 0.75, at the threshold
        // page: dropped 2 times out of 4 -> 0.5, below the threshold
        observe(filter, "http://example.com/a?sort=x&page=1", "http://example.com/a");
        observe(filter, "http://example.com/b?sort=x&page=1", "http://example.com/b");
        observe(filter, "http://example.com/c?sort=x&page=1", "http://example.com/c?page=1");
        observe(filter, "http://example.com/d?sort=x&page=1", "http://example.com/d?sort=x&page=1");

        assertEquals(
                "http://example.com/e?page=3", apply(filter, "http://example.com/e?sort=x&page=3"));
    }

    @Test
    void testEvidenceIsScopedToTheHost() throws MalformedURLException {
        AdaptiveURLNormalizer filter = createFilter();
        observeSessionParam(filter, 5);
        assertEquals(
                "http://another.com/other?id=9&sid=zzz",
                apply(filter, "http://another.com/other?id=9&sid=zzz"));
        assertEquals(
                "http://sub.example.com/other?id=9&sid=zzz",
                apply(filter, "http://sub.example.com/other?id=9&sid=zzz"));
    }

    @Test
    void testEvidenceCanBeScopedToTheDomain() throws MalformedURLException {
        ObjectNode filterParams = params();
        filterParams.put("scope", "domain");
        AdaptiveURLNormalizer filter = createFilter(filterParams);
        for (int i = 0; i < 5; i++) {
            observe(
                    filter,
                    "http://www.example.com/page" + i + "?id=" + i + "&sid=abc" + i,
                    "http://www.example.com/page" + i + "?id=" + i);
        }
        assertEquals(
                "http://shop.example.com/other?id=9",
                apply(filter, "http://shop.example.com/other?id=9&sid=zzz"));
        assertEquals(
                "http://another.com/other?id=9&sid=zzz",
                apply(filter, "http://another.com/other?id=9&sid=zzz"));
    }

    @Test
    void testHostIsCaseInsensitive() throws MalformedURLException {
        AdaptiveURLNormalizer filter = createFilter();
        observeSessionParam(filter, 5);
        assertEquals(
                "http://EXAMPLE.com/other?id=9",
                apply(filter, "http://EXAMPLE.com/other?id=9&sid=zzz"));
    }

    @Test
    void testCanonicalOnAnotherPathIsIgnored() throws MalformedURLException {
        AdaptiveURLNormalizer filter = createFilter();
        for (int i = 0; i < 10; i++) {
            observe(
                    filter,
                    "http://example.com/page" + i + "?id=" + i + "&sid=abc" + i,
                    "http://example.com/canonical" + i);
        }
        assertEquals(
                "http://example.com/other?id=9&sid=zzz",
                apply(filter, "http://example.com/other?id=9&sid=zzz"));
    }

    @Test
    void testCanonicalOnAnotherHostIsIgnored() throws MalformedURLException {
        AdaptiveURLNormalizer filter = createFilter();
        for (int i = 0; i < 10; i++) {
            observe(
                    filter,
                    "http://example.com/page" + i + "?id=" + i + "&sid=abc" + i,
                    "http://mirror.example.com/page" + i + "?id=" + i);
        }
        assertEquals(
                "http://example.com/other?id=9&sid=zzz",
                apply(filter, "http://example.com/other?id=9&sid=zzz"));
    }

    @Test
    void testRelativeCanonicalIsResolved() throws MalformedURLException {
        AdaptiveURLNormalizer filter = createFilter();
        for (int i = 0; i < 5; i++) {
            observe(
                    filter,
                    "http://example.com/page" + i + "?id=" + i + "&sid=abc" + i,
                    "/page" + i + "?id=" + i);
        }
        assertEquals(
                "http://example.com/other?id=9",
                apply(filter, "http://example.com/other?id=9&sid=zzz"));
    }

    @Test
    void testPureQueryCanonicalIsResolved() throws MalformedURLException {
        AdaptiveURLNormalizer filter = createFilter();
        for (int i = 0; i < 5; i++) {
            observe(
                    filter,
                    "http://example.com/page" + i + "?id=" + i + "&sid=abc" + i,
                    "?id=" + i);
        }
        assertEquals(
                "http://example.com/other?id=9",
                apply(filter, "http://example.com/other?id=9&sid=zzz"));
    }

    @Test
    void testInvalidCanonicalIsIgnored() throws MalformedURLException {
        AdaptiveURLNormalizer filter = createFilter();
        for (int i = 0; i < 10; i++) {
            observe(filter, "http://example.com/page" + i + "?id=" + i + "&sid=abc" + i, ":::");
        }
        assertEquals(
                "http://example.com/other?id=9&sid=zzz",
                apply(filter, "http://example.com/other?id=9&sid=zzz"));
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
                "http://example.com/other?id=a%20b#top",
                apply(filter, "http://example.com/other?sid=zzz&id=a%20b#top"));
    }

    @Test
    void testNonDefaultPortIsPreserved() throws MalformedURLException {
        AdaptiveURLNormalizer filter = createFilter();
        for (int i = 0; i < 5; i++) {
            observe(
                    filter,
                    "http://example.com:8080/page" + i + "?id=" + i + "&sid=abc" + i,
                    "http://example.com:8080/page" + i + "?id=" + i);
        }
        assertEquals(
                "http://example.com:8080/other?id=9",
                apply(filter, "http://example.com:8080/other?id=9&sid=zzz"));
    }

    @Test
    void testCanonicalOnAnotherPortIsIgnored() throws MalformedURLException {
        AdaptiveURLNormalizer filter = createFilter();
        for (int i = 0; i < 10; i++) {
            observe(
                    filter,
                    "http://example.com/page" + i + "?id=" + i + "&sid=abc" + i,
                    "http://example.com:8080/page" + i + "?id=" + i);
        }
        assertEquals(
                "http://example.com/other?id=9&sid=zzz",
                apply(filter, "http://example.com/other?id=9&sid=zzz"));
    }

    @Test
    void testDefaultPortMatchesImplicitOne() throws MalformedURLException {
        AdaptiveURLNormalizer filter = createFilter();
        for (int i = 0; i < 5; i++) {
            observe(
                    filter,
                    "http://example.com:80/page" + i + "?id=" + i + "&sid=abc" + i,
                    "http://example.com/page" + i + "?id=" + i);
        }
        assertEquals(
                "http://example.com/other?id=9",
                apply(filter, "http://example.com/other?id=9&sid=zzz"));
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
        String page = "http://example.com/page?id=1&sid=abc";
        String canonical = "http://example.com/page?id=1";
        // the filter is called once per outlink of the same page
        for (int i = 0; i < 20; i++) {
            observe(filter, page, canonical);
        }
        assertEquals(
                "http://example.com/other?id=9&sid=zzz",
                apply(filter, "http://example.com/other?id=9&sid=zzz"));
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
        String malformed = "this is not a URL";
        assertEquals(malformed, apply(filter, malformed));
    }

    @Test
    void testCustomCanonicalMetadataKey() throws MalformedURLException {
        ObjectNode filterParams = params();
        filterParams.put("canonicalMetadataKey", "parse.canonical");
        AdaptiveURLNormalizer filter = createFilter(filterParams);
        for (int i = 0; i < 5; i++) {
            observe(
                    filter,
                    "http://example.com/page" + i + "?id=" + i + "&sid=abc" + i,
                    "parse.canonical",
                    "http://example.com/page" + i + "?id=" + i);
        }
        assertEquals(
                "http://example.com/other?id=9",
                apply(filter, "http://example.com/other?id=9&sid=zzz"));
    }

    @Test
    void testNumberOfTrackedParametersIsBounded() throws MalformedURLException {
        ObjectNode filterParams = params();
        filterParams.put("maxParams", 1);
        AdaptiveURLNormalizer filter = createFilter(filterParams);

        for (int i = 0; i < 5; i++) {
            observe(
                    filter,
                    "http://example.com/first" + i + "?sid=abc" + i,
                    "http://example.com/first" + i);
        }
        for (int i = 0; i < 5; i++) {
            observe(
                    filter,
                    "http://example.com/second" + i + "?ref=abc" + i,
                    "http://example.com/second" + i);
        }

        // only the first parameter seen is tracked
        assertEquals(
                "http://example.com/other?ref=zzz",
                apply(filter, "http://example.com/other?sid=1&ref=zzz"));
    }

    @Test
    void testNumberOfTrackedSitesIsBounded() throws MalformedURLException {
        ObjectNode filterParams = params();
        filterParams.put("maxScopes", 1);
        AdaptiveURLNormalizer filter = createFilter(filterParams);

        // which site gets evicted once the limit is reached is left to the cache,
        // the rules of the only one tracked here must still be applied
        observeSessionParam(filter, 5);
        assertEquals(
                "http://example.com/other?id=9",
                apply(filter, "http://example.com/other?id=9&sid=zzz"));
    }

    @Test
    void testInvalidConfigurationValuesFallBackToDefaults() throws MalformedURLException {
        ObjectNode filterParams = params();
        filterParams.put("minObservations", 0);
        filterParams.put("confidenceThreshold", 1.5d);
        filterParams.put("maxScopes", 0);
        filterParams.put("maxParams", -1);
        filterParams.put("scope", "unknown");
        filterParams.put("canonicalMetadataKey", "  ");
        AdaptiveURLNormalizer filter = createFilter(filterParams);

        observeSessionParam(filter, 4);
        assertEquals(
                "http://example.com/other?id=9&sid=zzz",
                apply(filter, "http://example.com/other?id=9&sid=zzz"));

        observeSessionParam(filter, 5);
        assertEquals(
                "http://example.com/other?id=9",
                apply(filter, "http://example.com/other?id=9&sid=zzz"));
    }
}
