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

package org.apache.stormcrawler.parse.filter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.filtering.adaptive.AdaptiveURLNormalizer;
import org.apache.stormcrawler.filtering.adaptive.CanonicalRules;
import org.apache.stormcrawler.parse.ParseResult;
import org.junit.jupiter.api.Test;

/** Tests that the learner feeds the rules which the {@link AdaptiveURLNormalizer} applies. */
class CanonicalParamLearnerTest {

    private static final AtomicInteger STORE_COUNTER = new AtomicInteger();

    private final Map<String, Object> conf = new HashMap<>();

    private final String store = "learner-test-" + STORE_COUNTER.incrementAndGet();

    private CanonicalParamLearner createLearner() {
        CanonicalParamLearner learner = new CanonicalParamLearner();
        learner.configure(conf, storeParams());
        return learner;
    }

    private AdaptiveURLNormalizer createFilter() {
        AdaptiveURLNormalizer filter = new AdaptiveURLNormalizer();
        filter.configure(conf, storeParams());
        return filter;
    }

    private ObjectNode storeParams() {
        ObjectNode params = new ObjectNode(JsonNodeFactory.instance);
        params.put("store", store);
        return params;
    }

    /** Simulates a page being parsed, its canonical tag already extracted into the metadata. */
    private void parse(CanonicalParamLearner learner, String url, String canonicalValue) {
        ParseResult parse = new ParseResult();
        Metadata metadata = new Metadata();
        if (canonicalValue != null) {
            metadata.setValue("canonical", canonicalValue);
        }
        parse.set(url, metadata);
        learner.filter(url, new byte[0], null, parse);
    }

    @Test
    void testLearnerFeedsTheFilter() {
        CanonicalParamLearner learner = createLearner();
        AdaptiveURLNormalizer filter = createFilter();

        assertEquals(
                "http://example.com/other?pid=9&sid=zzz",
                filter.filter(null, null, "http://example.com/other?pid=9&sid=zzz"));

        for (int i = 0; i < 5; i++) {
            parse(
                    learner,
                    "http://example.com/page" + i + "?pid=" + i + "&sid=abc" + i,
                    "http://example.com/page" + i + "?pid=" + i);
        }

        assertEquals(
                "http://example.com/other?pid=9",
                filter.filter(null, null, "http://example.com/other?pid=9&sid=zzz"));
    }

    @Test
    void testPagesWithoutCanonicalAreIgnored() {
        CanonicalParamLearner learner = createLearner();
        AdaptiveURLNormalizer filter = createFilter();

        for (int i = 0; i < 20; i++) {
            parse(learner, "http://example.com/page" + i + "?pid=" + i + "&sid=abc" + i, null);
        }

        assertEquals(
                "http://example.com/other?pid=9&sid=zzz",
                filter.filter(null, null, "http://example.com/other?pid=9&sid=zzz"));
    }

    @Test
    void testUnknownURLIsIgnored() {
        CanonicalParamLearner learner = createLearner();
        ParseResult parse = new ParseResult();
        learner.filter("http://example.com/page?sid=1", new byte[0], null, parse);
        // no ParseData must have been created for it
        assertTrue(parse.getParseMap().isEmpty());
    }

    @Test
    void testMalformedURLIsIgnored() {
        CanonicalParamLearner learner = createLearner();
        parse(learner, "this is not a URL", "http://example.com/page");
    }

    @Test
    void testTheLearnerDoesNotNeedTheDOM() {
        assertEquals(false, createLearner().needsDOM());
    }

    @Test
    void testConfigurationIsSharedThroughTheStore() {
        conf.put(CanonicalRules.MIN_OBSERVATIONS_PARAM, 2);
        conf.put(CanonicalRules.MIN_DISTINCT_PATHS_PARAM, 2);
        CanonicalParamLearner learner = createLearner();
        AdaptiveURLNormalizer filter = createFilter();

        for (int i = 0; i < 2; i++) {
            parse(
                    learner,
                    "http://example.com/page" + i + "?pid=" + i + "&sid=abc" + i,
                    "http://example.com/page" + i + "?pid=" + i);
        }

        assertEquals(
                "http://example.com/other?pid=9",
                filter.filter(null, null, "http://example.com/other?pid=9&sid=zzz"));
    }
}
