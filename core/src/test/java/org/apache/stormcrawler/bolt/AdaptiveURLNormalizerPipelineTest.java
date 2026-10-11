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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.stormcrawler.Constants;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.parse.ParsingTester;
import org.apache.stormcrawler.persistence.Status;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Runs the whole chain in the parsing bolt: the JSoup XPathFilter extracting the canonical tag, the
 * CanonicalParamLearner gathering the evidence and the AdaptiveURLNormalizer applied to the
 * outlinks. The outlinks of a page are filtered before its own canonical is seen, so the rules only
 * ever come from the pages parsed before.
 */
class AdaptiveURLNormalizerPipelineTest extends ParsingTester {

    private int statusTuplesSeen;

    @BeforeEach
    void setupParserBolt() {
        bolt = new JSoupParserBolt();
        setupParserBolt(bolt);
        Map<String, Object> conf = new HashMap<>();
        conf.put("jsoup.filters.config.file", "test.adaptive.jsoupfilters.json");
        conf.put("urlfilters.config.file", "test.adaptive.urlfilters.json");
        prepareParserBolt("test.adaptive.parsefilters.json", conf);
    }

    @Test
    void testOutlinksAreNormalisedOnceTheCanonicalsHaveBeenLearnt() throws IOException {
        // no evidence yet: the outlink keeps its session id
        assertEquals(List.of("http://example.com/other?pid=9&sid=zzz"), outlinksOf(0));

        for (int i = 1; i < 5; i++) {
            outlinksOf(i);
        }

        // five pages have dropped sid from their canonical, on five paths: the rule is in place
        assertEquals(List.of("http://example.com/other?pid=9"), outlinksOf(5));
    }

    /**
     * Parses a page whose canonical drops <i>sid</i> and keeps <i>pid</i>, and which links to a
     * page with both. Returns the outlinks emitted for it.
     */
    private List<String> outlinksOf(int page) throws IOException {
        String url = "http://example.com/page" + page + "?pid=" + page + "&sid=abc" + page;
        String html =
                "<html><head><title>page "
                        + page
                        + "</title><link rel=\"canonical\" href=\"http://example.com/page"
                        + page
                        + "?pid="
                        + page
                        + "\"></head><body>"
                        + "<a href=\"http://example.com/other?pid=9&amp;sid=zzz\">other</a>"
                        + "</body></html>";
        parse(url, html.getBytes(StandardCharsets.UTF_8), new Metadata());

        List<List<Object>> status = output.getEmitted(Constants.StatusStreamName);
        List<String> outlinks = new ArrayList<>();
        for (List<Object> tuple : status.subList(statusTuplesSeen, status.size())) {
            if (tuple.get(2) == Status.DISCOVERED) {
                outlinks.add((String) tuple.get(0));
            }
        }
        statusTuplesSeen = status.size();
        return outlinks;
    }
}
