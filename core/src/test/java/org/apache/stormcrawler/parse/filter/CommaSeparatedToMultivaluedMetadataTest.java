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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.parse.ParseResult;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class CommaSeparatedToMultivaluedMetadataTest {

    /** Counts how often the copy-on-append write path is used. */
    private static class CountingMetadata extends Metadata {
        int addValueCalls = 0;

        @Override
        public void addValue(String key, String value) {
            addValueCalls++;
            super.addValue(key, value);
        }
    }

    private static CommaSeparatedToMultivaluedMetadata newFilter(String jsonParams)
            throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode params = mapper.readTree(jsonParams);
        CommaSeparatedToMultivaluedMetadata filter = new CommaSeparatedToMultivaluedMetadata();
        filter.configure(Map.of(), params);
        return filter;
    }

    private static String commaList(int tokens) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < tokens; i++) {
            if (i > 0) {
                sb.append(',');
            }
            sb.append('a');
        }
        return sb.toString();
    }

    @Test
    void splittingUsesABulkAppend() throws Exception {
        final String url = "https://example.com/";
        CountingMetadata md = new CountingMetadata();
        md.setValue("parse.keywords", commaList(1000));

        ParseResult parse = new ParseResult();
        parse.set(url, md);

        // the default cap is deliberately small; the bulk write path is what is under test here
        newFilter("{\"keys\": [\"parse.keywords\"], \"maxTokens\": 1000}")
                .filter(url, new byte[0], null, parse);

        Assertions.assertEquals(1000, md.getValues("parse.keywords").length);
        Assertions.assertTrue(
                md.addValueCalls <= 1,
                "the filter appended one token at a time: "
                        + md.addValueCalls
                        + " calls to Metadata.addValue, each copying the whole array");
    }

    @Test
    void timeLargeValue() throws Exception {
        final String url = "https://example.com/";
        // 32768 tokens / 65535 chars, far beyond any realistic tag list
        String value = commaList(32768);
        Assertions.assertEquals(65535, value.length());

        Metadata md = new Metadata();
        md.setValue("parse.keywords", value);
        ParseResult parse = new ParseResult();
        parse.set(url, md);

        long start = System.nanoTime();
        // the default cap is deliberately small; the bulk write path is what is under test here
        newFilter("{\"keys\": [\"parse.keywords\"], \"maxTokens\": 65536}")
                .filter(url, new byte[0], null, parse);
        long msec = (System.nanoTime() - start) / 1_000_000;

        System.out.println("32768 tokens took " + msec + " ms");
        Assertions.assertEquals(32768, md.getValues("parse.keywords").length);
    }

    @Test
    void splittingHandlesBlankTokens() throws Exception {
        final String url = "https://example.com/";

        // empty tokens between consecutive commas are dropped, as Metadata.addValue used to
        Metadata md = new Metadata();
        md.setValue("parse.keywords", "a,,b");
        ParseResult parse = new ParseResult();
        parse.set(url, md);
        newFilter("{\"keys\": [\"parse.keywords\"]}").filter(url, new byte[0], null, parse);
        Assertions.assertArrayEquals(new String[] {"a", "b"}, md.getValues("parse.keywords"));

        // a value made only of empty tokens leaves no entry behind
        md = new Metadata();
        md.setValue("parse.keywords", " , ,");
        parse = new ParseResult();
        parse.set(url, md);
        newFilter("{\"keys\": [\"parse.keywords\"]}").filter(url, new byte[0], null, parse);
        Assertions.assertNull(md.getValues("parse.keywords"));
    }

    @Test
    void capAppliesAfterBlankTokensAreDropped() throws Exception {
        final String url = "https://example.com/";

        // the cap counts real values: blank tokens must not eat into it
        Metadata md = new Metadata();
        md.setValue("parse.keywords", ",,,a,b,c");
        ParseResult parse = new ParseResult();
        parse.set(url, md);
        newFilter("{\"keys\": [\"parse.keywords\"], \"maxTokens\": 2}")
                .filter(url, new byte[0], null, parse);
        Assertions.assertArrayEquals(new String[] {"a", "b"}, md.getValues("parse.keywords"));
    }

    @Test
    void defaultCapTrimsLongValues() throws Exception {
        final String url = "https://example.com/";

        Metadata md = new Metadata();
        md.setValue("parse.keywords", commaList(200));
        ParseResult parse = new ParseResult();
        parse.set(url, md);
        newFilter("{\"keys\": [\"parse.keywords\"]}").filter(url, new byte[0], null, parse);
        Assertions.assertEquals(128, md.getValues("parse.keywords").length);
    }
}
