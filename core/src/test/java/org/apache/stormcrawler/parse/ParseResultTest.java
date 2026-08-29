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

package org.apache.stormcrawler.parse;

import org.apache.stormcrawler.Metadata;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class ParseResultTest {

    private static final String URL = "https://example.com/";

    @Test
    void getIfPresentReturnsNullForUnknownURL() {
        ParseResult parse = new ParseResult();

        Assertions.assertNull(parse.getIfPresent("https://example.com/never-parsed"));

        // the lookup must not modify the ParseResult
        Assertions.assertEquals(0, parse.size());
    }

    @Test
    void getIfPresentReturnsTheExistingEntry() {
        ParseResult parse = new ParseResult();
        parse.put(URL, "key", "value");

        ParseData parseData = parse.getIfPresent(URL);

        Assertions.assertNotNull(parseData);
        Assertions.assertSame(parse.getParseMap().get(URL), parseData);
        Assertions.assertEquals(1, parse.size());
    }

    @Test
    void getOrCreateCreatesAnEntryForAnUnknownURL() {
        ParseResult parse = new ParseResult();

        ParseData parseData = parse.getOrCreate("https://example.com/never-parsed");

        Assertions.assertNotNull(parseData);
        Assertions.assertEquals(1, parse.size());
        Assertions.assertSame(parseData, parse.getIfPresent("https://example.com/never-parsed"));
    }

    @Test
    void getOrCreateReturnsTheSameInstance() {
        ParseResult parse = new ParseResult();

        ParseData first = parse.getOrCreate(URL);
        ParseData second = parse.getOrCreate(URL);

        Assertions.assertSame(first, second);
        Assertions.assertEquals(1, parse.size());
    }

    @Test
    void getOrCreateSetsAnEmptyContentArray() {
        ParseResult parse = new ParseResult();

        byte[] content = parse.getOrCreate("https://example.com/never-parsed").getContent();

        Assertions.assertNotNull(content);
        Assertions.assertEquals(0, content.length);
    }

    @Test
    void deprecatedGetCreatesAnEntryForAnUnknownURL() {
        ParseResult parse = new ParseResult();

        ParseData parseData = parse.get("https://example.com/never-parsed");

        // the deprecated method keeps the historic behaviour of get()
        Assertions.assertNotNull(parseData);
        Assertions.assertEquals(1, parse.size());
    }

    @Test
    void putCreatesAnEntryForASubDocument() {
        ParseResult parse = new ParseResult();

        parse.put("https://example.com/subdocument", "key", "value");

        Assertions.assertEquals(1, parse.size());
        Assertions.assertEquals(
                "value", parse.getValues("https://example.com/subdocument", "key")[0]);
    }

    @Test
    void setCreatesAnEntryForASubDocument() {
        ParseResult parse = new ParseResult();
        Metadata metadata = new Metadata();
        metadata.addValue("key", "value");

        parse.set("https://example.com/subdocument", metadata);

        Assertions.assertEquals(1, parse.size());
        ParseData parseData = parse.getIfPresent("https://example.com/subdocument");
        Assertions.assertNotNull(parseData);
        Assertions.assertEquals("value", parseData.getMetadata().getFirstValue("key"));
    }

    @Test
    void getValuesReturnsNullForAnUnknownURL() {
        ParseResult parse = new ParseResult();

        Assertions.assertNull(parse.getValues("https://example.com/never-parsed", "key"));

        Assertions.assertEquals(0, parse.size());
    }

    @Test
    void parseDataConstructorsSetAnEmptyContentArray() {
        Assertions.assertNotNull(new ParseData().getMetadata());
        Assertions.assertEquals(0, new ParseData().getContent().length);
        Assertions.assertEquals(0, new ParseData(new Metadata()).getContent().length);
        Assertions.assertEquals(0, new ParseData("text", new Metadata()).getContent().length);
    }
}
