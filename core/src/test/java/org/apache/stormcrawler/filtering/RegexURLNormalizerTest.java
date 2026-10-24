/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.stormcrawler.filtering;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.HashMap;
import org.apache.stormcrawler.filtering.regex.RegexURLNormalizer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class RegexURLNormalizerTest {

    private static final String URL = "https://example.com/page?sessionid=abc";

    private static URLFilter createNormalizer(String rulesFile) {
        ObjectNode params = new ObjectNode(JsonNodeFactory.instance);
        params.put("regexNormalizerFile", rulesFile);
        RegexURLNormalizer normalizer = new RegexURLNormalizer();
        normalizer.configure(new HashMap<>(), params);
        return normalizer;
    }

    @Test
    void appliesRulesFromFile() {
        URLFilter normalizer = createNormalizer("test-regex-normalizers-plain.xml");
        Assertions.assertEquals("https://example.com/page", normalizer.filter(null, null, URL));
    }

    /**
     * The rules file declares an external DTD on a port nothing listens on. It must not be fetched:
     * a parser that tried would fail and load no rules at all.
     */
    @Test
    void externalDtdIsNotFetched() {
        URLFilter normalizer = createNormalizer("test-regex-normalizers-dtd.xml");
        Assertions.assertEquals("https://example.com/page", normalizer.filter(null, null, URL));
    }
}
