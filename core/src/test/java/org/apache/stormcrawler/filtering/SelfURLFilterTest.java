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

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.filtering.basic.SelfURLFilter;
import org.apache.stormcrawler.util.URLUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class SelfURLFilterTest {

    private static String filter(String source, String target) throws MalformedURLException {
        SelfURLFilter filter = new SelfURLFilter();
        filter.configure(new HashMap<>(), JsonNodeFactory.instance.objectNode());
        URL sourceURL = URLUtil.toURL(source);
        return filter.filter(sourceURL, new Metadata(), target);
    }

    @Test
    void testSelfIsRemoved() throws MalformedURLException {
        Assertions.assertNull(
                filter("https://www.example.com/a.html", "https://www.example.com/a.html"));
    }

    @Test
    void testSchemeAndHostAreCaseInsensitive() throws MalformedURLException {
        Assertions.assertNull(
                filter("https://www.example.com/a.html", "HTTPS://WWW.Example.COM/a.html"));
    }

    @Test
    void testPathIsCaseSensitive() throws MalformedURLException {
        String target = "https://whimsy.apache.org/board/minutes/ACE.html";
        Assertions.assertEquals(
                target, filter("https://whimsy.apache.org/board/minutes/ace.html", target));
    }

    @Test
    void testQueryIsCaseSensitive() throws MalformedURLException {
        String target = "https://www.example.com/search?q=ACE";
        Assertions.assertEquals(target, filter("https://www.example.com/search?q=ace", target));
    }

    @Test
    void testOtherURLsAreKept() throws MalformedURLException {
        String source = "https://www.example.com/a.html";
        for (String target :
                new String[] {
                    "http://www.example.com/a.html",
                    "https://example.com/a.html",
                    "https://www.example.com:8443/a.html",
                    "https://www.example.com/b.html",
                    "https://www.example.com/a.html?x=1",
                    "https://www.example.com/a.html#top"
                }) {
            Assertions.assertEquals(target, filter(source, target));
        }
    }

    @Test
    void testNoSource() {
        SelfURLFilter filter = new SelfURLFilter();
        String target = "https://www.example.com/a.html";
        Assertions.assertEquals(target, filter.filter(null, new Metadata(), target));
    }
}
