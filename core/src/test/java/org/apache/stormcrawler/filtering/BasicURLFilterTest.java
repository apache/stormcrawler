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
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.filtering.basic.BasicURLFilter;
import org.apache.stormcrawler.util.URLUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class BasicURLFilterTest {

    private URLFilter createFilter(int length, int repeat) {
        BasicURLFilter filter = new BasicURLFilter();
        ObjectNode filterParams = new ObjectNode(JsonNodeFactory.instance);
        filterParams.put("maxPathRepetition", repeat);
        filterParams.put("maxLength", length);
        Map<String, Object> conf = new HashMap<>();
        filter.configure(conf, filterParams);
        return filter;
    }

    @Test
    void testRepetition() throws MalformedURLException {
        URLFilter filter = createFilter(-1, 3);
        Metadata metadata = new Metadata();
        URL targetURL = URLUtil.toURL("http://www.sourcedomain.com/a/a/a/index.html");
        String filterResult = filter.filter(targetURL, metadata, targetURL.toExternalForm());
        Assertions.assertNull(filterResult);
        targetURL = URLUtil.toURL("http://www.sourcedomain.com/a/b/a/index.html");
        filterResult = filter.filter(targetURL, metadata, targetURL.toExternalForm());
        Assertions.assertEquals(targetURL.toExternalForm(), filterResult);
    }

    @Test
    void testLength() throws MalformedURLException {
        URLFilter filter = createFilter(32, -1);
        Metadata metadata = new Metadata();
        URL targetURL = URLUtil.toURL("http://www.sourcedomain.com/a/a/a/index.html");
        String filterResult = filter.filter(targetURL, metadata, targetURL.toExternalForm());
        Assertions.assertNull(filterResult);
        targetURL = URLUtil.toURL("http://www.sourcedomain.com/");
        filterResult = filter.filter(targetURL, metadata, targetURL.toExternalForm());
        Assertions.assertEquals(targetURL.toExternalForm(), filterResult);
    }
}
