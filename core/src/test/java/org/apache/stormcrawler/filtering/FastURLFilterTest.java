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
import org.apache.stormcrawler.filtering.regex.FastURLFilter;
import org.apache.stormcrawler.util.URLUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class FastURLFilterTest {

    private URLFilter createFilter() {
        ObjectNode filterParams = new ObjectNode(JsonNodeFactory.instance);
        filterParams.put("files", "fast.urlfilter.json");
        FastURLFilter filter = new FastURLFilter();
        Map<String, Object> conf = new HashMap<>();
        filter.configure(conf, filterParams);
        return filter;
    }

    @Test
    void testImagesFilter() throws MalformedURLException {
        URL url = URLUtil.toURL("http://www.somedomain.com/image.jpg");
        Metadata metadata = new Metadata();
        String filterResult = createFilter().filter(url, metadata, url.toExternalForm());
        Assertions.assertNull(filterResult);
    }

    @Test
    void testDomainNotAllowed() throws MalformedURLException {
        URL url = URLUtil.toURL("http://stormcrawler.net/");
        Metadata metadata = new Metadata();
        String filterResult = createFilter().filter(url, metadata, url.toExternalForm());
        Assertions.assertNull(filterResult);
        // allowed
        url = URLUtil.toURL("http://stormcrawler.net/bla/");
        filterResult = createFilter().filter(url, metadata, url.toExternalForm());
        Assertions.assertEquals(url.toString(), filterResult);
    }

    @Test
    void testMD() throws MalformedURLException {
        URL url = URLUtil.toURL("http://somedomain.net/");
        Metadata metadata = new Metadata();
        metadata.addValue("key", "value");
        String filterResult = createFilter().filter(url, metadata, url.toExternalForm());
        Assertions.assertNull(filterResult);
    }
}
