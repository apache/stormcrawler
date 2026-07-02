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
import org.apache.stormcrawler.filtering.regex.RegexURLFilter;
import org.apache.stormcrawler.util.URLUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class RegexFilterTest {

    private URLFilter createFilter() {
        ObjectNode filterParams = new ObjectNode(JsonNodeFactory.instance);
        filterParams.put("regexFilterFile", "default-regex-filters.txt");
        return createFilter(filterParams);
    }

    private URLFilter createFilter(ObjectNode filterParams) {
        RegexURLFilter filter = new RegexURLFilter();
        Map<String, Object> conf = new HashMap<>();
        filter.configure(conf, filterParams);
        return filter;
    }

    @Test
    void testProtocolFilter() throws MalformedURLException {
        URLFilter allAllowed = createFilter();
        URL url = URLUtil.toURL("ftp://www.someFTP.com/#0");
        Metadata metadata = new Metadata();
        String filterResult = allAllowed.filter(url, metadata, url.toExternalForm());
        Assertions.assertNull(filterResult);
    }

    @Test
    void testImagesFilter() throws MalformedURLException {
        URLFilter allAllowed = createFilter();
        URL url = URLUtil.toURL("http://www.someFTP.com/bla.gif");
        Metadata metadata = new Metadata();
        String filterResult = allAllowed.filter(url, metadata, url.toExternalForm());
        Assertions.assertNull(filterResult);
        url = URLUtil.toURL("http://www.someFTP.com/bla.GIF");
        filterResult = allAllowed.filter(url, metadata, url.toExternalForm());
        Assertions.assertNull(filterResult);
        url = URLUtil.toURL("http://www.someFTP.com/bla.GIF&somearg=0");
        filterResult = allAllowed.filter(url, metadata, url.toExternalForm());
        Assertions.assertNull(filterResult);
        url = URLUtil.toURL("http://www.someFTP.com/bla.GIF?somearg=0");
        filterResult = allAllowed.filter(url, metadata, url.toExternalForm());
        Assertions.assertNull(filterResult);
        // not this one : the gif is within the path
        url = URLUtil.toURL("http://www.someFTP.com/bla.GIF.orNot");
        filterResult = allAllowed.filter(url, metadata, url.toExternalForm());
        Assertions.assertEquals(url.toExternalForm(), filterResult);
        url = URLUtil.toURL("http://www.someFTP.com/bla.mp4");
        filterResult = allAllowed.filter(url, metadata, url.toExternalForm());
        Assertions.assertNull(filterResult);
    }
}
