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

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Map;
import org.apache.stormcrawler.Metadata;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class MetadataFilterFromJsonTest {

    private URLFilters createURLFilters(String configFile) {
        return URLFilters.fromConf(Map.of("urlfilters.config.file", configFile));
    }

    // old filter mechanism (backward compatible)
    @Test
    void testFilterNoMD() throws MalformedURLException {
        URLFilters filter = createURLFilters("test.metadata.1.urlfilters.json");
        URL url = new URL("http://www.sourcedomain.com/");
        Metadata metadata = new Metadata();
        String filterResult = filter.filter(url, metadata, url.toExternalForm());
        Assertions.assertEquals(url.toExternalForm(), filterResult);
    }

    @Test
    void testFilterHit() throws MalformedURLException {
        URLFilters filter = createURLFilters("test.metadata.1.urlfilters.json");
        URL url = new URL("http://www.sourcedomain.com/");
        Metadata metadata = new Metadata();
        metadata.addValue("key", "val");
        String filterResult = filter.filter(url, metadata, url.toExternalForm());
        Assertions.assertNull(filterResult);
    }

    @Test
    void testFilterNoHit() throws MalformedURLException {
        URLFilters filter = createURLFilters("test.metadata.1.urlfilters.json");
        URL url = new URL("http://www.sourcedomain.com/");
        Metadata metadata = new Metadata();
        metadata.addValue("key", "val2");
        metadata.addValue("key", "val3");
        String filterResult = filter.filter(url, metadata, url.toExternalForm());
        Assertions.assertEquals(url.toExternalForm(), filterResult);
    }

    // new filter mechanism
    @Test
    void testNewFilterWithEmptyFilterAndNullMetadata() throws MalformedURLException {
        URLFilters filter = createURLFilters("test.metadata.2.urlfilters.json");
        URL url = new URL("http://www.sourcedomain.com/");
        String filterResult = filter.filter(url, null, url.toExternalForm());
        Assertions.assertEquals(url.toExternalForm(), filterResult);
    }

    @Test
    void testNewFilterWithEmptyFilterAndEmptyMetadata() throws MalformedURLException {
        URLFilters filter = createURLFilters("test.metadata.2.urlfilters.json");
        URL url = new URL("http://www.sourcedomain.com/");
        Metadata metadata = new Metadata();
        String filterResult = filter.filter(url, metadata, url.toExternalForm());
        Assertions.assertEquals(url.toExternalForm(), filterResult);
    }

    @Test
    void testNewFilterWithEmptyMetadata() throws MalformedURLException {
        URLFilters filter = createURLFilters("test.metadata.2.urlfilters.json");
        URL url = new URL("http://www.sourcedomain.com/");
        Metadata metadata = new Metadata();
        String filterResult = filter.filter(url, metadata, url.toExternalForm());
        Assertions.assertEquals(url.toExternalForm(), filterResult);
    }

    @Test
    void testNewFilterWithOnlyOneMatchingANDFilter() throws MalformedURLException {
        // Filter if key=>val AND key2=>val2 match
        URLFilters filter = createURLFilters("test.metadata.2.urlfilters.json");
        URL url = new URL("http://www.sourcedomain.com/");
        Metadata metadata = new Metadata();
        metadata.addValue("key", "val");
        String filterResult = filter.filter(url, metadata, url.toExternalForm());
        Assertions.assertEquals(url.toExternalForm(), filterResult);
    }

    @Test
    void testNewFilterWithAllMatchingANDFilter() throws MalformedURLException {
        // Filter if key=>val AND key2=>val2 match
        URLFilters filter = createURLFilters("test.metadata.2.urlfilters.json");
        URL url = new URL("http://www.sourcedomain.com/");
        Metadata metadata = new Metadata();
        metadata.addValue("key", "val");
        metadata.addValue("key2", "val2");
        String filterResult = filter.filter(url, metadata, url.toExternalForm());
        Assertions.assertNull(filterResult);
    }

    @Test
    void testNewFilterWithComplexFilter() throws MalformedURLException {
        // Filter if key=>val AND (key2=>val2 OR key3=>val3) match
        URLFilters filter = createURLFilters("test.metadata.3.urlfilters.json");
        URL url = new URL("http://www.sourcedomain.com/");

        Metadata metadata = new Metadata();
        metadata.addValue("key", "val");
        String filterResult = filter.filter(url, metadata, url.toExternalForm());
        Assertions.assertEquals(url.toExternalForm(), filterResult);

        metadata = new Metadata();
        metadata.addValue("key2", "val2");
        filterResult = filter.filter(url, metadata, url.toExternalForm());
        Assertions.assertEquals(url.toExternalForm(), filterResult);

        metadata = new Metadata();
        metadata.addValue("key", "val");
        metadata.addValue("key2", "val2");
        filterResult = filter.filter(url, metadata, url.toExternalForm());
        Assertions.assertNull(filterResult);

        metadata = new Metadata();
        metadata.addValue("key3", "val3");
        filterResult = filter.filter(url, metadata, url.toExternalForm());
        Assertions.assertEquals(url.toExternalForm(), filterResult);

        metadata = new Metadata();
        metadata.addValue("key", "val");
        metadata.addValue("key3", "val3");
        filterResult = filter.filter(url, metadata, url.toExternalForm());
        Assertions.assertNull(filterResult);

        metadata = new Metadata();
        metadata.addValue("key2", "val2");
        metadata.addValue("key3", "val3");
        filterResult = filter.filter(url, metadata, url.toExternalForm());
        Assertions.assertEquals(url.toExternalForm(), filterResult);
    }
}
