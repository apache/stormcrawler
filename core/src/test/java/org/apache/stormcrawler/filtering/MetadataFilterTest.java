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
import org.apache.stormcrawler.filtering.metadata.MetadataFilter;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class MetadataFilterTest {

    private URLFilter createFilter(String key, String value) {
        MetadataFilter filter = new MetadataFilter();
        ObjectNode filterParams = new ObjectNode(JsonNodeFactory.instance);
        filterParams.put(key, value);
        Map<String, Object> conf = new HashMap<>();
        filter.configure(conf, filterParams);
        return filter;
    }

    // old filter mechanism (backward compatible)
    @Test
    void testFilterNoMD() throws MalformedURLException {
        URLFilter filter = createFilter("key", "val");
        URL url = new URL("http://www.sourcedomain.com/");
        Metadata metadata = new Metadata();
        String filterResult = filter.filter(url, metadata, url.toExternalForm());
        Assertions.assertEquals(url.toExternalForm(), filterResult);
    }

    @Test
    void testFilterHit() throws MalformedURLException {
        URLFilter filter = createFilter("key", "val");
        URL url = new URL("http://www.sourcedomain.com/");
        Metadata metadata = new Metadata();
        metadata.addValue("key", "val");
        String filterResult = filter.filter(url, metadata, url.toExternalForm());
        Assertions.assertNull(filterResult);
    }

    @Test
    void testFilterNoHit() throws MalformedURLException {
        URLFilter filter = createFilter("key", "val");
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
        MetadataFilter filter = new MetadataFilter();
        URL url = new URL("http://www.sourcedomain.com/");
        String filterResult = filter.filter(url, null, url.toExternalForm());
        Assertions.assertEquals(url.toExternalForm(), filterResult);
    }

    @Test
    void testNewFilterWithEmptyFilterAndEmptyMetadata() throws MalformedURLException {
        MetadataFilter filter = new MetadataFilter();
        URL url = new URL("http://www.sourcedomain.com/");
        Metadata metadata = new Metadata();
        String filterResult = filter.filter(url, metadata, url.toExternalForm());
        Assertions.assertEquals(url.toExternalForm(), filterResult);
    }

    @Test
    void testNewFilterWithEmptyFilter() throws MalformedURLException {
        MetadataFilter filter = new MetadataFilter();
        URL url = new URL("http://www.sourcedomain.com/");
        Metadata metadata = new Metadata();
        metadata.addValue("key", "val");
        String filterResult = filter.filter(url, metadata, url.toExternalForm());
        Assertions.assertEquals(url.toExternalForm(), filterResult);
    }

    @Test
    void testNewFilterWithEmptyMetadata() throws MalformedURLException {
        MetadataFilter filter = new MetadataFilter();
        filter.addFilter("key", "val");
        URL url = new URL("http://www.sourcedomain.com/");
        Metadata metadata = new Metadata();
        String filterResult = filter.filter(url, metadata, url.toExternalForm());
        Assertions.assertEquals(url.toExternalForm(), filterResult);
    }

    @Test
    void testNewFilterWithSingleMatchingORFilter() throws MalformedURLException {
        // Filter if key=>val match (OR operation)
        MetadataFilter filter = new MetadataFilter();
        filter.addFilter("key", "val");
        URL url = new URL("http://www.sourcedomain.com/");
        Metadata metadata = new Metadata();
        metadata.addValue("key", "val");
        String filterResult = filter.filter(url, metadata, url.toExternalForm());
        Assertions.assertNull(filterResult);
    }

    @Test
    void testNewFilterWithSingleMatchingANDFilter() throws MalformedURLException {
        // Filter if key=>val match (AND operation)
        MetadataFilter filter = new MetadataFilter();
        filter.addFilter("key", "val");
        filter.setOperation(MetadataFilter.FilterOperation.AND);
        URL url = new URL("http://www.sourcedomain.com/");
        Metadata metadata = new Metadata();
        metadata.addValue("key", "val");
        String filterResult = filter.filter(url, metadata, url.toExternalForm());
        Assertions.assertNull(filterResult);
    }

    @Test
    void testNewFilterWithOnlyOneMatchingORFilter() throws MalformedURLException {
        // Filter if key=>val OR key2=>val2 match
        MetadataFilter filter = new MetadataFilter();
        filter.addFilter("key", "val");
        filter.addFilter("key2", "val2");
        URL url = new URL("http://www.sourcedomain.com/");
        Metadata metadata = new Metadata();
        metadata.addValue("key", "val");
        String filterResult = filter.filter(url, metadata, url.toExternalForm());
        Assertions.assertNull(filterResult);
    }

    @Test
    void testNewFilterWithOnlyOneMatchingANDFilter() throws MalformedURLException {
        // Filter if key=>val AND key2=>val2 match
        MetadataFilter filter = new MetadataFilter();
        filter.addFilter("key", "val");
        filter.addFilter("key2", "val2");
        filter.setOperation(MetadataFilter.FilterOperation.AND);
        URL url = new URL("http://www.sourcedomain.com/");
        Metadata metadata = new Metadata();
        metadata.addValue("key", "val");
        String filterResult = filter.filter(url, metadata, url.toExternalForm());
        Assertions.assertEquals(url.toExternalForm(), filterResult);
    }

    @Test
    void testNewFilterWithAllMatchingANDFilter() throws MalformedURLException {
        // Filter if key=>val AND key2=>val2 match
        MetadataFilter filter = new MetadataFilter();
        filter.addFilter("key", "val");
        filter.addFilter("key2", "val2");
        filter.setOperation(MetadataFilter.FilterOperation.AND);
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
        MetadataFilter filter = new MetadataFilter();
        filter.addFilter("key", "val");
        filter.setOperation(MetadataFilter.FilterOperation.AND);
        MetadataFilter.ComplexFilter filter2 = new MetadataFilter.ComplexFilter();
        filter2.addFilter("key2", "val2");
        filter2.addFilter("key3", "val3");
        filter.addFilter(filter2);
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

    @Test
    void testNewFilterWithOtherComplexFilter() throws MalformedURLException {
        // Filter if key=>val OR (key2=>val2 AND key3=>val3) match
        MetadataFilter filter = new MetadataFilter();
        filter.addFilter("key", "val");
        MetadataFilter.ComplexFilter filter2 = new MetadataFilter.ComplexFilter();
        filter2.addFilter("key2", "val2");
        filter2.addFilter("key3", "val3");
        filter2.setOperation(MetadataFilter.FilterOperation.AND);
        filter.addFilter(filter2);
        URL url = new URL("http://www.sourcedomain.com/");

        Metadata metadata = new Metadata();
        metadata.addValue("key", "val");
        String filterResult = filter.filter(url, metadata, url.toExternalForm());
        Assertions.assertNull(filterResult);

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
        Assertions.assertNull(filterResult);
    }

    // configure() backward compatible
    @Test
    void testConfigureBackwardCompatible() {
        MetadataFilter filter = new MetadataFilter();
        ObjectNode filterParams = new ObjectNode(JsonNodeFactory.instance);
        filterParams.put("key", "val");
        Map<String, Object> conf = new HashMap<>();
        filter.configure(conf, filterParams);

        MetadataFilter expectedFilter = new MetadataFilter();
        expectedFilter.addFilter("key", "val");

        Assertions.assertEquals(expectedFilter, filter);
    }

    // new configure()
    @Test
    void testNewConfigure() {
        MetadataFilter filter = new MetadataFilter();
        ObjectNode filterParams = new ObjectNode(JsonNodeFactory.instance);
        filterParams.put("operation", "AND");
        ObjectNode filters = new ObjectNode(JsonNodeFactory.instance);
        filters.put("key", "val");
        filterParams.set("filters", filters);
        Map<String, Object> conf = new HashMap<>();
        filter.configure(conf, filterParams);

        MetadataFilter expectedFilter = new MetadataFilter();
        expectedFilter.addFilter("key", "val");
        expectedFilter.setOperation(MetadataFilter.FilterOperation.AND);

        Assertions.assertEquals(expectedFilter, filter);
    }

    @Test
    void testNewConfigureWithComplexFilter() {
        MetadataFilter filter = new MetadataFilter();
        ObjectNode filterParams = new ObjectNode(JsonNodeFactory.instance);
        filterParams.put("operation", "OR");
        ObjectNode filters = new ObjectNode(JsonNodeFactory.instance);
        filters.put("key", "val");
        ObjectNode filters2 = new ObjectNode(JsonNodeFactory.instance);
        filters2.put("operation", "AND");
        ObjectNode subfilters = new ObjectNode(JsonNodeFactory.instance);
        subfilters.put("key2", "val2");
        subfilters.put("key3", "val3");
        filters2.set("filters", subfilters);
        filters.set("unique_key_for_complex_filtering_1", filters2);
        filterParams.set("filters", filters);
        Map<String, Object> conf = new HashMap<>();
        filter.configure(conf, filterParams);

        // Filter if key=>val OR (key2=>val2 AND key3=>val3) match
        MetadataFilter expectedFilter = new MetadataFilter();
        expectedFilter.addFilter("key", "val");
        MetadataFilter.ComplexFilter filter2 = new MetadataFilter.ComplexFilter();
        filter2.addFilter("key2", "val2");
        filter2.addFilter("key3", "val3");
        filter2.setOperation(MetadataFilter.FilterOperation.AND);
        expectedFilter.addFilter(filter2);

        Assertions.assertEquals(expectedFilter, filter);
    }
}
