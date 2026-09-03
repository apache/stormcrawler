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

import java.io.IOException;
import java.util.List;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.bolt.JSoupParserBolt;
import org.apache.stormcrawler.parse.ParsingTester;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SubDocumentsFilterTest extends ParsingTester {

    @BeforeEach
    void setupParserBolt() {
        bolt = new JSoupParserBolt();
        setupParserBolt(bolt);
    }

    @Test
    void testSubdocuments() throws IOException {
        prepareParserBolt("test.subdocfilter.json");
        Metadata metadata = new Metadata();
        parse("https://stormcrawler.apache.org/", "subdocuments.html", metadata);
        Assertions.assertEquals(7, output.getEmitted().size());
    }

    @Test
    void testEmptySubDocumentsAreNotEmitted() throws IOException {
        prepareParserBolt("test.emptysubdocfilter.json");
        parse("https://stormcrawler.apache.org/", "subdocuments.html", new Metadata());
        List<List<Object>> emitted = output.getEmitted();
        // only the document itself is emitted; the empty entry created by the
        // filter for a URL which was never parsed must be skipped
        Assertions.assertEquals(1, emitted.size());
        Assertions.assertEquals("https://stormcrawler.apache.org/", emitted.get(0).get(0));
    }

    @Test
    void testEmptiedParentDocumentIsStillEmitted() throws IOException {
        prepareParserBolt("test.emptyparentfilter.json");
        parse("https://stormcrawler.apache.org/", "subdocuments.html", new Metadata());
        List<List<Object>> emitted = output.getEmitted();
        // the entry for the URL being parsed is always emitted, even when a
        // filter emptied it, so that its status keeps being updated downstream
        Assertions.assertEquals(1, emitted.size());
        Assertions.assertEquals("https://stormcrawler.apache.org/", emitted.get(0).get(0));
    }
}
