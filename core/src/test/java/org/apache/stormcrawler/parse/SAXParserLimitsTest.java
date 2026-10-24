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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import javax.xml.parsers.SAXParserFactory;
import org.junit.jupiter.api.Test;
import org.xml.sax.helpers.DefaultHandler;

/**
 * Checks that the SAX factory used by crawler-commons, ROME and Tika's timeout path parses repeated
 * entity references.
 */
class SAXParserLimitsTest {

    @Test
    void parsesManyEntityReferences() {
        byte[] xml =
                ("<urlset>" + "&amp;".repeat(120_000) + "</urlset>")
                        .getBytes(StandardCharsets.UTF_8);
        assertDoesNotThrow(
                () ->
                        SAXParserFactory.newInstance()
                                .newSAXParser()
                                .parse(new ByteArrayInputStream(xml), new DefaultHandler()));
    }
}
