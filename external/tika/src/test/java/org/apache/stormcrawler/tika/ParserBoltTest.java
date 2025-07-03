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
package org.apache.stormcrawler.tika;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.http.HttpHeaders;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Tuple;
import org.apache.stormcrawler.Constants;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.TestUtil;
import org.apache.stormcrawler.parse.ParsingTester;
import org.apache.stormcrawler.persistence.Status;
import org.apache.stormcrawler.protocol.ProtocolResponse;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ParserBoltTest extends ParsingTester {

    @BeforeEach
    void setupParserBolt() {
        bolt = new ParserBolt();
        setupParserBolt(bolt);
    }

    @Test
    /**
     * Checks that recursive docs are handled correctly
     *
     * @see https://issues.apache.org/jira/browse/TIKA-2096
     */
    void testRecursiveDoc() throws IOException {
        Map conf = new HashMap();
        conf.put("parser.extract.embedded", true);
        bolt.prepare(conf, TestUtil.getMockedTopologyContext(), new OutputCollector(output));
        parse(
                "https://stormcrawler.apache.org/test_recursive_embedded.docx",
                "test_recursive_embedded.docx");
        List<List<Object>> outTuples = output.getEmitted();
        // TODO could we get as many subdocs as embedded in the original one?
        // or just one for now? but should at least contain the text of the
        // subdocs
        Assertions.assertEquals(1, outTuples.size());
        Assertions.assertTrue(
                outTuples
                        .get(0)
                        .get(3)
                        .toString()
                        .contains("Life, Liberty and the pursuit of Happiness"));
    }

    @Test
    /**
     * Checks that the mimetype whitelists are handled correctly
     *
     * @see https://github.com/apache/stormcrawler/issues/712
     */
    void testMimeTypeWhileList() throws IOException {
        Map conf = new HashMap();
        conf.put("parser.mimetype.whitelist", "application/.+word.*");
        conf.put(ProtocolResponse.PROTOCOL_MD_PREFIX_PARAM, "http.");
        bolt.prepare(conf, TestUtil.getMockedTopologyContext(), new OutputCollector(output));
        String url = "http://thisisatest.com/adoc.pdf";
        Metadata metadata = new Metadata();
        metadata.addValue("http." + HttpHeaders.CONTENT_TYPE, "application/pdf");
        byte[] content = new byte[] {};
        Tuple tuple = mock(Tuple.class);
        when(tuple.getBinaryByField("content")).thenReturn(content);
        when(tuple.getStringByField("url")).thenReturn(url);
        when(tuple.getValueByField("metadata")).thenReturn(metadata);
        bolt.execute(tuple);
        List<List<Object>> outTuples = output.getEmitted(Constants.StatusStreamName);
        Assertions.assertEquals(1, outTuples.size());
        Assertions.assertTrue(outTuples.get(0).get(2).equals(Status.ERROR));
        outTuples.clear();
        metadata = new Metadata();
        metadata.addValue(
                "http." + HttpHeaders.CONTENT_TYPE,
                "application/vnd.openxmlformats-officedocument.wordprocessingml.document");
        parse(
                "https://stormcrawler.apache.org/test_recursive_embedded.docx",
                "test_recursive_embedded.docx",
                metadata);
        outTuples = output.getEmitted();
        Assertions.assertEquals(1, outTuples.size());
    }
}
