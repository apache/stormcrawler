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
package org.apache.stormcrawler.warc;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.stormcrawler.TestOutputCollector;
import org.apache.stormcrawler.TestUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class WARCSpoutTest {

    private TestOutputCollector output;

    private WARCSpout spout;

    private Map<String, Object> conf;

    @BeforeEach
    void setup() throws IOException {
        output = new TestOutputCollector();
        // pass it as input to the spout
        java.io.File refInputFile = new java.io.File("src/test/resources/warc.inputs");
        Map<String, Object> hdfsConf = new HashMap<>();
        hdfsConf.put("fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem");
        conf = new HashMap<>();
        conf.put("hdfs", hdfsConf);
        spout = new WARCSpout(refInputFile.getAbsolutePath());
        spout.open(conf, TestUtil.getMockedTopologyContext(), new SpoutOutputCollector(output));
        spout.activate();
    }

    @AfterEach
    void cleanup() {
        spout.close();
        output = null;
    }

    /*
     * Parsing the WARC file should produce 17 tuples (test.warc has 17 records) without
     * failing due to the unparsable record in unparsable-date.warc
     */
    @Test
    void test() throws IOException {
        int expected = 17;
        while (output.getEmitted().size() < expected) {
            spout.nextTuple();
        }
        assertEquals(output.getEmitted().size(), expected);
    }
}
