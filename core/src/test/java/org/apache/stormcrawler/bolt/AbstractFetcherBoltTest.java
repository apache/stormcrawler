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
package org.apache.stormcrawler.bolt;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlMatching;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.github.tomakehurst.wiremock.junit5.WireMockTest;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.Utils;
import org.apache.stormcrawler.Constants;
import org.apache.stormcrawler.TestOutputCollector;
import org.apache.stormcrawler.TestUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

@WireMockTest
abstract class AbstractFetcherBoltTest {

    BaseRichBolt bolt;

    @AfterEach
    void cleanupParserBolt() {
        bolt.cleanup();
    }

    @Test
    void testDodgyURL() throws IOException {
        TestOutputCollector output = new TestOutputCollector();
        Map<String, Object> config = new HashMap<>();
        config.put("http.agent.name", "this_is_only_a_test");
        bolt.prepare(config, TestUtil.getMockedTopologyContext(), new OutputCollector(output));
        Tuple tuple = mock(Tuple.class);
        when(tuple.getSourceComponent()).thenReturn("source");
        when(tuple.getStringByField("url")).thenReturn("ahahaha");
        when(tuple.getValueByField("metadata")).thenReturn(null);
        bolt.execute(tuple);
        boolean acked = output.getAckedTuples().contains(tuple);
        boolean failed = output.getAckedTuples().contains(tuple);
        // should be acked or failed
        Assertions.assertTrue(acked || failed);
        List<List<Object>> statusTuples = output.getEmitted(Constants.StatusStreamName);
        // we should get one tuple on the status stream
        // to notify that the URL is an error
        Assertions.assertEquals(1, statusTuples.size());
    }

    @Test
    void test304(WireMockRuntimeInfo wmRuntimeInfo) {
        stubFor(get(urlMatching(".+")).willReturn(aResponse().withStatus(304)));
        TestOutputCollector output = new TestOutputCollector();
        Map<String, Object> config = new HashMap<>();
        config.put("http.agent.name", "this_is_only_a_test");
        bolt.prepare(config, TestUtil.getMockedTopologyContext(), new OutputCollector(output));
        Tuple tuple = mock(Tuple.class);
        when(tuple.getSourceComponent()).thenReturn("source");
        when(tuple.getStringByField("url"))
                .thenReturn("http://localhost:" + wmRuntimeInfo.getHttpPort() + "/");
        when(tuple.getValueByField("metadata")).thenReturn(null);
        bolt.execute(tuple);
        while (output.getAckedTuples().size() == 0 && output.getFailedTuples().size() == 0) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
            }
        }
        boolean acked = output.getAckedTuples().contains(tuple);
        boolean failed = output.getFailedTuples().contains(tuple);
        // should be acked or failed
        Assertions.assertTrue(acked || failed);
        List<List<Object>> statusTuples = output.getEmitted(Constants.StatusStreamName);
        // we should get one tuple on the status stream
        // to notify that the URL has been fetched
        Assertions.assertEquals(1, statusTuples.size());
        // and none on the default stream as there is nothing to parse and/or
        // index
        Assertions.assertEquals(0, output.getEmitted(Utils.DEFAULT_STREAM_ID).size());
    }
}
