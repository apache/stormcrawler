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

import java.util.HashMap;
import java.util.Map;
import org.apache.storm.task.OutputCollector;
import org.apache.stormcrawler.Constants;
import org.apache.stormcrawler.TestUtil;
import org.apache.stormcrawler.util.ConfUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

/**
 * Regression coverage for the partitioner-to-fetcher path: the key the partitioner bolt emits must
 * be identical for host aliases (percent-escaping, trailing dot, case), otherwise the fetcher opens
 * one politeness queue per spelling.
 */
class URLPartitionerBoltTest {

    private OutputCollector collector;

    private URLPartitionerBolt bolt;

    @BeforeEach
    void setUp() {
        collector = Mockito.mock(OutputCollector.class);
        bolt = new URLPartitionerBolt();
        Map<String, Object> conf = new HashMap<>();
        bolt.prepare(conf, TestUtil.getMockedTopologyContext(), collector);
    }

    private String keyFor(String url) {
        Mockito.reset(collector);
        org.apache.storm.tuple.Tuple tuple = Mockito.mock(org.apache.storm.tuple.Tuple.class);
        Mockito.when(tuple.contains("metadata")).thenReturn(false);
        Mockito.when(tuple.getStringByField("url")).thenReturn(url);
        bolt.execute(tuple);
        ArgumentCaptor<org.apache.storm.tuple.Values> captor =
                ArgumentCaptor.forClass(org.apache.storm.tuple.Values.class);
        Mockito.verify(collector).emit(Mockito.eq(tuple), captor.capture());
        return (String) captor.getValue().get(1);
    }

    /** The shipped default partition mode is byHost. */
    @Test
    void byHostIsTheDefaultAndCollapsesAliases() {
        Assertions.assertEquals(
                ConfUtils.getString(
                        Map.of(Constants.PARTITION_MODEParamName, Constants.PARTITION_MODE_HOST),
                        Constants.PARTITION_MODEParamName,
                        Constants.PARTITION_MODE_HOST),
                Constants.PARTITION_MODE_HOST);
        Assertions.assertEquals(keyFor("http://example.org/a"), keyFor("http://exampl%65.org/a"));
        Assertions.assertEquals(keyFor("http://example.org/a"), keyFor("http://example.org./a"));
        Assertions.assertEquals(keyFor("http://example.org/a"), keyFor("http://EXAMPLE.org/a"));
    }

    /** The URL itself is not rewritten - only the key derived from it is canonicalised. */
    @Test
    void urlIsEmittedUnchanged() {
        Mockito.reset(collector);
        org.apache.storm.tuple.Tuple tuple = Mockito.mock(org.apache.storm.tuple.Tuple.class);
        Mockito.when(tuple.contains("metadata")).thenReturn(false);
        Mockito.when(tuple.getStringByField("url")).thenReturn("http://exampl%65.org/a");
        bolt.execute(tuple);
        ArgumentCaptor<org.apache.storm.tuple.Values> captor =
                ArgumentCaptor.forClass(org.apache.storm.tuple.Values.class);
        Mockito.verify(collector).emit(Mockito.eq(tuple), captor.capture());
        Assertions.assertEquals("http://exampl%65.org/a", captor.getValue().get(0));
    }
}
