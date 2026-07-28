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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.stormcrawler.Constants;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.TestUtil;
import org.apache.stormcrawler.persistence.Status;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

/**
 * Tuples emitted by {@link URLFilterBolt} must be anchored on the tuple they were derived from,
 * otherwise the acker cannot tie the downstream tree back to the spout and failures go unnoticed.
 *
 * @see <a href="https://github.com/apache/stormcrawler/issues/2021">Issue #2021</a>
 */
class URLFilterBoltTest {

    /** Passes the filter chain declared in {@code test.urlfilterbolt.json}. */
    private static final String ALLOWED_URL = "http://may.go.com/page.html";

    /** Rejected by {@code default-regex-filters.txt} because of the image extension. */
    private static final String REJECTED_URL = "http://no.go.com/image.jpg";

    private static final String FILTER_CONFIG = "test.urlfilterbolt.json";

    private OutputCollector collector;

    private URLFilterBolt bolt;

    @BeforeEach
    void setUp() {
        collector = mock(OutputCollector.class);
        bolt = prepareBolt(false);
    }

    private URLFilterBolt prepareBolt(boolean discoveredOnly) {
        URLFilterBolt prepared = new URLFilterBolt(discoveredOnly, FILTER_CONFIG);
        Map<String, Object> conf = new HashMap<>();
        prepared.prepare(conf, TestUtil.getMockedTopologyContext(), collector);
        return prepared;
    }

    private static Tuple statusTuple(String url, Status status) {
        Tuple tuple = mock(Tuple.class);
        when(tuple.getSourceStreamId()).thenReturn(Constants.StatusStreamName);
        when(tuple.getStringByField("url")).thenReturn(url);
        when(tuple.getValueByField("metadata")).thenReturn(new Metadata());
        when(tuple.getValueByField("status")).thenReturn(status);
        return tuple;
    }

    @Test
    void emitsAnchoredOnTheIncomingStream() {
        Tuple input = statusTuple(ALLOWED_URL, Status.DISCOVERED);

        bolt.execute(input);

        ArgumentCaptor<Values> captor = ArgumentCaptor.forClass(Values.class);
        verify(collector).emit(eq(Constants.StatusStreamName), eq(input), captor.capture());
        verify(collector).ack(input);

        Values emitted = captor.getValue();
        Assertions.assertEquals(ALLOWED_URL, emitted.get(0));
        Assertions.assertEquals(Status.DISCOVERED, emitted.get(2));
    }

    @Test
    void neverUsesTheUnanchoredEmit() {
        bolt.execute(statusTuple(ALLOWED_URL, Status.DISCOVERED));

        verify(collector, never()).emit(anyString(), anyList());
    }

    @Test
    void rejectedUrlIsAckedAndNotEmitted() {
        Tuple input = statusTuple(REJECTED_URL, Status.DISCOVERED);

        bolt.execute(input);

        verify(collector).ack(input);
        verify(collector, never()).emit(anyString(), any(Tuple.class), anyList());
        verify(collector, never()).fail(any(Tuple.class));
    }

    @Test
    void passesThroughUnfilteredWhenDiscoveredOnly() {
        // discoveredOnly short-circuit: a non-DISCOVERED status skips the filters entirely, but
        // must still be emitted anchored - it is the second emit site in the upstream bug. Using a
        // URL the filters would reject proves the short-circuit really ran.
        bolt = prepareBolt(true);

        Tuple input = statusTuple(REJECTED_URL, Status.FETCHED);

        bolt.execute(input);

        ArgumentCaptor<Values> captor = ArgumentCaptor.forClass(Values.class);
        verify(collector).emit(eq(Constants.StatusStreamName), eq(input), captor.capture());
        verify(collector).ack(input);
        Assertions.assertEquals(REJECTED_URL, captor.getValue().get(0));
    }
}
