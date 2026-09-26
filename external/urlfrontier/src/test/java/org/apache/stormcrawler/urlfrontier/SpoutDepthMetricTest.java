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

package org.apache.stormcrawler.urlfrontier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.util.HashMap;
import java.util.Map;
import org.apache.storm.metric.api.MultiCountMetric;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.TestUtil;
import org.apache.stormcrawler.util.MetadataTransfer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

/** The spout counts the URLs handed out by the frontier per depth level. */
class SpoutDepthMetricTest {

    private Spout spout;
    private MultiCountMetric depthMetric;
    private MultiCountMetric depthLeMetric;

    @BeforeEach
    void setUp() {
        openSpout(new HashMap<>());
    }

    @AfterEach
    void tearDown() {
        spout.close();
    }

    private void openSpout(Map<String, Object> conf) {
        conf.put(
                "urlbuffer.class", "org.apache.stormcrawler.persistence.urlbuffer.SimpleURLBuffer");
        TopologyContext context = TestUtil.getMockedTopologyContext();
        spout = new Spout();
        spout.open(conf, context, mock(SpoutOutputCollector.class));

        ArgumentCaptor<MultiCountMetric> captor = ArgumentCaptor.forClass(MultiCountMetric.class);
        verify(context).registerMetric(eq(Constants.DEPTH_METRIC_NAME), captor.capture(), anyInt());
        depthMetric = captor.getValue();

        ArgumentCaptor<MultiCountMetric> leCaptor = ArgumentCaptor.forClass(MultiCountMetric.class);
        verify(context)
                .registerMetric(eq(Constants.DEPTH_LE_METRIC_NAME), leCaptor.capture(), anyInt());
        depthLeMetric = leCaptor.getValue();
    }

    private static Metadata withDepth(String depth) {
        Metadata md = new Metadata();
        if (depth != null) {
            md.setValue(MetadataTransfer.depthKeyName, depth);
        }
        return md;
    }

    @Test
    void countsUrlsPerDepthLevel() {
        spout.recordDepth(withDepth("0"));
        spout.recordDepth(withDepth("1"));
        spout.recordDepth(withDepth("1"));
        spout.recordDepth(withDepth("3"));

        Map<String, Object> counts = depthMetric.getValueAndReset();
        assertEquals(1L, counts.get("0"));
        assertEquals(2L, counts.get("1"));
        assertFalse(counts.containsKey("2"));
        assertEquals(1L, counts.get("3"));
    }

    @Test
    void missingOrNonNumericDepthGoesToUnknown() {
        spout.recordDepth(withDepth(null));
        spout.recordDepth(withDepth("deep"));
        spout.recordDepth(withDepth("-1"));

        Map<String, Object> counts = depthMetric.getValueAndReset();
        assertEquals(3L, counts.get(Constants.DEPTH_METRIC_UNKNOWN_SCOPE));
        assertEquals(1, counts.size());
    }

    @Test
    void depthsAtOrBeyondTheCapShareOneBucket() {
        spout.recordDepth(withDepth("9"));
        spout.recordDepth(withDepth("10"));
        spout.recordDepth(withDepth("250"));

        Map<String, Object> counts = depthMetric.getValueAndReset();
        assertEquals(1L, counts.get("9"));
        assertEquals(2L, counts.get("10+"));
        assertFalse(counts.containsKey("250"));
    }

    @Test
    void capIsConfigurable() {
        spout.close();
        Map<String, Object> conf = new HashMap<>();
        conf.put(Constants.URLFRONTIER_DEPTH_METRIC_MAX_KEY, 3);
        openSpout(conf);

        spout.recordDepth(withDepth("2"));
        spout.recordDepth(withDepth("3"));
        spout.recordDepth(withDepth("7"));

        Map<String, Object> counts = depthMetric.getValueAndReset();
        assertEquals(1L, counts.get("2"));
        assertEquals(2L, counts.get("3+"));
    }

    @Test
    void cumulativeCountersCountEveryDepthAtOrBelowTheScope() {
        spout.recordDepth(withDepth("0"));
        spout.recordDepth(withDepth("1"));
        spout.recordDepth(withDepth("1"));
        spout.recordDepth(withDepth("3"));

        Map<String, Object> counts = depthLeMetric.getValueAndReset();
        assertEquals(1L, counts.get("0"));
        assertEquals(3L, counts.get("1"));
        assertEquals(3L, counts.get("2"));
        assertEquals(4L, counts.get("3"));
        assertEquals(4L, counts.get("9"));
        assertEquals(4L, counts.get(Constants.DEPTH_METRIC_INF_SCOPE));
        assertFalse(counts.containsKey("10"));
    }

    @Test
    void cumulativeCountersIgnoreUnknownDepthAndCountDeepUrlsOnlyInTotal() {
        spout.recordDepth(withDepth(null));
        spout.recordDepth(withDepth("250"));

        Map<String, Object> counts = depthLeMetric.getValueAndReset();
        assertFalse(counts.containsKey("9"));
        assertEquals(1L, counts.get(Constants.DEPTH_METRIC_INF_SCOPE));
    }

    @Test
    void probabilityOfDepthAtMostIsTheCumulativeShare() {
        spout.recordDepth(withDepth("0"));
        spout.recordDepth(withDepth("1"));
        spout.recordDepth(withDepth("1"));
        spout.recordDepth(withDepth("3"));
        spout.recordDepth(withDepth("deep"));

        assertEquals(0.25, spout.probabilityDepthAtMost(0), 1e-9);
        assertEquals(0.75, spout.probabilityDepthAtMost(1), 1e-9);
        assertEquals(0.75, spout.probabilityDepthAtMost(2), 1e-9);
        assertEquals(1.0, spout.probabilityDepthAtMost(3), 1e-9);
        assertEquals(1.0, spout.probabilityDepthAtMost(50), 1e-9);
    }

    @Test
    void probabilityAccountsForUrlsBeyondTheCap() {
        spout.recordDepth(withDepth("2"));
        spout.recordDepth(withDepth("250"));

        assertEquals(0.5, spout.probabilityDepthAtMost(9), 1e-9);
        assertEquals(1.0, spout.probabilityDepthAtMost(10), 1e-9);
    }

    @Test
    void probabilityIsNaNWithoutData() {
        assertTrue(Double.isNaN(spout.probabilityDepthAtMost(0)));
    }

    @Test
    void windowProbabilityFollowsRecordedDepths() {
        spout.recordDepth(withDepth("0"));
        spout.recordDepth(withDepth("4"));
        spout.recordDepth(withDepth("deep"));

        assertEquals(0.5, spout.windowProbabilityDepthAtMost(0), 1e-9);
        assertEquals(1.0, spout.windowProbabilityDepthAtMost(4), 1e-9);
    }
}
