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

package org.apache.stormcrawler;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.storm.metric.api.IMetric;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class TestUtil {

    private TestUtil() {}

    public static TopologyContext getMockedTopologyContext() {
        TopologyContext context = mock(TopologyContext.class);
        mockMetricRegistration(context);
        return context;
    }

    /**
     * Creates a mocked TopologyContext for testing bucket partitioning.
     *
     * @param taskIndex The task index for this spout instance (determines bucket number)
     * @param totalTasks Total number of tasks in the topology
     * @param componentId The component ID
     * @return Mocked TopologyContext
     */
    public static TopologyContext getMockedTopologyContextWithBucket(
            int taskIndex, int totalTasks, String componentId) {
        TopologyContext context = mock(TopologyContext.class);

        mockMetricRegistration(context);

        // Mock task information for bucket assignment
        when(context.getThisTaskIndex()).thenReturn(taskIndex);
        when(context.getThisComponentId()).thenReturn(componentId);

        // Create list of task IDs (0 to totalTasks-1)
        List<Integer> taskIds = new ArrayList<>();
        for (int i = 0; i < totalTasks; i++) {
            taskIds.add(i);
        }
        when(context.getComponentTasks(componentId)).thenReturn(taskIds);

        return context;
    }

    /** Sets up mock responses for both V1 and V2 metric registration on a TopologyContext. */
    private static void mockMetricRegistration(TopologyContext context) {
        // V1 metric registration
        when(context.registerMetric(anyString(), any(IMetric.class), anyInt()))
                .thenAnswer(
                        new Answer<IMetric>() {
                            @Override
                            public IMetric answer(InvocationOnMock invocation) throws Throwable {
                                return invocation.getArgument(1, IMetric.class);
                            }
                        });

        // V2 metric registration
        when(context.registerCounter(anyString()))
                .thenAnswer(invocation -> new com.codahale.metrics.Counter());
        when(context.registerHistogram(anyString()))
                .thenAnswer(
                        invocation ->
                                new com.codahale.metrics.Histogram(
                                        new com.codahale.metrics.ExponentiallyDecayingReservoir()));
        when(context.registerMeter(anyString()))
                .thenAnswer(invocation -> new com.codahale.metrics.Meter());
        when(context.registerGauge(anyString(), any(com.codahale.metrics.Gauge.class)))
                .thenAnswer(
                        invocation -> invocation.getArgument(1, com.codahale.metrics.Gauge.class));
    }

    public static Tuple getMockedTestTuple(String url, String content, Metadata metadata) {
        Tuple tuple = mock(Tuple.class);
        when(tuple.getStringByField("url")).thenReturn(url);
        when(tuple.getBinaryByField("content"))
                .thenReturn(content.getBytes(Charset.defaultCharset()));
        if (metadata == null) {
            when(tuple.contains("metadata")).thenReturn(Boolean.FALSE);
        } else {
            when(tuple.contains("metadata")).thenReturn(Boolean.TRUE);
            when(tuple.getValueByField("metadata")).thenReturn(metadata);
        }
        return tuple;
    }

    /** A more generic test tuple */
    public static Tuple getMockedTestTuple(final Map<String, Object> tupleValues) {
        Tuple tuple = mock(Tuple.class);
        when(tuple.contains(anyString()))
                .thenAnswer(
                        new Answer<Boolean>() {
                            @Override
                            public Boolean answer(InvocationOnMock invocation) throws Throwable {
                                return tupleValues.containsKey(
                                        invocation.getArgument(0, String.class));
                            }
                        });
        when(tuple.getValueByField(anyString()))
                .thenAnswer(
                        new Answer<>() {
                            @Override
                            public Object answer(InvocationOnMock invocation) throws Throwable {
                                return tupleValues.get(invocation.getArgument(0, String.class));
                            }
                        });
        when(tuple.getStringByField(anyString()))
                .thenAnswer(
                        new Answer<String>() {
                            @Override
                            public String answer(InvocationOnMock invocation) throws Throwable {
                                return (String)
                                        tupleValues.get(invocation.getArgument(0, String.class));
                            }
                        });
        return tuple;
    }
}
