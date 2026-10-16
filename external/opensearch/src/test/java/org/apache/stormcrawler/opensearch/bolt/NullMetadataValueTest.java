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

package org.apache.stormcrawler.opensearch.bolt;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.TestOutputCollector;
import org.apache.stormcrawler.TestUtil;
import org.apache.stormcrawler.indexing.AbstractIndexerBolt;
import org.apache.stormcrawler.opensearch.IndexCreation;
import org.apache.stormcrawler.opensearch.OpenSearchConnection;
import org.apache.stormcrawler.opensearch.persistence.StatusUpdaterBolt;
import org.apache.stormcrawler.persistence.AbstractStatusUpdaterBolt;
import org.apache.stormcrawler.persistence.DefaultScheduler;
import org.apache.stormcrawler.persistence.Scheduler;
import org.apache.stormcrawler.persistence.Status;
import org.apache.stormcrawler.util.MetadataTransfer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.opensearch.client.opensearch.core.bulk.BulkOperation;

/**
 * Metadata values can be null. Runs without OpenSearch: the static connection factory and index
 * creation are mocked, and the bulk operation passed to the connection is inspected.
 */
class NullMetadataValueTest {

    private MockedStatic<OpenSearchConnection> connectionFactory;

    private MockedStatic<IndexCreation> indexCreation;

    private OpenSearchConnection connection;

    private TestOutputCollector output;

    private BaseRichBolt bolt;

    @BeforeEach
    void mockConnection() {
        connection = mock(OpenSearchConnection.class);
        connectionFactory = mockStatic(OpenSearchConnection.class);
        connectionFactory
                .when(() -> OpenSearchConnection.getConnection(any(), anyString(), any()))
                .thenReturn(connection);
        indexCreation = mockStatic(IndexCreation.class);
        output = new TestOutputCollector();
    }

    @AfterEach
    void cleanup() {
        try {
            // fails the tuples still waiting for a bulk response, so it runs after the assertions
            bolt.cleanup();
        } finally {
            indexCreation.close();
            connectionFactory.close();
        }
    }

    private static Metadata metadataWithNullValue() {
        Metadata md = new Metadata();
        md.setValue("key", null);
        md.addValue("key", "value");
        return md;
    }

    private static Tuple tuple() {
        Tuple tuple = mock(Tuple.class);
        when(tuple.getStringByField("url")).thenReturn("https://example.com/");
        when(tuple.getValueByField("status")).thenReturn(Status.FETCHED);
        when(tuple.getValueByField("metadata")).thenReturn(metadataWithNullValue());
        return tuple;
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> sentDocument() {
        ArgumentCaptor<BulkOperation> op = ArgumentCaptor.forClass(BulkOperation.class);
        verify(connection).addToProcessor(op.capture());
        return (Map<String, Object>) op.getValue().index().document();
    }

    @Test
    @SuppressWarnings("unchecked")
    void statusUpdaterBoltSendsNullValue() {
        Map<String, Object> conf = new HashMap<>();
        conf.put(Scheduler.schedulerClassParamName, DefaultScheduler.class.getName());
        conf.put(
                AbstractStatusUpdaterBolt.cacheConfigParamName,
                "maximumSize=10000,expireAfterAccess=1h");
        conf.put(MetadataTransfer.metadataPersistParamName, "key");
        bolt = new StatusUpdaterBolt();
        bolt.prepare(conf, TestUtil.getMockedTopologyContext(), new OutputCollector(output));

        bolt.execute(tuple());

        assertTrue(output.getFailedTuples().isEmpty());
        Map<String, Object> metadata = (Map<String, Object>) sentDocument().get("metadata");
        assertEquals(Arrays.asList(null, "value"), metadata.get("key"));
    }

    @Test
    void indexerBoltSendsNullValue() {
        Map<String, Object> conf = new HashMap<>();
        conf.put(AbstractIndexerBolt.urlFieldParamName, "url");
        conf.put(AbstractIndexerBolt.metadata2fieldParamName, "key");
        bolt = new IndexerBolt("content");
        bolt.prepare(conf, TestUtil.getMockedTopologyContext(), new OutputCollector(output));

        bolt.execute(tuple());

        assertTrue(output.getFailedTuples().isEmpty());
        assertEquals(Arrays.asList(null, "value"), sentDocument().get("key"));
    }
}
