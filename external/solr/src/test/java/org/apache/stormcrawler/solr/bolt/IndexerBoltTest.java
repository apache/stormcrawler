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
package org.apache.stormcrawler.solr.bolt;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.common.SolrDocumentList;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Tuple;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.TestOutputCollector;
import org.apache.stormcrawler.TestUtil;
import org.apache.stormcrawler.indexing.AbstractIndexerBolt;
import org.apache.stormcrawler.solr.SolrContainerTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class IndexerBoltTest extends SolrContainerTest {
    private IndexerBolt bolt;
    protected TestOutputCollector output;

    private static final Logger LOG = LoggerFactory.getLogger(IndexerBoltTest.class);

    @BeforeEach
    void setup() throws IOException, InterruptedException {
        createCollection("docs", 1);

        bolt = new IndexerBolt();
        output = new TestOutputCollector();

        Map<String, Object> conf = new HashMap<>();
        conf.put(AbstractIndexerBolt.urlFieldParamName, "url");
        conf.put(AbstractIndexerBolt.textFieldParamName, "content");
        conf.put(AbstractIndexerBolt.canonicalMetadataParamName, "canonical");
        conf.put("solr.indexer.url", getSolrBaseUrl() + "/docs");

        bolt.prepare(conf, TestUtil.getMockedTopologyContext(), new OutputCollector(output));
    }

    @AfterEach
    void close() {
        LOG.info("Closing Solr container");
        output = null;
    }

    private Future<Integer> index(String url, String text, Metadata metadata) {
        Tuple tuple = mock(Tuple.class);
        when(tuple.getStringByField("text")).thenReturn(text);
        when(tuple.getStringByField("url")).thenReturn(url);
        when(tuple.getValueByField("metadata")).thenReturn(metadata);
        bolt.execute(tuple);

        return executorService.submit(
                () -> {
                    var outputSize = output.getAckedTuples().size();
                    while (outputSize == 0) {
                        Thread.sleep(100);
                        outputSize = output.getAckedTuples().size();
                    }
                    return outputSize;
                });
    }

    @Test
    void basicTest()
            throws SolrServerException,
                    IOException,
                    ExecutionException,
                    InterruptedException,
                    TimeoutException {

        String url = "https://www.url.net/something";

        Metadata md = new Metadata();

        md.addValue("someKey", "someValue");

        index(url, "test", md).get(10, TimeUnit.SECONDS);

        assertEquals(1, output.getAckedTuples().size());

        // Flush concurrent update batch
        bolt.cleanup();

        // Make sure the document is indexed in Solr
        SolrClient client = new Http2SolrClient.Builder(getSolrBaseUrl() + "/docs").build();
        client.commit();

        SolrQuery query = new SolrQuery("*:*");
        SolrDocumentList solrDocs = client.query(query).getResults();

        assertEquals(1, solrDocs.getNumFound());
        assertEquals("[test]", solrDocs.get(0).get("content").toString());
    }
}
