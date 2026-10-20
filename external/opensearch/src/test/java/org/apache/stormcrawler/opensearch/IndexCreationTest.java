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

package org.apache.stormcrawler.opensearch;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.function.Function;
import org.junit.jupiter.api.Test;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch.indices.ExistsTemplateRequest;
import org.opensearch.client.opensearch.indices.OpenSearchIndicesClient;
import org.opensearch.client.transport.endpoints.BooleanResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class IndexCreationTest {

    private static final Logger LOG = LoggerFactory.getLogger(IndexCreationTest.class);

    /** A client on which neither the index nor the template exists. */
    @SuppressWarnings("unchecked")
    private static OpenSearchClient emptyCluster() throws IOException {
        OpenSearchClient client = mock(OpenSearchClient.class);
        OpenSearchIndicesClient indices = mock(OpenSearchIndicesClient.class);
        when(client.indices()).thenReturn(indices);
        when(indices.exists(any(Function.class))).thenReturn(new BooleanResponse(false));
        when(indices.existsTemplate(any(ExistsTemplateRequest.class)))
                .thenReturn(new BooleanResponse(false));
        return client;
    }

    @Test
    void indexCreationFailsWhenTheMappingIsNotOnTheClasspath() throws IOException {
        OpenSearchClient client = emptyCluster();

        IllegalArgumentException e =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> IndexCreation.checkOrCreateIndex(client, "idx", "missing", LOG));

        assertTrue(e.getMessage().contains("missing.mapping"));
    }

    @Test
    void templateCreationFailsWhenTheMappingIsNotOnTheClasspath() throws IOException {
        OpenSearchClient client = emptyCluster();

        IllegalArgumentException e =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> IndexCreation.checkOrCreateIndexTemplate(client, "missing", LOG));

        assertTrue(e.getMessage().contains("missing.mapping"));
    }
}
