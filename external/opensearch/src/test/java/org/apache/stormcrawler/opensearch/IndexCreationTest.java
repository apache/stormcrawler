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
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.function.Function;
import org.junit.jupiter.api.Test;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch.generic.Body;
import org.opensearch.client.opensearch.generic.OpenSearchGenericClient;
import org.opensearch.client.opensearch.generic.Request;
import org.opensearch.client.opensearch.generic.Response;
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

    private static final String REJECTION =
            "{\"error\":{\"type\":\"mapper_parsing_exception\"},\"status\":400}";

    /** A client on which neither the index nor the template exists and that rejects both. */
    private static OpenSearchClient rejectingCluster() throws IOException {
        OpenSearchClient client = emptyCluster();
        Response response = mock(Response.class);
        when(response.getStatus()).thenReturn(400);
        when(response.getBody())
                .thenReturn(
                        Optional.of(
                                Body.from(
                                        REJECTION.getBytes(StandardCharsets.UTF_8),
                                        "application/json")));
        OpenSearchGenericClient generic = mock(OpenSearchGenericClient.class);
        when(generic.execute(any(Request.class))).thenReturn(response);
        when(client.generic()).thenReturn(generic);
        return client;
    }

    @Test
    void rejectedIndexIsLoggedWithStatusAndBody() throws IOException {
        Logger log = mock(Logger.class);

        IndexCreation.checkOrCreateIndex(rejectingCluster(), "idx", "status", log);

        verify(log).warn(anyString(), eq("idx"), eq(400), eq(REJECTION));
    }

    @Test
    void rejectedTemplateIsLoggedWithStatusAndBody() throws IOException {
        Logger log = mock(Logger.class);

        IndexCreation.checkOrCreateIndexTemplate(rejectingCluster(), "status", log);

        verify(log).warn(anyString(), eq("status-template"), eq(400), eq(REJECTION));
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
