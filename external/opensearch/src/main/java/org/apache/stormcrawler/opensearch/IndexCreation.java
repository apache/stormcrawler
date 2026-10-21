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

import com.google.common.io.Resources;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch.generic.Body;
import org.opensearch.client.opensearch.generic.Requests;
import org.opensearch.client.opensearch.generic.Response;
import org.opensearch.client.opensearch.indices.ExistsTemplateRequest;
import org.slf4j.Logger;

public class IndexCreation {

    /**
     * Creates the index with the {@code <boltType>.mapping} resource if it does not exist.
     *
     * @throws IllegalArgumentException if the index does not exist and the resource is not on the
     *     classpath
     */
    public static synchronized void checkOrCreateIndex(
            OpenSearchClient client, String indexName, String boltType, Logger log)
            throws IOException {
        final boolean indexExists = client.indices().exists(req -> req.index(indexName)).value();
        log.info("Index '{}' exists? {}", indexName, indexExists);
        // there's a possible check-then-update race condition
        if (!indexExists) {
            boolean created =
                    IndexCreation.createIndex(client, indexName, boltType + ".mapping", log);
            log.info("Index '{}' created? {} using {}", indexName, created, boltType + ".mapping");
        }
    }

    /**
     * Creates the template {@code <boltType>-template} with the {@code <boltType>.mapping} resource
     * if it does not exist.
     *
     * @throws IllegalArgumentException if the template does not exist and the resource is not on
     *     the classpath
     */
    public static synchronized void checkOrCreateIndexTemplate(
            OpenSearchClient client, String boltType, Logger log) throws IOException {
        final String templateName = boltType + "-template";
        final boolean templateExists =
                client.indices()
                        .existsTemplate(ExistsTemplateRequest.of(r -> r.name(templateName)))
                        .value();
        log.info("Template '{}' exists? {}", templateName, templateExists);
        // there's a possible check-then-update race condition
        if (!templateExists) {
            boolean created =
                    IndexCreation.createTemplate(client, templateName, boltType + ".mapping", log);
            log.info("templateExists '{}' created? {}", templateName, created);
        }
    }

    private static boolean createTemplate(
            OpenSearchClient client, String templateName, String resourceName, Logger log) {

        final URL mapping = Resources.getResource(resourceName);

        try {
            final String jsonIndexConfiguration =
                    Resources.toString(mapping, StandardCharsets.UTF_8);

            try (Response response =
                    client.generic()
                            .execute(
                                    Requests.builder()
                                            .endpoint("/_template/" + templateName)
                                            .method("PUT")
                                            .json(jsonIndexConfiguration)
                                            .build())) {
                int statusCode = response.getStatus();
                if (statusCode == 200 || statusCode == 201) {
                    return true;
                }
                log.warn(
                        "template '{}' not created: {} {}",
                        templateName,
                        statusCode,
                        response.getBody().map(Body::bodyAsString).orElse(""));
                return false;
            }
        } catch (Exception e) {
            log.warn("template '{}' not created", templateName, e);
            return false;
        }
    }

    private static boolean createIndex(
            OpenSearchClient client, String indexName, String resourceName, Logger log) {

        final URL mapping = Resources.getResource(resourceName);

        try {
            final String jsonIndexConfiguration =
                    Resources.toString(mapping, StandardCharsets.UTF_8);

            try (Response response =
                    client.generic()
                            .execute(
                                    Requests.builder()
                                            .endpoint("/" + indexName)
                                            .method("PUT")
                                            .json(jsonIndexConfiguration)
                                            .build())) {
                int statusCode = response.getStatus();
                if (statusCode == 200 || statusCode == 201) {
                    return true;
                }
                log.warn(
                        "index '{}' not created: {} {}",
                        indexName,
                        statusCode,
                        response.getBody().map(Body::bodyAsString).orElse(""));
                return false;
            }
        } catch (Exception e) {
            log.warn("index '{}' not created", indexName, e);
            return false;
        }
    }
}
