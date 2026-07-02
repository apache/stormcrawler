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

import com.fasterxml.jackson.databind.JsonNode;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import org.apache.stormcrawler.JSONResource;
import org.opensearch.client.json.JsonData;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch.core.GetResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Loads a delegate class that implements both a required base type and {@link JSONResource}, then
 * periodically refreshes its configuration from OpenSearch. Used by {@link
 * org.apache.stormcrawler.opensearch.filtering.JSONURLFilterWrapper} and {@link
 * org.apache.stormcrawler.opensearch.parse.filter.JSONResourceWrapper} to eliminate duplicated
 * setup/refresh/cleanup logic.
 *
 * <p>This is the opensearch-java (OpenSearch Java Client 3.x / HC5) counterpart of the class with
 * the same name in the {@code external/opensearch} module. It uses the typed {@link
 * OpenSearchClient} instead of the deprecated {@code RestHighLevelClient}.
 *
 * @param <T> the base type that the delegate must extend (e.g. URLFilter or ParseFilter)
 */
public class DelegateRefresher<T> {

    private static final Logger LOG = LoggerFactory.getLogger(DelegateRefresher.class);

    private final T delegate;
    private Timer refreshTimer;
    private OpenSearchClient osClient;

    /**
     * Creates a refresher by loading the delegate class from the JSON configuration.
     *
     * @param baseType the required base class (e.g. URLFilter.class or ParseFilter.class)
     * @param stormConf the Storm configuration map
     * @param filterParams the JSON params node containing "delegate" and optional "refresh"
     * @param configurer callback to configure the delegate after instantiation
     */
    public DelegateRefresher(
            Class<T> baseType,
            Map<String, Object> stormConf,
            JsonNode filterParams,
            DelegateConfigure<T> configurer) {

        JsonNode delegateNode = filterParams.get("delegate");
        if (delegateNode == null) {
            throw new RuntimeException("delegateNode undefined!");
        }

        String delegateClassName = null;
        JsonNode node = delegateNode.get("class");
        if (node != null && node.isTextual()) {
            delegateClassName = node.asText();
        }
        if (delegateClassName == null) {
            throw new RuntimeException(baseType.getSimpleName() + " delegate class undefined!");
        }

        try {
            Class<?> filterClass = Class.forName(delegateClassName);

            if (!baseType.isAssignableFrom(filterClass)) {
                throw new RuntimeException(
                        "Filter " + delegateClassName + " does not extend " + baseType.getName());
            }

            @SuppressWarnings("unchecked")
            T instance = (T) filterClass.getDeclaredConstructor().newInstance();

            if (!(instance instanceof JSONResource)) {
                throw new RuntimeException(
                        "Filter " + delegateClassName + " does not implement JSONResource");
            }

            this.delegate = instance;
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            LOG.error("Can't setup {}: {}", delegateClassName, e);
            throw new RuntimeException("Can't setup " + delegateClassName, e);
        }

        // configure the delegate
        JsonNode paramsNode = delegateNode.get("params");
        configurer.configure(delegate, stormConf, paramsNode);

        // set up periodic refresh from OpenSearch
        int refreshRate = 600;
        node = filterParams.get("refresh");
        if (node != null && (node.isInt() || node.isTextual())) {
            refreshRate = node.asInt(refreshRate);
        }

        final JSONResource resource = (JSONResource) delegate;

        refreshTimer = new Timer();
        refreshTimer.schedule(
                new TimerTask() {
                    public void run() {
                        if (osClient == null) {
                            try {
                                osClient = OpenSearchConnection.getClient(stormConf, "config");
                            } catch (Exception e) {
                                LOG.error("Exception while creating OpenSearch connection", e);
                            }
                        }
                        if (osClient != null) {
                            LOG.info("Reloading json resources from OpenSearch");
                            try {
                                GetResponse<JsonData> response =
                                        osClient.get(
                                                g ->
                                                        g.index("config")
                                                                .id(resource.getResourceFile()),
                                                JsonData.class);
                                if (response.found() && response.source() != null) {
                                    String json = response.source().toJson().toString();
                                    resource.loadJSONResources(
                                            new ByteArrayInputStream(
                                                    json.getBytes(StandardCharsets.UTF_8)));
                                }
                            } catch (Exception e) {
                                LOG.error("Can't load config from OpenSearch", e);
                            }
                        }
                    }
                },
                refreshRate * 1000L,
                refreshRate * 1000L);
    }

    /** Returns the delegate instance. */
    public T getDelegate() {
        return delegate;
    }

    /** Cancels the refresh timer and closes the OpenSearch client. */
    public void cleanup() {
        if (refreshTimer != null) {
            refreshTimer.cancel();
        }
        if (osClient != null) {
            try {
                osClient._transport().close();
            } catch (IOException e) {
                LOG.error("Exception when closing OpenSearch client", e);
            }
            osClient = null;
        }
    }

    /** Callback interface for configuring the delegate after instantiation. */
    @FunctionalInterface
    public interface DelegateConfigure<T> {
        void configure(T delegate, Map<String, Object> stormConf, JsonNode params);
    }
}
