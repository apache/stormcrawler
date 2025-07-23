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
package org.apache.stormcrawler.solr;

import java.io.File;
import org.junit.jupiter.api.BeforeAll;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.ComposeContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.containers.wait.strategy.WaitStrategy;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.MountableFile;

@Testcontainers
public abstract class SolrCloudContainerTest {
    private static final Logger LOG = LoggerFactory.getLogger(SolrCloudContainerTest.class);

    private static final String configsetsPath = new File("configsets").getAbsolutePath();
    private static final WaitStrategy waitStrategy =
            Wait.forHttp("/solr/admin/cores?action=STATUS").forStatusCode(200);

    @Container
    static ComposeContainer environment =
            new ComposeContainer(new File("src/test/resources/docker-compose.yml"))
                    .withExposedService("solr", 8983, waitStrategy)
                    .withExposedService("zookeeper", 2181);

    @BeforeAll
    static void before() {
        environment
                .getContainerByServiceName("solr")
                .ifPresent(
                        container -> {
                            container.copyFileToContainer(
                                    MountableFile.forHostPath(configsetsPath),
                                    "/opt/solr/server/solr/configsets");
                        });
    }

    protected static void createCollection(String collectionName, int shards, int replicas) {
        environment
                .getContainerByServiceName("solr")
                .ifPresent(
                        container -> {
                            try {
                                // Upload configuration to Zookeeper
                                container.execInContainer(
                                        "/opt/solr/bin/solr",
                                        "zk",
                                        "upconfig",
                                        "-n",
                                        collectionName,
                                        "-d",
                                        "/opt/solr/server/solr/configsets/" + collectionName,
                                        "-z",
                                        "zookeeper:2181");

                                // Create the collection
                                container.execInContainer(
                                        "/opt/solr/bin/solr",
                                        "create",
                                        "-c",
                                        collectionName,
                                        "-n",
                                        collectionName,
                                        "-sh",
                                        String.valueOf(shards),
                                        "-rf",
                                        String.valueOf(replicas));
                            } catch (Exception e) {
                                LOG.error("Error while creating collection {}", collectionName, e);
                            }
                        });
    }
}
