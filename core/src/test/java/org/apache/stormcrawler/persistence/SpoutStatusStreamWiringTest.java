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

package org.apache.stormcrawler.persistence;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import org.apache.stormcrawler.Constants;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.yaml.snakeyaml.Yaml;

/**
 * The shipped archetype topologies must connect the spout's status stream to the status updater:
 * rows the spout refuses to emit (a scheme not in the protocols list) are reported as ERROR so that
 * the status updater removes them from the store. Declaring the stream in the spout alone routes
 * the tuples nowhere without this wiring.
 */
class SpoutStatusStreamWiringTest {

    /** Archetype crawler.flux files, relative to the repository root. */
    private static final String FLUX_PATH = "src/main/resources/archetype-resources/crawler.flux";

    /** Resolves a path relative to the repository root from the module the test runs in. */
    private static Path fromRepositoryRoot(String relative) {
        Path fromModule = Paths.get("..", relative).toAbsolutePath().normalize();
        if (Files.exists(fromModule)) {
            return fromModule;
        }
        return Paths.get(relative).toAbsolutePath().normalize();
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> load(Path flux) {
        try (InputStream in = Files.newInputStream(flux)) {
            return new Yaml().load(in);
        } catch (IOException e) {
            throw new IllegalStateException("Could not load " + flux, e);
        }
    }

    @SuppressWarnings("unchecked")
    private static boolean hasStream(
            Map<String, Object> flux, String from, String to, String streamId) {
        List<Map<String, Object>> streams = (List<Map<String, Object>>) flux.get("streams");
        for (Map<String, Object> s : streams) {
            if (from.equals(s.get("from"))
                    && to.equals(s.get("to"))
                    && streamId.equals(((Map<String, Object>) s.get("grouping")).get("streamId"))) {
                return true;
            }
        }
        return false;
    }

    @ParameterizedTest
    @ValueSource(
            strings = {"archetype", "external/opensearch/archetype", "external/solr/archetype"})
    void spoutStatusStreamIsConnectedToTheStatusUpdater(String archetype) {
        Path flux = fromRepositoryRoot(archetype + "/" + FLUX_PATH);
        Assertions.assertTrue(
                hasStream(load(flux), "spout", "status", Constants.StatusStreamName),
                flux
                        + " must connect the spout's status stream to the status updater bolt,"
                        + " otherwise rows the spout refuses to emit stay in the store");
    }
}
