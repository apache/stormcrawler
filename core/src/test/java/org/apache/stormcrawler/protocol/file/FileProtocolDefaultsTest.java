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

package org.apache.stormcrawler.protocol.file;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.storm.Config;
import org.apache.storm.utils.Utils;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.protocol.ProtocolResponse;
import org.apache.stormcrawler.util.ConfUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class FileProtocolDefaultsTest {

    /** The file scheme must not be part of the shipped default protocols list. */
    @Test
    void fileSchemeIsNotEnabledByDefault() {
        Config conf = new Config();
        Map<String, Object> defaults = Utils.findAndReadConfigFile("crawler-default.yaml", false);
        conf.putAll(ConfUtils.extractConfigElement(defaults));
        String protocols = ConfUtils.getString(conf, "protocols", "http,https");
        List<String> schemes = Arrays.asList(protocols.split(" *, *"));
        Assertions.assertFalse(
                schemes.contains("file"),
                "crawler-default.yaml enables the file scheme by default: " + protocols);
    }

    /**
     * Without a configured root, FileProtocol must refuse to serve anything.
     */
    @Test
    void fileProtocolServesNothingWithoutARoot(@TempDir Path tmp) throws Exception {
        Path inside = tmp.resolve("inside.txt");
        Files.write(inside, "content".getBytes(StandardCharsets.UTF_8));

        Config conf = new Config();
        FileProtocol protocol = new FileProtocol();
        protocol.configure(conf);

        String url = inside.toUri().toURL().toString();
        ProtocolResponse response = protocol.getProtocolOutput(url, new Metadata());
        Assertions.assertEquals(
                403, response.getStatusCode(), "FileProtocol served a file with no root set");
    }

    /**
     * With a root directory configured, FileProtocol must refuse to read a file outside that root.
     */
    @Test
    void fileProtocolConfinesReadsToConfiguredRoot(@TempDir Path tmp) throws Exception {
        Path base = tmp.toRealPath();
        Path root = base.resolve("root");
        Files.createDirectories(root);
        Path inside = root.resolve("inside.txt");
        Files.write(inside, "for the crawler".getBytes(StandardCharsets.UTF_8));
        Path outside = base.resolve("outside.txt");
        Files.write(outside, "not for the crawler".getBytes(StandardCharsets.UTF_8));

        Config conf = new Config();
        conf.put(FileProtocol.ROOT_KEY, root.toString());

        FileProtocol protocol = new FileProtocol();
        protocol.configure(conf);

        String insideUrl = inside.toUri().toURL().toString();
        ProtocolResponse response = protocol.getProtocolOutput(insideUrl, new Metadata());
        Assertions.assertEquals(
                200, response.getStatusCode(), "FileProtocol refused a file inside the root");

        String outsideUrl = outside.toUri().toURL().toString();
        response = protocol.getProtocolOutput(outsideUrl, new Metadata());
        Assertions.assertNotEquals(
                200,
                response.getStatusCode(),
                "FileProtocol read a file outside the configured root: " + outsideUrl);
    }

    /** A symlink or parent-directory escape must not leave the root either. */
    @Test
    void fileProtocolConfinesCanonicalisedPaths(@TempDir Path tmp) throws Exception {
        Path base = tmp.toRealPath();
        Path root = base.resolve("root");
        Files.createDirectories(root);
        Path secret = base.resolve("secret.txt");
        Files.write(secret, "not for the crawler".getBytes(StandardCharsets.UTF_8));

        File link = root.resolve("link.txt").toFile();
        try {
            Files.createSymbolicLink(
                    link.toPath().toAbsolutePath(), secret.toAbsolutePath());
        } catch (UnsupportedOperationException | java.io.IOException e) {
            // filesystem without symlink support; nothing to test here
            return;
        }

        Config conf = new Config();
        conf.put(FileProtocol.ROOT_KEY, root.toString());
        FileProtocol protocol = new FileProtocol();
        protocol.configure(conf);

        String url = link.toURI().toURL().toString();
        ProtocolResponse response = protocol.getProtocolOutput(url, new Metadata());
        Assertions.assertNotEquals(
                200,
                response.getStatusCode(),
                "FileProtocol followed a symlink out of the configured root: " + url);
    }
}
