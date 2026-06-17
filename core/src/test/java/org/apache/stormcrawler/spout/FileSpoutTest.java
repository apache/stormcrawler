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

package org.apache.stormcrawler.spout;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.persistence.Status;
import org.apache.stormcrawler.spout.mocks.FileSpoutOutputCollectorMock;
import org.apache.stormcrawler.spout.mocks.FileSpoutTopologyContextMock;
import org.junit.jupiter.api.Test;

class FileSpoutTest {

    @Test
    void testSeedFile() throws URISyntaxException {
        final Path path = getPath("seed-list-default.txt");
        assertNotNull(path);
        final FileSpout spout = new FileSpout(path.toAbsolutePath().toString());
        final FileSpoutOutputCollectorMock collectorMock = new FileSpoutOutputCollectorMock();
        // simulate Storm init
        spout.open(Map.of(), new FileSpoutTopologyContextMock(), collectorMock);
        spout.activate();
        // simulate Storm next tuple
        spout.nextTuple();
        // test
        final List<Object> tuple = collectorMock.getTuple();
        assertNotNull(tuple);
        assertEquals(2, tuple.size());
        assertEquals("https://stormcrawler.apache.org", tuple.get(0));
        final Metadata metadata = (Metadata) tuple.get(1);
        assertNotNull(metadata);
        assertTrue(metadata.keySet().isEmpty());
    }

    @Test
    void testSeedFileWithStatus() throws URISyntaxException {
        final Path path = getPath("seed-list-default.txt");
        assertNotNull(path);
        final FileSpout spout = new FileSpout(true, path.toAbsolutePath().toString());
        final FileSpoutOutputCollectorMock collectorMock = new FileSpoutOutputCollectorMock();
        // simulate Storm init
        spout.open(Map.of(), new FileSpoutTopologyContextMock(), collectorMock);
        spout.activate();
        // simulate Storm next tuple
        spout.nextTuple();
        // test
        final List<Object> tuple = collectorMock.getTuple();
        assertNotNull(tuple);
        assertEquals(3, tuple.size());
        assertEquals("https://stormcrawler.apache.org", tuple.get(0));
        final Metadata metadata = (Metadata) tuple.get(1);
        assertNotNull(metadata);
        assertTrue(metadata.keySet().isEmpty());
        assertEquals(Status.DISCOVERED, tuple.get(2));
    }

    @Test
    void testSeedFileWithCustomData() throws URISyntaxException {
        final Path path = getPath("seed-list-custom-metadata.txt");
        assertNotNull(path);
        final FileSpout spout = new FileSpout(true, path.toAbsolutePath().toString());
        final FileSpoutOutputCollectorMock collectorMock = new FileSpoutOutputCollectorMock();
        // simulate Storm init
        spout.open(Map.of(), new FileSpoutTopologyContextMock(), collectorMock);
        spout.activate();
        // simulate Storm next tuple
        spout.nextTuple();
        // test
        final List<Object> tuple = collectorMock.getTuple();
        assertNotNull(tuple);
        assertEquals(3, tuple.size());
        assertEquals("https://stormcrawler.apache.org", tuple.get(0));
        final Metadata metadata = (Metadata) tuple.get(1);
        assertNotNull(metadata);
        assertEquals(2, metadata.keySet().size());
        assertEquals("bla1", metadata.getFirstValue("custom1"));
        assertEquals("bla2", metadata.getFirstValue("custom2"));
        assertEquals(Status.DISCOVERED, tuple.get(2));
    }

    @Test
    void testGzipSeedFile(@org.junit.jupiter.api.io.TempDir Path tempDir) throws Exception {
        Path gzipFile = tempDir.resolve("seed-list.txt.gz");
        try (java.io.OutputStream os = Files.newOutputStream(gzipFile);
                java.util.zip.GZIPOutputStream gzos = new java.util.zip.GZIPOutputStream(os)) {
            gzos.write("https://stormcrawler.apache.org\n".getBytes(StandardCharsets.UTF_8));
            gzos.write("https://github.com\n".getBytes(StandardCharsets.UTF_8));
        }

        final FileSpout spout = new FileSpout(gzipFile.toAbsolutePath().toString());
        final FileSpoutOutputCollectorMock collectorMock = new FileSpoutOutputCollectorMock();
        spout.open(Map.of(), new FileSpoutTopologyContextMock(), collectorMock);
        spout.activate();

        // 1st tuple
        spout.nextTuple();
        List<Object> tuple = collectorMock.getTuple();
        assertNotNull(tuple);
        assertEquals("https://stormcrawler.apache.org", tuple.get(0));

        // 2nd tuple
        spout.nextTuple();
        tuple = collectorMock.getTuple();
        assertNotNull(tuple);
        assertEquals("https://github.com", tuple.get(0));
    }

    @Test
    void testBzip2SeedFile(@org.junit.jupiter.api.io.TempDir Path tempDir) throws Exception {
        Path bzip2File = tempDir.resolve("seed-list.txt.bz2");
        try (java.io.OutputStream os = Files.newOutputStream(bzip2File);
                org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream bzos =
                        new org.apache.commons.compress.compressors.bzip2
                                .BZip2CompressorOutputStream(os)) {
            bzos.write("https://stormcrawler.apache.org\n".getBytes(StandardCharsets.UTF_8));
            bzos.write("https://github.com\n".getBytes(StandardCharsets.UTF_8));
        }

        final FileSpout spout = new FileSpout(bzip2File.toAbsolutePath().toString());
        final FileSpoutOutputCollectorMock collectorMock = new FileSpoutOutputCollectorMock();
        spout.open(Map.of(), new FileSpoutTopologyContextMock(), collectorMock);
        spout.activate();

        // 1st tuple
        spout.nextTuple();
        List<Object> tuple = collectorMock.getTuple();
        assertNotNull(tuple);
        assertEquals("https://stormcrawler.apache.org", tuple.get(0));

        // 2nd tuple
        spout.nextTuple();
        tuple = collectorMock.getTuple();
        assertNotNull(tuple);
        assertEquals("https://github.com", tuple.get(0));
    }

    @Test
    void testDirectoryResolvedAtOpenNotConstruction(@org.junit.jupiter.api.io.TempDir Path tempDir)
            throws Exception {
        // Build the spout while the directory is still empty.
        final FileSpout spout = new FileSpout(tempDir.toAbsolutePath().toString(), "*.txt");

        // Create the seed file AFTER construction but BEFORE open().
        // On today's code the constructor already listed the (empty) directory,
        // so nothing is emitted. Once resolution moves to open(), the file is seen.
        Files.write(
                tempDir.resolve("seeds.txt"),
                "https://stormcrawler.apache.org\n".getBytes(StandardCharsets.UTF_8));

        final FileSpoutOutputCollectorMock collectorMock = new FileSpoutOutputCollectorMock();
        spout.open(Map.of(), new FileSpoutTopologyContextMock(), collectorMock);
        spout.activate();
        spout.nextTuple();

        final List<Object> tuple = collectorMock.getTuple();
        assertNotNull(tuple);
        assertEquals("https://stormcrawler.apache.org", tuple.get(0));
    }

    @Test
    void testDirectoryConstructorHappyPath(@org.junit.jupiter.api.io.TempDir Path tempDir)
            throws Exception {
        Files.write(
                tempDir.resolve("seeds.txt"),
                "https://stormcrawler.apache.org\n".getBytes(StandardCharsets.UTF_8));

        final FileSpout spout = new FileSpout(tempDir.toAbsolutePath().toString(), "*.txt");
        final FileSpoutOutputCollectorMock collectorMock = new FileSpoutOutputCollectorMock();
        spout.open(Map.of(), new FileSpoutTopologyContextMock(), collectorMock);
        spout.activate();
        spout.nextTuple();

        final List<Object> tuple = collectorMock.getTuple();
        assertNotNull(tuple);
        assertEquals("https://stormcrawler.apache.org", tuple.get(0));
    }

    @Test
    void testSurvivesSerialization(@org.junit.jupiter.api.io.TempDir Path tempDir)
            throws Exception {
        Files.write(
                tempDir.resolve("seeds.txt"),
                "https://stormcrawler.apache.org\n".getBytes(StandardCharsets.UTF_8));

        final FileSpout spout = new FileSpout(tempDir.toAbsolutePath().toString(), "*.txt");

        final java.io.ByteArrayOutputStream bos = new java.io.ByteArrayOutputStream();
        try (java.io.ObjectOutputStream oos = new java.io.ObjectOutputStream(bos)) {
            oos.writeObject(spout);
        }
        final FileSpout restored;
        try (java.io.ObjectInputStream ois =
                new java.io.ObjectInputStream(
                        new java.io.ByteArrayInputStream(bos.toByteArray()))) {
            restored = (FileSpout) ois.readObject();
        }

        final FileSpoutOutputCollectorMock collectorMock = new FileSpoutOutputCollectorMock();
        restored.open(Map.of(), new FileSpoutTopologyContextMock(), collectorMock);
        restored.activate();
        restored.nextTuple();

        final List<Object> tuple = collectorMock.getTuple();
        assertNotNull(tuple);
        assertEquals("https://stormcrawler.apache.org", tuple.get(0));
    }

    private Path getPath(String resource) throws URISyntaxException {
        return Path.of(
                Objects.requireNonNull(
                                Thread.currentThread()
                                        .getContextClassLoader()
                                        .getResource(resource))
                        .toURI());
    }
}
