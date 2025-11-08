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

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import org.apache.commons.lang3.StringUtils;
import org.apache.storm.spout.Scheme;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.stormcrawler.Constants;
import org.apache.stormcrawler.persistence.Status;
import org.apache.stormcrawler.util.StringTabScheme;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reads the lines from a UTF-8 file and use them as a spout. Load the entire content into memory.
 * Uses StringTabScheme to parse the lines into URLs and Metadata, generates tuples on the default
 * stream unless withDiscoveredStatus is set to true.
 */
public class FileSpout extends BaseRichSpout {

    public static final int BATCH_SIZE = 10000;
    public static final Logger LOG = LoggerFactory.getLogger(FileSpout.class);

    protected SpoutOutputCollector collector;

    private final Queue<String> inputFiles;
    private BufferedReader currentBuffer;

    protected Scheme scheme = new StringTabScheme();

    protected LinkedList<byte[]> buffer = new LinkedList<>();
    protected boolean active;
    private boolean withDiscoveredStatus = false;
    protected int totalTasks;
    protected int taskIndex;

    /**
     * @param dir containing the seed files
     * @param filter to apply on the file names
     */
    public FileSpout(String dir, String filter) {
        this(dir, filter, false);
    }

    /**
     * @param files containing the URLs
     */
    public FileSpout(String... files) {
        this(false, files);
    }

    /**
     * @param withDiscoveredStatus whether the tuples generated should contain a Status field with
     *     DISCOVERED as value and be emitted on the status stream
     * @param dir containing the seed files
     * @param filter to apply on the file names
     * @since 1.13
     */
    public FileSpout(String dir, String filter, boolean withDiscoveredStatus) {
        this.withDiscoveredStatus = withDiscoveredStatus;
        Path pdir = Paths.get(dir);
        inputFiles = new LinkedList<>();
        LOG.info("Reading directory: {} (filter: {})", pdir, filter);
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(pdir, filter)) {
            for (Path entry : stream) {
                String inputFile = entry.toAbsolutePath().toString();
                inputFiles.add(inputFile);
                LOG.info("Input : {}", inputFile);
            }
        } catch (IOException ioe) {
            LOG.error("IOException: %s%n", ioe);
        }
    }

    /**
     * @param withDiscoveredStatus whether the tuples generated should contain a Status field with
     *     DISCOVERED as value and be emitted on the status stream
     * @param files containing the URLs
     * @since 1.13
     */
    public FileSpout(boolean withDiscoveredStatus, String... files) {
        this.withDiscoveredStatus = withDiscoveredStatus;
        if (files.length == 0) {
            throw new IllegalArgumentException("Must configure at least one inputFile");
        }
        inputFiles = new LinkedList<>();
        Collections.addAll(inputFiles, files);
    }

    /**
     * Specify a Scheme for parsing the lines into URLs and Metadata. StringTabScheme is used by
     * default. The Scheme must generate a String for the URL and a Metadata object.
     *
     * @since 1.13
     */
    public void setScheme(Scheme scheme) {
        this.scheme = scheme;
    }

    protected void populateBuffer() throws IOException {
        if (currentBuffer == null) {
            String file = inputFiles.poll();
            if (file == null) {
                return;
            }
            Path inputPath = Paths.get(file);
            currentBuffer =
                    new BufferedReader(
                            new InputStreamReader(
                                    new FileInputStream(inputPath.toFile()),
                                    StandardCharsets.UTF_8));
        }

        String line = null;
        int linesRead = 0;
        while (linesRead < BATCH_SIZE && (line = currentBuffer.readLine()) != null) {
            if (StringUtils.isBlank(line)) {
                continue;
            }
            if (line.startsWith("#")) {
                continue;
            }
            // check whether this entry should be skipped?
            // totalTasks could be at 0 if a subclass forgot to
            // call this classes open()
            if (totalTasks == 0 || linesRead % totalTasks == taskIndex) {
                LOG.debug(
                        "Adding to buffer for spout {} -> line ({}) {}",
                        taskIndex,
                        linesRead,
                        line);
                buffer.add(line.trim().getBytes(StandardCharsets.UTF_8));
            }
            linesRead++;
        }

        // finished the file?
        if (line == null) {
            currentBuffer.close();
            currentBuffer = null;
        }
    }

    @Override
    public void open(
            Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;

        // if more than one instance is used we expect their number to be the
        // same as the number of shards
        totalTasks = context.getComponentTasks(context.getThisComponentId()).size();
        taskIndex = context.getThisTaskIndex();
    }

    @Override
    public void nextTuple() {
        if (!active) {
            return;
        }

        if (buffer.isEmpty()) {
            try {
                populateBuffer();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        // still empty?
        if (buffer.isEmpty()) {
            return;
        }

        byte[] head = buffer.removeFirst();
        List<Object> fields = this.scheme.deserialize(ByteBuffer.wrap(head));

        if (withDiscoveredStatus) {
            fields.add(Status.DISCOVERED);
            this.collector.emit(Constants.StatusStreamName, fields, head);
        } else {
            this.collector.emit(fields, head);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(scheme.getOutputFields());
        if (withDiscoveredStatus) {
            // add status field to output
            List<String> s = scheme.getOutputFields().toList();
            s.add("status");
            declarer.declareStream(Constants.StatusStreamName, new Fields(s));
        }
    }

    @Override
    public void close() {}

    @Override
    public void activate() {
        super.activate();
        active = true;
    }

    @Override
    public void deactivate() {
        super.deactivate();
        active = false;
    }

    @Override
    public void ack(Object msgId) {}

    @Override
    public void fail(Object msgId) {
        if (msgId instanceof byte[]) {
            String msg = new String((byte[]) msgId, StandardCharsets.UTF_8);
            LOG.error("Failed - adding back to the queue: {}", msg);
            buffer.add((byte[]) msgId);
        } else {
            // unknown object type from extending class
            LOG.error(
                    "Failed - unknown message ID type `{}': {}",
                    msgId.getClass().getCanonicalName(),
                    msgId);
            throw new IllegalStateException(
                    "Unknown message ID type: " + msgId.getClass().getCanonicalName());
        }
    }
}
