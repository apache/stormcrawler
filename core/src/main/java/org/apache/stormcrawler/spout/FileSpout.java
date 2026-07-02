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

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
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
import java.util.Locale;
import java.util.Map;
import java.util.Queue;
import java.util.zip.GZIPInputStream;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
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
 * Reads the lines from a UTF-8 file and use them as a spout. The spout reads files in chunks of
 * 10,000 lines, keeping memory usage very low even for extremely large files with millions of seed
 * URLs. Uses StringTabScheme to parse the lines into URLs and Metadata, generates tuples on the
 * default stream unless withDiscoveredStatus is set to true.
 */
public class FileSpout extends BaseRichSpout {

    public static final int BATCH_SIZE = 10000;
    public static final Logger LOG = LoggerFactory.getLogger(FileSpout.class);
    private transient Queue<String> inputFiles;
    private final String seedDir;
    private final String fileFilter;
    private final String[] seedFiles;
    protected transient SpoutOutputCollector collector;
    protected Scheme scheme = new StringTabScheme();
    protected LinkedList<byte[]> buffer = new LinkedList<>();
    protected boolean active;
    protected transient int totalTasks;
    protected transient int taskIndex;
    private BufferedReader currentBuffer;
    private final boolean withDiscoveredStatus;

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
        this.seedDir = dir;
        this.fileFilter = filter;
        this.seedFiles = null;
    }

    /**
     * @param withDiscoveredStatus whether the tuples generated should contain a Status field with
     *     DISCOVERED as value and be emitted on the status stream
     * @param files containing the URLs
     * @since 1.13
     */
    public FileSpout(boolean withDiscoveredStatus, String... files) {
        if (files.length == 0) {
            throw new IllegalArgumentException("Must configure at least one inputFile");
        }
        this.withDiscoveredStatus = withDiscoveredStatus;
        this.seedDir = null;
        this.fileFilter = null;
        this.seedFiles = files;
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
            InputStream is = new BufferedInputStream(new FileInputStream(inputPath.toFile()));
            try {
                String fileLower = file.toLowerCase(Locale.ROOT);
                if (fileLower.endsWith(".gz") || fileLower.endsWith(".gzip")) {
                    is = new GZIPInputStream(is);
                } else if (fileLower.endsWith(".bz2")) {
                    is = new BZip2CompressorInputStream(is, true);
                }
                currentBuffer =
                        new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8));
            } catch (IOException e) {
                is.close();
                throw e;
            }
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

        // Resolve the seeds here, not in the constructor: in distributed mode the
        // spout is serialised on the submit client and only open() runs on the
        // workers, where the seed directory/files actually live (issue #1955).
        inputFiles = new LinkedList<>();
        populateInputFiles();
    }

    /**
     * Resolves the configured seed directory or file list into {@link #inputFiles}. Called from
     * {@link #open} so the filesystem is read on the worker, not on the client at construction
     * time.
     */
    private void populateInputFiles() {
        if (seedDir != null) {
            Path pdir = Paths.get(seedDir);
            LOG.info("Reading directory: {} (filter: {})", pdir, fileFilter);
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(pdir, fileFilter)) {
                for (Path entry : stream) {
                    String inputFile = entry.toAbsolutePath().toString();
                    inputFiles.add(inputFile);
                    LOG.info("Input : {}", inputFile);
                }
            } catch (IOException ioe) {
                LOG.error("IOException while reading seed directory {}", pdir, ioe);
            }
        } else {
            Collections.addAll(inputFiles, seedFiles);
        }
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
    public void close() {
        if (currentBuffer != null) {
            try {
                currentBuffer.close();
            } catch (IOException e) {
                LOG.error("Exception thrown when closing current buffer", e);
            }
            currentBuffer = null;
        }
    }

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
