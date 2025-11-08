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
package org.apache.stormcrawler.warc;

import java.io.IOException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.storm.metric.api.MultiCountMetric;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.stormcrawler.Constants;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.persistence.Status;
import org.apache.stormcrawler.protocol.ProtocolResponse;
import org.apache.stormcrawler.spout.FileSpout;
import org.apache.stormcrawler.util.ConfUtils;
import org.netpreserve.jwarc.HttpMessage;
import org.netpreserve.jwarc.HttpRequest;
import org.netpreserve.jwarc.HttpResponse;
import org.netpreserve.jwarc.IOUtils;
import org.netpreserve.jwarc.MediaType;
import org.netpreserve.jwarc.ParsingException;
import org.netpreserve.jwarc.WarcPayload;
import org.netpreserve.jwarc.WarcReader;
import org.netpreserve.jwarc.WarcRecord;
import org.netpreserve.jwarc.WarcRequest;
import org.netpreserve.jwarc.WarcResponse;
import org.netpreserve.jwarc.WarcTruncationReason;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Read WARC files from the local files system and emit the WARC captures as tuples into the
 * topology same way as done by {@link org.apache.stormcrawler.bolt.FetcherBolt}.
 */
public class WARCSpout extends FileSpout {

    private static final Logger LOG = LoggerFactory.getLogger(WARCSpout.class);

    private int maxContentSize = -1;
    private int contentBufferSize = 8192;

    private boolean storeHTTPHeaders = false;
    private String protocolMDprefix = "";

    private WarcReader warcReader;
    private String warcFileInProgress;
    private WarcRequest precedingWarcRequest;
    private Optional<WarcRecord> record;

    private MultiCountMetric eventCounter;

    protected transient Configuration hdfsConfig;

    public WARCSpout(String... files) {
        super(false, files);
    }

    public WARCSpout(String dir, String filter) {
        super(dir, filter, false);
    }

    /**
     * Holder of truncation status when WARC payload exceeding the content length limit
     * (http.content.limit) is truncated.
     */
    public static class TruncationStatus {
        boolean isTruncated = false;
        long originalSize = -1;

        public void set(boolean isTruncated) {
            this.isTruncated = isTruncated;
        }

        public boolean get() {
            return isTruncated;
        }

        public void setOriginalSize(long size) {
            originalSize = size;
        }

        public long getOriginalSize() {
            return originalSize;
        }
    }

    private void openWARC() {
        if (warcReader != null) {
            try {
                warcReader.close();
            } catch (IOException e) {
                LOG.warn("Failed to close open WARC file", e);
            }
            warcReader = null;
        }

        byte[] head = buffer.removeFirst();
        List<Object> fields = scheme.deserialize(ByteBuffer.wrap(head));
        warcFileInProgress = (String) fields.get(0);
        if (warcFileInProgress == null) return;

        LOG.info("Reading WARC file {}", warcFileInProgress);
        ReadableByteChannel warcChannel = null;
        try {
            warcChannel = openChannel(warcFileInProgress);
            warcReader = new WarcReader(warcChannel);
        } catch (IOException e) {
            LOG.error("Failed to open WARC file " + warcFileInProgress, e);
            warcFileInProgress = null;
            if (warcChannel != null) {
                try {
                    warcChannel.close();
                } catch (IOException ex) {
                }
            }
        }
    }

    private ReadableByteChannel openChannel(String path) throws IOException {
        if (path.matches("^https?://.*")) {
            URL warcUrl = new URL(path);
            return Channels.newChannel(warcUrl.openStream());
        }
        org.apache.hadoop.fs.Path hdfsPath = new org.apache.hadoop.fs.Path(path);
        return Channels.newChannel(hdfsPath.getFileSystem(hdfsConfig).open(hdfsPath));
    }

    private void closeWARC() {
        LOG.info("Finished reading WARC file {}", warcFileInProgress);
        try {
            warcReader.close();
        } catch (IOException e) {
            LOG.warn("Failed to close WARC reader", e);
        }
        warcReader = null;
    }

    /**
     * Proceed to next WARC record, calculate record length of current record and add the length to
     * metadata
     */
    private void nextRecord(long offset, Metadata metadata) {
        long nextOffset = nextRecord();
        if (nextOffset > offset) {
            metadata.addValue("warc.record.length", Long.toString(nextOffset - offset));
        } else {
            LOG.error(
                    "Implausible offset of next WARC record: {} - current offset: {}",
                    nextOffset,
                    offset);
        }
    }

    /**
     * Proceed to next WARC record.
     *
     * @return offset of next record in WARC file
     */
    private long nextRecord() {
        long nextOffset;
        while (warcReader == null && !buffer.isEmpty()) {
            openWARC();
        }
        if (warcReader == null) {
            // failed to open any new WARC file
            record = Optional.empty();
            return -1;
        }
        try {
            record = warcReader.next();
            nextOffset = warcReader.position();
            if (!record.isPresent()) {
                closeWARC();
            }
        } catch (IOException e) {
            LOG.error(
                    "Failed to read WARC {} at position {}:",
                    warcFileInProgress,
                    warcReader.position(),
                    e);
            nextOffset = warcReader.position();
            record = Optional.empty();
            closeWARC();
        }
        return nextOffset;
    }

    private boolean isHttpResponse(Optional<WarcRecord> record) {
        if (!record.isPresent()) return false;
        if (!(record.get() instanceof WarcResponse)) return false;
        if (record.get().contentType().equals(MediaType.HTTP_RESPONSE)) return true;
        return false;
    }

    private byte[] getContent(WarcResponse record, TruncationStatus isTruncated)
            throws IOException {
        Optional<WarcPayload> payload = record.payload();
        if (!payload.isPresent()) {
            return new byte[0];
        }
        long size = payload.get().body().size();
        ReadableByteChannel body = payload.get().body();

        // Check HTTP Content-Encoding header whether payload needs decoding
        List<String> contentEncodings = record.http().headers().all("Content-Encoding");
        try {
            if (contentEncodings.size() > 1) {
                LOG.error("Multiple Content-Encodings not supported: {}", contentEncodings);
                LOG.warn("Trying to read payload of {} without Content-Encoding", record.target());
            } else if (contentEncodings.isEmpty()
                    || contentEncodings.get(0).equalsIgnoreCase("identity")
                    || contentEncodings.get(0).equalsIgnoreCase("none")) {
                // no need for decoding
            } else if (contentEncodings.get(0).equalsIgnoreCase("gzip")
                    || contentEncodings.get(0).equalsIgnoreCase("x-gzip")) {
                LOG.debug(
                        "Decoding payload of {} from Content-Encoding {}",
                        record.target(),
                        contentEncodings.get(0));
                body = IOUtils.gunzipChannel(body);
                body.read(ByteBuffer.allocate(0));
                size = -1;
            } else if (contentEncodings.get(0).equalsIgnoreCase("deflate")) {
                LOG.debug(
                        "Decoding payload of {} from Content-Encoding {}",
                        record.target(),
                        contentEncodings.get(0));
                body = IOUtils.inflateChannel(body);
                body.read(ByteBuffer.allocate(0));
                size = -1;
            } else {
                LOG.error("Content-Encoding not supported: {}", contentEncodings.get(0));
                LOG.warn("Trying to read payload of {} without Content-Encoding", record.target());
            }
        } catch (IOException e) {
            LOG.error(
                    "Failed to read payload with Content-Encoding {}: {}",
                    contentEncodings.get(0),
                    e.getMessage());
            LOG.warn("Trying to read payload of {} without Content-Encoding", record.target());
            body = payload.get().body();
        }

        isTruncated.set(false);
        if (size > maxContentSize) {
            LOG.info(
                    "WARC payload of size {} to be truncated to {} bytes for {}",
                    size,
                    maxContentSize,
                    record.target());
            size = maxContentSize;
        }
        ByteBuffer buf;
        if (size >= 0) {
            buf = ByteBuffer.allocate((int) size);
        } else {
            buf = ByteBuffer.allocate(contentBufferSize);
        }
        // dynamically growing list of buffers for large content of unknown size
        ArrayList<ByteBuffer> bufs = new ArrayList<>();
        int r, read = 0;
        while (read < maxContentSize) {
            try {
                if ((r = body.read(buf)) < 0) break; // eof
            } catch (ParsingException e) {
                LOG.error("Failed to read chunked content of {}: {}", record.target(), e);
                /*
                 * caused by an invalid Transfer-Encoding or a HTTP header `Transfer-Encoding:
                 * chunked` removed although the Transfer-Encoding was removed in the WARC file
                 */
                // TODO: should retry without chunked Transfer-Encoding
                break;
            } catch (IOException e) {
                LOG.error("Failed to read content of {}: {}", record.target(), e);
                break;
            }
            if (r == 0 && !buf.hasRemaining()) {
                buf.flip();
                bufs.add(buf);
                buf = ByteBuffer.allocate(Math.min(contentBufferSize, (maxContentSize - read)));
            }
            read += r;
        }
        buf.flip();
        if (read == maxContentSize) {
            // to mark truncation: check whether there is more content
            r = body.read(ByteBuffer.allocate(1));
            if (r > -1) {
                isTruncated.set(true);
                // read remaining body (also to figure out original length)
                long truncatedLength = r;
                ByteBuffer buffer = ByteBuffer.allocate(8192);
                try {
                    while ((r = body.read(buffer)) >= 0) {
                        buffer.clear();
                        truncatedLength += r;
                    }
                } catch (IOException e) {
                    // log and ignore, it's about unused content
                    LOG.info("Exception while determining length of truncation:", e);
                }
                isTruncated.setOriginalSize(read + truncatedLength);
                LOG.info(
                        "WARC payload of size {} is truncated to {} bytes for {}",
                        isTruncated.getOriginalSize(),
                        maxContentSize,
                        record.target());
            }
        }
        if (read == size) {
            // short-cut: return buffer-internal array
            return buf.array();
        }
        // copy buffers into result byte[]
        byte[] arr = new byte[read];
        int pos = 0;
        for (ByteBuffer b : bufs) {
            r = b.remaining();
            b.get(arr, pos, r);
            pos += r;
        }
        buf.get(arr, pos, buf.remaining());
        return arr;
    }

    private static String httpHeadersVerbatim(HttpMessage http) {
        return new String(http.serializeHeader(), StandardCharsets.UTF_8);
    }

    private void addVerbatimHttpHeaders(
            Metadata metadata, WarcResponse response, HttpResponse http, HttpRequest request) {
        metadata.addValue(
                protocolMDprefix + ProtocolResponse.REQUEST_TIME_KEY,
                Long.toString(response.date().toEpochMilli()));
        if (response.ipAddress().isPresent()) {
            metadata.addValue(
                    protocolMDprefix + ProtocolResponse.RESPONSE_IP_KEY,
                    response.ipAddress().get().getHostAddress());
        }
        if (request != null) {
            metadata.addValue(
                    protocolMDprefix + ProtocolResponse.REQUEST_HEADERS_KEY,
                    httpHeadersVerbatim(request));
        }
        metadata.addValue(
                protocolMDprefix + ProtocolResponse.RESPONSE_HEADERS_KEY,
                httpHeadersVerbatim(http));
    }

    @Override
    public void open(
            Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {

        super.open(conf, context, collector);

        record = Optional.empty();

        maxContentSize = ConfUtils.getInt(conf, "http.content.limit", -1);
        if (maxContentSize == -1 || maxContentSize > Constants.MAX_ARRAY_SIZE) {
            // maximum possible payload length, must fit into an array
            maxContentSize = Constants.MAX_ARRAY_SIZE;
        }
        if (contentBufferSize > maxContentSize) {
            // no need to buffer more content than max. used
            contentBufferSize = maxContentSize;
        }
        storeHTTPHeaders = ConfUtils.getBoolean(conf, "http.store.headers", false);
        protocolMDprefix =
                ConfUtils.getString(
                        conf, ProtocolResponse.PROTOCOL_MD_PREFIX_PARAM, protocolMDprefix);

        int metricsTimeBucketSecs = ConfUtils.getInt(conf, "fetcher.metrics.time.bucket.secs", 10);
        eventCounter =
                context.registerMetric(
                        "warc_spout_counter", new MultiCountMetric(), metricsTimeBucketSecs);

        hdfsConfig = new Configuration();

        String configKey = ConfUtils.getString(conf, "hdfs.config.key", "hdfs");

        Map<String, Object> map = (Map<String, Object>) conf.get(configKey);
        if (map != null) {
            for (String key : map.keySet()) {
                this.hdfsConfig.set(key, String.valueOf(map.get(key)));
            }
        }
    }

    @Override
    public void nextTuple() {
        if (!active) return;

        if (buffer.isEmpty()) {
            try {
                populateBuffer();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        if (warcReader == null && buffer.isEmpty()) {
            // input exhausted
            return;
        }

        if (!record.isPresent()) nextRecord();

        while (record.isPresent() && !isHttpResponse(record)) {
            String warcType = record.get().type();
            if (warcType == null) {
                LOG.warn("No type for {}", record.get().getClass());
            } else {
                eventCounter.scope("warc_skipped_record_of_type_" + warcType).incr();
                LOG.debug("Skipped WARC record of type {}", warcType);
            }
            if (storeHTTPHeaders && record.get() instanceof WarcRequest) {
                // store request records to be able to add HTTP request
                // header to metadata
                precedingWarcRequest = (WarcRequest) record.get();
                try {
                    // need to read and parse HTTP header right now
                    // (otherwise it's skipped)
                    precedingWarcRequest.http();
                } catch (IOException e) {
                    LOG.error(
                            "Failed to read HTTP request for {} in {}: {}",
                            precedingWarcRequest.target(),
                            warcFileInProgress,
                            e);
                    precedingWarcRequest = null;
                }
            }
            nextRecord();
        }

        if (!record.isPresent()) return;

        eventCounter.scope("warc_http_response_record").incr();
        WarcResponse w = (WarcResponse) record.get();

        String url = w.target();
        HttpResponse http;
        try {
            http = w.http();
        } catch (IOException e) {
            LOG.error("Failed to read HTTP response for {} in {}: {}", url, warcFileInProgress, e);
            nextRecord();
            return;
        }
        LOG.info("Fetched {} with status {}", url, http.status());
        eventCounter.scope("fetched").incrBy(1);

        final Status status = Status.fromHTTPCode(http.status());
        eventCounter.scope("status_" + http.status()).incrBy(1);

        Metadata metadata = new Metadata();

        // add HTTP status code expected by schedulers
        metadata.addValue("fetch.statusCode", Integer.toString(http.status()));

        // add time when page was fetched (capture time)
        Instant captureTime;
        try {
            captureTime = w.date();
        } catch (DateTimeParseException e) {
            LOG.error("Failed to parse capture time for {} in {}: {}", url, warcFileInProgress, e);
            nextRecord();
            return;
        }
        metadata.addValue(
                protocolMDprefix + ProtocolResponse.REQUEST_TIME_KEY,
                Long.toString(captureTime.toEpochMilli()));

        // Add HTTP response headers to metadata
        for (Map.Entry<String, List<String>> e : http.headers().map().entrySet()) {
            metadata.addValues(protocolMDprefix + e.getKey(), e.getValue());
        }

        if (storeHTTPHeaders) {
            // if recording HTTP headers: add IP address, fetch date time,
            // literal request and response headers
            HttpRequest req = null;
            if (precedingWarcRequest != null
                    && (w.concurrentTo().contains(precedingWarcRequest.id())
                            || w.target().equals(precedingWarcRequest.target()))) {
                try {
                    req = precedingWarcRequest.http();
                } catch (IOException e) {
                    // ignore, no HTTP request headers are no issue
                }
            }
            addVerbatimHttpHeaders(metadata, w, http, req);
        }

        // add WARC record information
        metadata.addValue("warc.file.name", warcFileInProgress);
        long offset = warcReader.position();
        metadata.addValue("warc.record.offset", Long.toString(offset));
        /*
         * note: warc.record.length must be calculated after WARC record has been
         * entirely processed
         */

        if (status == Status.FETCHED && http.status() != 304) {
            byte[] content;
            TruncationStatus isTruncated = new TruncationStatus();
            try {
                content = getContent(w, isTruncated);
            } catch (IOException e) {
                LOG.error("Failed to read payload for {} in {}: {}", url, warcFileInProgress, e);
                content = new byte[0];
            }
            eventCounter.scope("bytes_fetched").incrBy(content.length);

            if (isTruncated.get() || w.truncated() != WarcTruncationReason.NOT_TRUNCATED) {
                WarcTruncationReason reason = WarcTruncationReason.LENGTH;
                if (w.truncated() != WarcTruncationReason.NOT_TRUNCATED) {
                    reason = w.truncated();
                }
                metadata.setValue(protocolMDprefix + ProtocolResponse.TRIMMED_RESPONSE_KEY, "true");
                metadata.setValue(
                        protocolMDprefix + ProtocolResponse.TRIMMED_RESPONSE_REASON_KEY,
                        reason.toString().toLowerCase(Locale.ROOT));
            }

            nextRecord(offset, metadata); // proceed and calculate length

            collector.emit(new Values(url, content, metadata), url);

            return;
        }

        nextRecord(offset, metadata); // proceed and calculate length

        // redirects, 404s, etc.
        collector.emit(Constants.StatusStreamName, new Values(url, metadata, status), url);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(Constants.StatusStreamName, new Fields("url", "metadata", "status"));
        declarer.declare(new Fields("url", "content", "metadata"));
    }

    @Override
    public void fail(Object msgId) {
        LOG.error("Failed - unable to replay WARC record of: {}", msgId);
    }
}
