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

package org.apache.stormcrawler.aws.bolt;

import static org.apache.stormcrawler.Constants.StatusStreamName;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.storm.Config;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.TupleUtils;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.indexing.AbstractIndexerBolt;
import org.apache.stormcrawler.metrics.CrawlerMetrics;
import org.apache.stormcrawler.metrics.ScopedCounter;
import org.apache.stormcrawler.persistence.Status;
import org.apache.stormcrawler.util.ConfUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cloudsearch.CloudSearchClient;
import software.amazon.awssdk.services.cloudsearch.model.DescribeDomainsRequest;
import software.amazon.awssdk.services.cloudsearch.model.DescribeDomainsResponse;
import software.amazon.awssdk.services.cloudsearch.model.DescribeIndexFieldsRequest;
import software.amazon.awssdk.services.cloudsearch.model.DescribeIndexFieldsResponse;
import software.amazon.awssdk.services.cloudsearch.model.DomainStatus;
import software.amazon.awssdk.services.cloudsearch.model.IndexFieldStatus;
import software.amazon.awssdk.services.cloudsearchdomain.CloudSearchDomainClient;
import software.amazon.awssdk.services.cloudsearchdomain.model.ContentType;
import software.amazon.awssdk.services.cloudsearchdomain.model.DocumentServiceWarning;
import software.amazon.awssdk.services.cloudsearchdomain.model.UploadDocumentsRequest;
import software.amazon.awssdk.services.cloudsearchdomain.model.UploadDocumentsResponse;

/** Writes documents to CloudSearch. */
public class CloudSearchIndexerBolt extends AbstractIndexerBolt {

    public static final Logger LOG = LoggerFactory.getLogger(CloudSearchIndexerBolt.class);

    private static final int MAX_SIZE_BATCH_BYTES = 5242880;
    private static final int MAX_SIZE_DOC_BYTES = 1048576;

    private static final SimpleDateFormat DATE_FORMAT =
            new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", Locale.ROOT);

    private CloudSearchDomainClient client;

    private int maxDocsInBatch = -1;

    private StringBuffer buffer;

    private int numDocsInBatch = 0;

    /** Max amount of time wait before indexing * */
    private int maxTimeBuffered = 10;

    private boolean dumpBatchFilesToTemp = false;

    private OutputCollector _collector;

    private ScopedCounter eventCounter;

    private Map<String, String> csfields = new HashMap<>();

    private long timeLastBatchSent = System.currentTimeMillis();

    private List<Tuple> unacked = new ArrayList<>();

    @Override
    public void prepare(
            Map<String, Object> conf, TopologyContext context, OutputCollector collector) {
        super.prepare(conf, context, collector);
        _collector = collector;

        this.eventCounter = CrawlerMetrics.registerCounter(context, conf, "CloudSearchIndexer", 10);

        maxTimeBuffered = ConfUtils.getInt(conf, CloudSearchConstants.MAX_TIME_BUFFERED, 10);

        maxDocsInBatch = ConfUtils.getInt(conf, CloudSearchConstants.MAX_DOCS_BATCH, -1);

        buffer = new StringBuffer(MAX_SIZE_BATCH_BYTES).append('[');

        dumpBatchFilesToTemp = ConfUtils.getBoolean(conf, "cloudsearch.batch.dump", false);

        if (dumpBatchFilesToTemp) {
            // only dumping to local file
            // no more config required
            return;
        }

        String endpoint = ConfUtils.getString(conf, "cloudsearch.endpoint");

        if (StringUtils.isBlank(endpoint)) {
            String message = "Missing CloudSearch endpoint";
            LOG.error(message);
            throw new RuntimeException(message);
        }

        String regionName = ConfUtils.getString(conf, CloudSearchConstants.REGION);

        String domainName = null;

        var clBuilder = CloudSearchClient.builder();
        if (StringUtils.isNotBlank(regionName)) {
            clBuilder.region(Region.of(regionName));
        }
        try (CloudSearchClient cl = clBuilder.build()) {
            // retrieve the domain name
            DescribeDomainsResponse domains =
                    cl.describeDomains(DescribeDomainsRequest.builder().build());

            for (DomainStatus ds : domains.domainStatusList()) {
                if (ds.docService().endpoint().equals(endpoint)) {
                    domainName = ds.domainName();
                    break;
                }
            }
            // check domain name
            if (StringUtils.isBlank(domainName)) {
                throw new RuntimeException("No domain name found for CloudSearch endpoint");
            }

            DescribeIndexFieldsResponse indexDescription =
                    cl.describeIndexFields(
                            DescribeIndexFieldsRequest.builder().domainName(domainName).build());
            for (IndexFieldStatus ifs : indexDescription.indexFields()) {
                String indexname = ifs.options().indexFieldName();
                String indextype = ifs.options().indexFieldTypeAsString();
                LOG.info("CloudSearch index name {} of type {}", indexname, indextype);
                csfields.put(indexname, indextype);
            }
        }

        var builder = CloudSearchDomainClient.builder().endpointOverride(URI.create(endpoint));
        if (StringUtils.isNotBlank(regionName)) {
            builder.region(Region.of(regionName));
        }
        client = builder.build();
    }

    @Override
    protected String getDocumentID(Metadata metadata, String normalisedUrl) {
        return CloudSearchUtils.getID(normalisedUrl);
    }

    @Override
    public void execute(Tuple tuple) {

        if (TupleUtils.isTick(tuple)) {
            // check when we last sent a batch
            long now = System.currentTimeMillis();
            long gap = now - timeLastBatchSent;
            if (gap >= maxTimeBuffered * 1000) {
                sendBatch();
            }
            _collector.ack(tuple);
            return;
        }

        String url = tuple.getStringByField("url");
        // Distinguish the value used for indexing
        // from the one used for the status
        String normalisedurl = valueForURL(tuple);

        Metadata metadata = (Metadata) tuple.getValueByField("metadata");
        String text = tuple.getStringByField("text");

        boolean keep = filterDocument(metadata);
        if (!keep) {
            eventCounter.scope("Filtered").incrBy(1);
            // treat it as successfully processed even if
            // we do not index it
            _collector.emit(StatusStreamName, tuple, new Values(url, metadata, Status.FETCHED));
            _collector.ack(tuple);
            return;
        }

        try {
            ObjectMapper objectMapper = new ObjectMapper();
            ObjectNode doc_builder = objectMapper.createObjectNode();

            doc_builder.put("type", "add");

            // generate the id from the normalised url
            String ID = getDocumentID(metadata, normalisedurl);
            doc_builder.put("id", ID);

            ObjectNode fields = objectMapper.createObjectNode();

            // which metadata to include as fields
            Map<String, String[]> keyVals = filterMetadata(metadata);

            for (final Entry<String, String[]> e : keyVals.entrySet()) {
                String fieldname = CloudSearchUtils.cleanFieldName(e.getKey());
                String type = csfields.get(fieldname);

                // undefined in index
                if (type == null && !this.dumpBatchFilesToTemp) {
                    LOG.info(
                            "Field {} not defined in CloudSearch domain for {} - skipping.",
                            fieldname,
                            url);
                    continue;
                }

                String[] values = e.getValue();

                // check that there aren't multiple values if not defined so in
                // the index
                if (values.length > 1 && !StringUtils.containsIgnoreCase(type, "-array")) {
                    LOG.info(
                            "{} values found for field {} of type {} - keeping only the first one. {}",
                            values.length,
                            fieldname,
                            type,
                            url);
                    values = new String[] {values[0]};
                }

                ArrayNode arrayNode = doc_builder.arrayNode();

                // write the values
                for (String value : values) {
                    // Check that the date format is correct
                    if (StringUtils.containsIgnoreCase(type, "date")) {
                        try {
                            DATE_FORMAT.parse(value);
                        } catch (ParseException pe) {
                            LOG.info("Unparsable date {}", value);
                            continue;
                        }
                    } else {
                        // normalise strings
                        value = CloudSearchUtils.stripNonCharCodepoints(value);
                    }

                    arrayNode.add(value);
                }

                if (arrayNode.size() > 0) {
                    doc_builder.set(fieldname, arrayNode);
                }
            }

            // include the url ?
            String fieldNameForURL = fieldNameForURL();
            if (StringUtils.isNotBlank(fieldNameForURL)) {
                fieldNameForURL = CloudSearchUtils.cleanFieldName(fieldNameForURL);
                if (this.dumpBatchFilesToTemp || csfields.get(fieldNameForURL) != null) {
                    String _url = CloudSearchUtils.stripNonCharCodepoints(normalisedurl);
                    fields.put(fieldNameForURL, _url);
                }
            }

            // include the text ?
            String fieldNameForText = fieldNameForText();
            if (StringUtils.isNotBlank(fieldNameForText)) {
                fieldNameForText = CloudSearchUtils.cleanFieldName(fieldNameForText);
                if (this.dumpBatchFilesToTemp || csfields.get(fieldNameForText) != null) {
                    text = CloudSearchUtils.stripNonCharCodepoints(trimText(text));
                    fields.put(fieldNameForText, text);
                }
            }

            doc_builder.set("fields", fields);

            addToBatch(objectMapper.writeValueAsString(doc_builder), url, tuple);

        } catch (Exception e) {
            LOG.error("Exception caught while building JSON object", e);
            // resending would produce the same results no point in retrying
            _collector.emit(StatusStreamName, tuple, new Values(url, metadata, Status.ERROR));
            _collector.ack(tuple);
        }
    }

    private void addToBatch(String currentDoc, String url, Tuple tuple) {
        int currentDocLength = currentDoc.getBytes(StandardCharsets.UTF_8).length;

        // check that the doc is not too large -> skip it if it does
        if (currentDocLength > MAX_SIZE_DOC_BYTES) {
            LOG.error("Doc too large. currentDoc.length {} : {}", currentDocLength, url);
            return;
        }

        int currentBufferLength = buffer.toString().getBytes(StandardCharsets.UTF_8).length;

        LOG.debug("currentDoc.length {}, buffer length {}", currentDocLength, currentBufferLength);

        // can add it to the buffer without overflowing?
        if (currentDocLength + 2 + currentBufferLength < MAX_SIZE_BATCH_BYTES) {
            if (numDocsInBatch != 0) {
                buffer.append(',');
            }
            buffer.append(currentDoc);
            this.unacked.add(tuple);
            numDocsInBatch++;
        } else {
            // flush the previous batch and create a new one with this doc
            sendBatch();
            buffer.append(currentDoc);
            this.unacked.add(tuple);
            numDocsInBatch++;
        }

        // have we reached the max number of docs in a batch after adding
        // this doc?
        if (maxDocsInBatch > 0 && numDocsInBatch == maxDocsInBatch) {
            sendBatch();
        }
    }

    public void sendBatch() {

        timeLastBatchSent = System.currentTimeMillis();

        // nothing to do
        if (numDocsInBatch == 0) {
            return;
        }

        // close the array
        buffer.append(']');

        LOG.info("Sending {} docs to CloudSearch", numDocsInBatch);

        byte[] bb = buffer.toString().getBytes(StandardCharsets.UTF_8);

        if (dumpBatchFilesToTemp) {
            try {
                File temp = Files.createTempFile("CloudSearch_", ".json").toFile();
                FileUtils.writeByteArrayToFile(temp, bb);
                LOG.info("Wrote batch file {}", temp.getName());
                // ack the tuples
                for (Tuple t : unacked) {
                    String url = t.getStringByField("url");
                    Metadata metadata = (Metadata) t.getValueByField("metadata");
                    _collector.emit(StatusStreamName, t, new Values(url, metadata, Status.FETCHED));
                    _collector.ack(t);
                }
                unacked.clear();
            } catch (IOException e1) {
                LOG.error("Exception while generating batch file", e1);
                // fail the tuples
                for (Tuple t : unacked) {
                    _collector.fail(t);
                }
                unacked.clear();
            } finally {
                // reset buffer and doc counter
                buffer = new StringBuffer(MAX_SIZE_BATCH_BYTES).append('[');
                numDocsInBatch = 0;
            }
            return;
        }
        // not in debug mode
        try {
            UploadDocumentsRequest batch =
                    UploadDocumentsRequest.builder()
                            .contentLength((long) bb.length)
                            .contentType(ContentType.APPLICATION_JSON)
                            .build();
            UploadDocumentsResponse result =
                    client.uploadDocuments(batch, RequestBody.fromBytes(bb));
            LOG.info(result.status());
            for (DocumentServiceWarning warning : result.warnings()) {
                LOG.info(warning.message());
            }
            if (!result.warnings().isEmpty()) {
                eventCounter.scope("Warnings").incrBy(result.warnings().size());
            }
            eventCounter.scope("Added").incrBy(result.adds());
            // ack the tuples
            for (Tuple t : unacked) {
                String url = t.getStringByField("url");
                Metadata metadata = (Metadata) t.getValueByField("metadata");
                _collector.emit(StatusStreamName, t, new Values(url, metadata, Status.FETCHED));
                _collector.ack(t);
            }
            unacked.clear();
        } catch (Exception e) {
            LOG.error("Exception while sending batch", e);
            LOG.error(buffer.toString());
            // fail the tuples
            for (Tuple t : unacked) {
                _collector.fail(t);
            }
            unacked.clear();
        } finally {
            // reset buffer and doc counter
            buffer = new StringBuffer(MAX_SIZE_BATCH_BYTES).append('[');
            numDocsInBatch = 0;
        }
    }

    @Override
    public void cleanup() {
        // This will flush any unsent documents.
        sendBatch();
        if (client != null) {
            client.close();
        }
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Config conf = new Config();
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 1);
        return conf;
    }
}
