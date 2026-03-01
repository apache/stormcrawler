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
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.fs.Path;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy.Units;
import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;
import org.apache.storm.hdfs.common.AbstractHDFSWriter;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.Utils;
import org.apache.stormcrawler.protocol.ProtocolResponse;
import org.apache.stormcrawler.util.ConfUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WARCHdfsBolt extends GzipHdfsBolt {

    private static final Logger LOG = LoggerFactory.getLogger(WARCHdfsBolt.class);

    private static final String METADATA_KEYS_STORE = "warc.metadata.keys";

    private Map<String, String> header_fields = new HashMap<>();

    // by default remains as is-pre 1.17
    private String protocolMDprefix = "";

    private boolean withRequestRecords = false;

    public WARCHdfsBolt() {
        super();
        FileSizeRotationPolicy rotpol = new FileSizeRotationPolicy(1.0f, Units.GB);
        withRotationPolicy(rotpol);
        // dummy sync policy
        withSyncPolicy(new CountSyncPolicy(10));
        // default local filesystem
        withFsUrl("file:///");
    }

    public WARCHdfsBolt withHeader(Map<String, String> header_fields) {
        this.header_fields = header_fields;
        return this;
    }

    public WARCHdfsBolt withRequestRecords() {
        withRequestRecords = true;
        return this;
    }

    @Override
    public void doPrepare(
            Map<String, Object> conf, TopologyContext topologyContext, OutputCollector collector)
            throws IOException {
        super.doPrepare(conf, topologyContext, collector);
        protocolMDprefix = ConfUtils.getString(conf, ProtocolResponse.PROTOCOL_MD_PREFIX_PARAM, "");
        withRecordFormat(new WARCRecordFormat(protocolMDprefix));
        if (withRequestRecords) {
            addRecordFormat(new WARCRequestRecordFormat(protocolMDprefix), 0);
        }
        // detect if a list of keys was specified to be stored in the metadata
        List<String> metadataToWrite = ConfUtils.loadListFromConf(METADATA_KEYS_STORE, conf);
        if (!metadataToWrite.isEmpty()) {
            addRecordFormat(new MetadataRecordFormat(metadataToWrite), 1);
        }
    }

    @Override
    protected AbstractHDFSWriter makeNewWriter(Path path, Tuple tuple) throws IOException {
        AbstractHDFSWriter writer = super.makeNewWriter(path, tuple);

        Instant now = Instant.now();

        // overrides the filename and creation date in the headers
        header_fields.put("WARC-Date", WARCRecordFormat.WARC_DF.format(now));
        header_fields.put("WARC-Filename", path.getName());

        LOG.info("Opening WARC file {}", path);

        byte[] header = WARCRecordFormat.generateWARCInfo(header_fields);

        // write the header at the beginning of the file
        if (header != null && header.length > 0) {
            super.out.write(Utils.gzip(header));
        }

        return writer;
    }
}
