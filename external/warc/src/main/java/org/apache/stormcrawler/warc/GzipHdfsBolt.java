/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.stormcrawler.warc;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.storm.hdfs.bolt.HdfsBolt;
import org.apache.storm.hdfs.bolt.format.RecordFormat;
import org.apache.storm.hdfs.common.AbstractHDFSWriter;
import org.apache.storm.hdfs.common.HDFSWriter;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Unlike the standard HdfsBolt this one writes to a gzipped stream with per-record compression. */
public class GzipHdfsBolt extends HdfsBolt {

    private static final Logger LOG = LoggerFactory.getLogger(GzipHdfsBolt.class);

    protected transient FSDataOutputStream out = null;

    protected RecordFormat format;

    public static class GzippedRecordFormat implements RecordFormat {

        private RecordFormat baseFormat;

        /**
         * whether to skip empty records (byte[] of length 0), i.e. do not create an gzip container
         * containing nothing
         */
        protected boolean compressEmpty = false;

        public GzippedRecordFormat(RecordFormat format) {
            baseFormat = format;
        }

        @Override
        public byte[] format(Tuple tuple) {
            try {
                byte[] bytes = baseFormat.format(tuple);
                if (bytes.length == 0 && !compressEmpty) {
                    return new byte[0];
                }
                return Utils.gzip(bytes);
            } catch (Exception e) {
                LOG.error("Exception caught when formatting - skipping the whole tuple");
                return new byte[0];
            }
        }
    }

    public static class MultipleRecordFormat implements RecordFormat {

        private List<RecordFormat> formats = new ArrayList<>();

        public MultipleRecordFormat(RecordFormat format) {
            addFormat(format, 0);
        }

        public void addFormat(RecordFormat format, int position) {
            if (position < 0 || position > formats.size()) {
                formats.add(format);
            } else {
                formats.add(position, format);
            }
        }

        @Override
        public byte[] format(Tuple tuple) {
            // aggregate the arrays returned by each format into a single one
            byte[][] tmp = new byte[formats.size()][];
            int i = -1;
            int size = 0;
            for (RecordFormat format : formats) {
                try {
                    tmp[++i] = format.format(tuple);
                    size += tmp[i].length;
                } catch (Exception e) {
                    LOG.error("Exception caught when formatting - skipping the whole tuple");
                    return new byte[0];
                }
            }
            byte[] res = new byte[size];
            int pos = 0;
            for (i = 0; i < tmp.length; i++) {
                System.arraycopy(tmp[i], 0, res, pos, tmp[i].length);
                pos += tmp[i].length;
            }
            return res;
        }
    }

    /** Sets the record format, overwriting the existing one(s) */
    public GzipHdfsBolt withRecordFormat(RecordFormat format) {
        this.format = new GzippedRecordFormat(format);
        return this;
    }

    /** Add an additional record format at end of existing ones */
    public GzipHdfsBolt addRecordFormat(RecordFormat format) {
        return addRecordFormat(format, -1);
    }

    /** Add an additional record format at given position */
    public GzipHdfsBolt addRecordFormat(RecordFormat format, int position) {
        MultipleRecordFormat formats;
        if (this.format == null) {
            formats = new MultipleRecordFormat(format);
            this.format = formats;
        } else {
            if (this.format instanceof MultipleRecordFormat) {
                formats = (MultipleRecordFormat) this.format;
            } else {
                formats = new MultipleRecordFormat(this.format);
                this.format = formats;
            }
            formats.addFormat(new GzippedRecordFormat(format), position);
        }
        return this;
    }

    @Override
    public void cleanup() {
        LOG.info("Cleanup called on bolt");
        if (this.out == null) {
            LOG.warn("Nothing to cleanup: output stream not initialized");
            return;
        }
        try {
            this.out.flush();
            this.out.close();
        } catch (IOException e) {
            LOG.error("Exception while calling cleanup");
        }
    }

    @Override
    protected AbstractHDFSWriter makeNewWriter(Path path, Tuple tuple) throws IOException {
        out = this.fs.create(path);
        return new HDFSWriter(rotationPolicy, path, out, format);
    }
}
