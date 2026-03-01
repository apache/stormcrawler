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

package org.apache.stormcrawler.aws.s3;

import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectResult;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Map;
import org.apache.storm.metric.api.MultiCountMetric;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.util.ConfUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Stores binary content into Amazon S3. The credentials must be stored in ~/.aws/credentials */
public abstract class S3Cacher extends AbstractS3CacheBolt {

    public static final Logger LOG = LoggerFactory.getLogger(S3Cacher.class);

    protected abstract byte[] getContentToCache(Metadata metadata, byte[] content, String url);

    protected abstract String getKeyPrefix();

    protected abstract String getMetricPrefix();

    protected abstract boolean shouldOverwrite(Metadata metadata);

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {

        super.prepare(conf, context, collector);

        bucketName = ConfUtils.getString(conf, BUCKET);

        boolean bucketExists = client.doesBucketExist(bucketName);
        if (!bucketExists) {
            String message = "Bucket " + bucketName + " does not exist";
            throw new RuntimeException(message);
        }
        this.eventCounter =
                context.registerMetric(
                        getMetricPrefix() + "s3cache_counter", new MultiCountMetric(), 10);
    }

    @Override
    public void execute(Tuple tuple) {
        // stores the binary content on S3

        byte[] content = tuple.getBinaryByField("content");
        String url = tuple.getStringByField("url");
        final Metadata metadata = (Metadata) tuple.getValueByField("metadata");

        // If there is no content
        byte[] contentToCache = getContentToCache(metadata, content, url);
        if (contentToCache == null) {
            LOG.info("{} had no data to cache", url);
            _collector.emit(tuple, new Values(url, content, metadata));
            // ack it no matter what
            _collector.ack(tuple);
            return;
        }

        // already in the cache
        // don't need to recache it
        if (!shouldOverwrite(metadata)) {
            eventCounter.scope("already_in_cache").incr();
            _collector.emit(tuple, new Values(url, content, metadata));
            // ack it no matter what
            _collector.ack(tuple);
            return;
        }

        // normalises URL
        String key = "";
        try {
            key = URLEncoder.encode(url, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            // ignore it - we know UTF-8 is valid
        }
        // check size of the key
        if (key.length() >= 1024) {
            LOG.info("Key too large : {}", key);
            eventCounter.scope("key_too_large").incr();
            _collector.emit(tuple, new Values(url, content, metadata));
            // ack it no matter what
            _collector.ack(tuple);
            return;
        }

        ByteArrayInputStream input = new ByteArrayInputStream(contentToCache);

        ObjectMetadata md = new ObjectMetadata();
        md.setContentLength(contentToCache.length);
        md.setHeader("x-amz-storage-class", "STANDARD_IA");

        try {
            PutObjectResult result = client.putObject(bucketName, getKeyPrefix() + key, input, md);
            eventCounter.scope("cached").incr();
            // TODO check something with the result?
        } catch (AmazonS3Exception exception) {
            LOG.error("AmazonS3Exception while storing {}", url, exception);
            eventCounter.scope("s3_exception").incr();
        } finally {
            try {
                input.close();
            } catch (IOException e) {
                LOG.error("Error while closing ByteArrayInputStream", e);
            }
        }

        _collector.emit(tuple, new Values(url, content, metadata));
        // ack it no matter what
        _collector.ack(tuple);
    }
}
