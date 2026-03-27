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
import com.amazonaws.services.s3.model.S3Object;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Map;
import org.apache.commons.io.IOUtils;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.metrics.CrawlerMetrics;
import org.apache.stormcrawler.util.ConfUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Checks whether a URL can be found in the cache, if not delegate to the following bolt e.g.
 * Fetcher, which gets bypassed otherwise. Does not enforce any politeness. The credentials must be
 * stored in ~/.aws/credentials
 */
public class S3CacheChecker extends AbstractS3CacheBolt {

    public static final Logger LOG = LoggerFactory.getLogger(S3CacheChecker.class);

    public static final String CACHE_STREAM = "cached";

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        super.prepare(conf, context, collector);
        bucketName = ConfUtils.getString(conf, BUCKET);

        boolean bucketExists = client.doesBucketExist(bucketName);
        if (!bucketExists) {
            String message = "Bucket " + bucketName + " does not exist";
            throw new RuntimeException(message);
        }
        this.eventCounter = CrawlerMetrics.registerCounter(context, conf, "s3cache_counter", 10);
    }

    @Override
    public void execute(Tuple tuple) {
        String url = tuple.getStringByField("url");
        Metadata metadata = (Metadata) tuple.getValueByField("metadata");

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
            eventCounter.scope("result_keytoobig").incrBy(1);
            _collector.emit(tuple, new Values(url, metadata));
            // ack it no matter what
            _collector.ack(tuple);
            return;
        }

        long preCacheQueryTime = System.currentTimeMillis();
        S3Object obj = null;
        try {
            obj = client.getObject(bucketName, key);
        } catch (AmazonS3Exception e) {
            eventCounter.scope("result_misses").incrBy(1);
            // key does not exist?
            // no need for logging
        }
        long postCacheQueryTime = System.currentTimeMillis();
        LOG.debug("Queried S3 cache in {} msec", (postCacheQueryTime - preCacheQueryTime));

        if (obj != null) {
            try {
                byte[] content = IOUtils.toByteArray(obj.getObjectContent());
                eventCounter.scope("result_hits").incrBy(1);
                eventCounter.scope("bytes_fetched").incrBy(content.length);

                metadata.setValue(INCACHE, "true");

                _collector.emit(CACHE_STREAM, tuple, new Values(url, content, metadata));
                _collector.ack(tuple);
                return;
            } catch (Exception e) {
                eventCounter.scope("result.exception").incrBy(1);
                LOG.error("IOException when extracting byte array", e);
            }
        }

        _collector.emit(tuple, new Values(url, metadata));
        _collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("url", "metadata"));
        declarer.declareStream(CACHE_STREAM, new Fields("url", "content", "metadata"));
    }
}
