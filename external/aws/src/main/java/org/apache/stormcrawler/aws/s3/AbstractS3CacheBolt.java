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

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.RegionUtils;
import com.amazonaws.services.s3.AmazonS3Client;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.storm.metric.api.MultiCountMetric;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.stormcrawler.util.ConfUtils;

public abstract class AbstractS3CacheBolt extends BaseRichBolt {

    public static final String S3_PREFIX = "s3.";
    public static final String ENDPOINT = S3_PREFIX + "endpoint";
    public static final String BUCKET = S3_PREFIX + "bucket";

    // is the region needed?
    public static final String REGION = S3_PREFIX + "region";

    public static final String INCACHE = S3_PREFIX + "inCache";

    protected OutputCollector _collector;
    protected MultiCountMetric eventCounter;

    protected AmazonS3Client client;

    protected String bucketName;

    /** Returns an S3 client given the configuration * */
    public static AmazonS3Client getS3Client(Map<String, Object> conf) {
        AWSCredentialsProvider provider = new DefaultAWSCredentialsProviderChain();
        AWSCredentials credentials = provider.getCredentials();
        ClientConfiguration config = new ClientConfiguration();

        AmazonS3Client client = new AmazonS3Client(credentials, config);

        String regionName = ConfUtils.getString(conf, REGION);
        if (StringUtils.isNotBlank(regionName)) {
            client.setRegion(RegionUtils.getRegion(regionName));
        }

        String endpoint = ConfUtils.getString(conf, ENDPOINT);
        if (StringUtils.isNotBlank(endpoint)) {
            client.setEndpoint(endpoint);
        }
        return client;
    }

    @Override
    public void prepare(
            Map<String, Object> conf, TopologyContext context, OutputCollector collector) {
        _collector = collector;
        client = getS3Client(conf);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("url", "content", "metadata"));
    }
}
