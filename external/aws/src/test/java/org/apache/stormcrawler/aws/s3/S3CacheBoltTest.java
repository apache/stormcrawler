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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Tuple;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.TestOutputCollector;
import org.apache.stormcrawler.TestUtil;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;

@Testcontainers(disabledWithoutDocker = true)
@Timeout(value = 120, unit = TimeUnit.SECONDS)
class S3CacheBoltTest {

    private static final DockerImageName IMAGE =
            DockerImageName.parse("localstack/localstack:3.8.1");

    @Container
    static LocalStackContainer localstack =
            new LocalStackContainer(IMAGE).withServices(LocalStackContainer.Service.S3);

    private static final String BUCKET = "sc-cache";

    private TestOutputCollector output;

    @BeforeAll
    static void beforeAll() {
        // The bolts resolve credentials through the default provider chain; expose the LocalStack
        // ones as Java system properties so that chain picks them up.
        System.setProperty("aws.accessKeyId", localstack.getAccessKey());
        System.setProperty("aws.secretAccessKey", localstack.getSecretKey());

        try (S3Client admin = AbstractS3CacheBolt.getS3Client(conf())) {
            admin.createBucket(CreateBucketRequest.builder().bucket(BUCKET).build());
        }
    }

    @AfterAll
    static void afterAll() {
        System.clearProperty("aws.accessKeyId");
        System.clearProperty("aws.secretAccessKey");
    }

    @BeforeEach
    void setup() {
        output = new TestOutputCollector();
    }

    private static Map<String, Object> conf() {
        Map<String, Object> conf = new HashMap<>();
        conf.put(AbstractS3CacheBolt.ENDPOINT, localstack.getEndpoint().toString());
        conf.put(AbstractS3CacheBolt.REGION, localstack.getRegion());
        conf.put(AbstractS3CacheBolt.BUCKET, BUCKET);
        // LocalStack is reached through a host/port endpoint, so path-style access is required.
        conf.put(AbstractS3CacheBolt.PATH_STYLE_ACCESS, true);
        return conf;
    }

    private Tuple cacherTuple(String url, byte[] content, Metadata metadata) {
        Tuple tuple = mock(Tuple.class);
        when(tuple.getBinaryByField("content")).thenReturn(content);
        when(tuple.getStringByField("url")).thenReturn(url);
        when(tuple.getValueByField("metadata")).thenReturn(metadata);
        return tuple;
    }

    private Tuple checkerTuple(String url, Metadata metadata) {
        Tuple tuple = mock(Tuple.class);
        when(tuple.getStringByField("url")).thenReturn(url);
        when(tuple.getValueByField("metadata")).thenReturn(metadata);
        return tuple;
    }

    @Test
    void storesAndRetrievesContent() {
        String url = "https://www.example.com/some/page";
        byte[] content = "the cached body".getBytes(StandardCharsets.UTF_8);

        // store the content in S3
        S3ContentCacher cacher = new S3ContentCacher();
        cacher.prepare(conf(), TestUtil.getMockedTopologyContext(), new OutputCollector(output));
        cacher.execute(cacherTuple(url, content, new Metadata()));

        assertEquals(1, output.getAckedTuples().size());

        // retrieve it again through the checker
        TestOutputCollector checkerOutput = new TestOutputCollector();
        S3CacheChecker checker = new S3CacheChecker();
        checker.prepare(
                conf(), TestUtil.getMockedTopologyContext(), new OutputCollector(checkerOutput));

        Metadata metadata = new Metadata();
        checker.execute(checkerTuple(url, metadata));

        assertEquals(1, checkerOutput.getAckedTuples().size());

        List<List<Object>> cached = checkerOutput.getEmitted(S3CacheChecker.CACHE_STREAM);
        assertEquals(1, cached.size());
        assertArrayEquals(content, (byte[]) cached.get(0).get(1));
        assertEquals("true", metadata.getFirstValue(AbstractS3CacheBolt.INCACHE));
    }

    @Test
    void missingKeyIsForwardedDownstream() {
        S3CacheChecker checker = new S3CacheChecker();
        checker.prepare(conf(), TestUtil.getMockedTopologyContext(), new OutputCollector(output));

        Metadata metadata = new Metadata();
        checker.execute(checkerTuple("https://www.example.com/never-cached", metadata));

        assertEquals(1, output.getAckedTuples().size());
        // forwarded on the default stream, not the cache stream
        assertEquals(1, output.getEmitted().size());
        assertEquals(0, output.getEmitted(S3CacheChecker.CACHE_STREAM).size());
    }

    @Test
    void prepareFailsWhenBucketIsMissing() {
        Map<String, Object> conf = conf();
        conf.put(AbstractS3CacheBolt.BUCKET, "does-not-exist");

        S3CacheChecker checker = new S3CacheChecker();
        assertThrows(
                RuntimeException.class,
                () ->
                        checker.prepare(
                                conf,
                                TestUtil.getMockedTopologyContext(),
                                new OutputCollector(output)));
    }
}
