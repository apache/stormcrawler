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

package org.apache.stormcrawler.persistence;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.GZIPInputStream;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.storm.Config;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.metrics2.StormMetricRegistry;
import org.apache.storm.task.IOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.TupleImpl;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.util.ConfUtils;

/**
 * Simple utility for measuring the performance of a status updater bolt and of the backend it
 * writes to, without running a topology. The bolt is instantiated from its class name, prepared
 * with the configuration and given the URLs from a file one by one, after which the utility waits
 * for all of them to be acked or failed. The number of URLs acked and the throughput are printed
 * every minute and at the end; only the warnings and errors are logged so that they stay readable.
 *
 * <p>The URLs are passed directly to the {@code store} method of the bolt instead of {@code
 * execute}, so that only the storage is measured, without the effect of the cache of discovered
 * URLs, of the scheduling or of the filtering of the metadata done by {@link
 * AbstractStatusUpdaterBolt}. The cache is disabled whatever the configuration says.
 *
 * <p>The arguments are, in this order, the class name of the bolt, one or more configuration files,
 * which are loaded after crawler-default.yaml, and the file containing the URLs. They are
 * positional as the {@code storm} command would take options such as {@code -c} for its own.
 *
 * <p>The input has the same format as the one used by the URLFrontier client: each line is either a
 * plain URL or the JSON representation of a URLFrontier URLItem, e.g. <code>
 * {"discovered": {"info": {"url": "https://example.com/", "metadata": {"depth": {"values":
 * ["0"]}}}}}</code>. Plain URLs and discovered items are stored as DISCOVERED, with the current
 * date as next fetch date. Known items are stored as FETCHED, with their refetchable_from_date as
 * next fetch date or, if it is 0 or not set, with none i.e. never to be refetched. The metadata are
 * stored as they are in the input. The file is decompressed with gzip if its name ends in .gz.
 *
 * <p>The URLs should be shuffled beforehand, e.g. with {@code zcat urls.gz | shuf | gzip >
 * shuffled.gz}. Frontier or index dumps usually list them host by host whereas in a crawl the hosts
 * are interleaved, which matters when the backend partitions the URLs by host: with the OpenSearch
 * status updater and routing enabled, a file sorted by host sends each bulk request to one or two
 * shards instead of spreading it across all of them, which lowers the throughput.
 *
 * <p>The utility must be run with {@code storm local}, from the jar of a crawl project, such as one
 * generated with the archetype: Storm is a provided dependency, so its classes are not in the jar
 * and running the utility with {@code java -cp} fails with a {@link NoClassDefFoundError}. As the
 * bolt is loaded from its class name, the dependency for the module it belongs to, e.g. {@code
 * stormcrawler-opensearch} for the OpenSearch status updater, must also be present in the pom.xml
 * of the project.
 *
 * <pre>
 * storm local target/crawler-1.0-SNAPSHOT.jar \
 *   org.apache.stormcrawler.persistence.StatusUpdaterBenchmark \
 *   org.apache.stormcrawler.opensearch.persistence.StatusUpdaterBolt \
 *   crawler-conf.yaml opensearch-conf.yaml urls.txt.gz
 * </pre>
 */
public class StatusUpdaterBenchmark {

    private static final String SOURCE = "benchmark";
    private static final int SOURCE_TASK = 0;
    private static final String BOLT = "status";
    private static final int BOLT_TASK = 1;

    /** how long to wait for the acks once all the URLs have been sent */
    private static final long ACK_TIMEOUT_MSEC = 5 * 60 * 1000;

    /** how often the intermediate throughput is displayed */
    private static final long REPORT_INTERVAL_SEC = 60;

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println(
                    "Usage: StatusUpdaterBenchmark <bolt class> <conf file>... <URL file>");
            System.exit(1);
        }

        // log4j is what storm local uses, the INFO messages of the bolts would drown the reports
        Configurator.setAllLevels(LogManager.ROOT_LOGGER_NAME, Level.WARN);

        Config conf = new Config();
        conf.putAll(
                ConfUtils.extractConfigElement(
                        Utils.findAndReadConfigFile("crawler-default.yaml", false)));
        for (int i = 1; i < args.length - 1; i++) {
            ConfUtils.loadConf(args[i], conf);
        }
        // store() bypasses the cache, the URLs would be added to it but never looked up
        conf.put(AbstractStatusUpdaterBolt.useCacheParamName, false);

        AbstractStatusUpdaterBolt bolt =
                Class.forName(args[0])
                        .asSubclass(AbstractStatusUpdaterBolt.class)
                        .getDeclaredConstructor()
                        .newInstance();

        TopologyContext context = createContext(conf);
        CountingCollector collector = new CountingCollector();
        bolt.prepare(conf, context, new OutputCollector(collector));

        long start = System.currentTimeMillis();
        long sent = 0;

        // reports the throughput at regular intervals, as the URLFrontier client does
        ScheduledExecutorService reporter =
                Executors.newSingleThreadScheduledExecutor(
                        r -> {
                            Thread t = new Thread(r, "StatusUpdaterBenchmark-reporter");
                            t.setDaemon(true);
                            return t;
                        });
        AtomicLong lastAcked = new AtomicLong();
        AtomicLong lastReport = new AtomicLong(start);
        reporter.scheduleAtFixedRate(
                () -> {
                    long now = System.currentTimeMillis();
                    long elapsed = now - lastReport.getAndSet(now);
                    long current = collector.acked.get();
                    long delta = current - lastAcked.getAndSet(current);
                    System.out.printf(
                            Locale.ROOT,
                            "Acked: %d - OPS over the last %d sec: %.2f%n",
                            current,
                            Math.round(elapsed / 1000.0),
                            delta * 1000.0 / Math.max(1, elapsed));
                },
                REPORT_INTERVAL_SEC,
                REPORT_INTERVAL_SEC,
                TimeUnit.SECONDS);

        try (BufferedReader reader = openInput(args[args.length - 1])) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.isBlank()) {
                    continue;
                }
                Item item = parse(line);
                if (item == null) {
                    System.err.println("Invalid input line: " + line);
                    continue;
                }
                // needed by the bolt to ack or fail
                Tuple tuple =
                        new TupleImpl(
                                context,
                                new Values(item.url(), item.metadata(), item.status()),
                                SOURCE,
                                SOURCE_TASK,
                                Utils.DEFAULT_STREAM_ID);
                sent++;
                // store() is protected but accessible from this package
                try {
                    bolt.store(item.url(), item.status(), item.metadata(), item.nextFetch(), tuple);
                } catch (Exception e) {
                    System.err.println("Exception caught when storing " + item.url() + ": " + e);
                    collector.fail(tuple);
                }
            }
        }

        // the acks can be done asynchronously by the bolt
        long deadline = System.currentTimeMillis() + ACK_TIMEOUT_MSEC;
        while (collector.acked.get() + collector.failed.get() < sent
                && System.currentTimeMillis() < deadline) {
            Thread.sleep(10);
        }

        long elapsed = Math.max(1, System.currentTimeMillis() - start);
        reporter.shutdownNow();

        bolt.cleanup();

        System.out.println("Sent: " + sent);
        System.out.println("Acked: " + collector.acked.get());
        System.out.println("Failed: " + collector.failed.get());
        System.out.println("Total time: " + elapsed + " msec");
        System.out.printf(
                Locale.ROOT, "Average OPS: %.2f%n", collector.acked.get() * 1000.0 / elapsed);

        // some backends leave non-daemon threads behind
        System.exit(collector.acked.get() == sent ? 0 : 1);
    }

    /** Context in which the bolt is the only task of its component. */
    private static TopologyContext createContext(Map<String, Object> conf) {
        return new TopologyContext(
                new StormTopology(),
                conf,
                Map.of(SOURCE_TASK, SOURCE, BOLT_TASK, BOLT),
                Map.of(SOURCE, List.of(SOURCE_TASK), BOLT, List.of(BOLT_TASK)),
                Map.of(
                        SOURCE,
                        Map.of(Utils.DEFAULT_STREAM_ID, new Fields("url", "metadata", "status"))),
                new HashMap<>(),
                "status-updater-benchmark",
                null,
                null,
                BOLT_TASK,
                0,
                List.of(BOLT_TASK),
                new HashMap<>(),
                new HashMap<>(),
                new HashMap<>(),
                new HashMap<>(),
                new AtomicBoolean(false),
                new StormMetricRegistry());
    }

    private static BufferedReader openInput(String file) throws IOException {
        InputStream is = Files.newInputStream(Paths.get(file));
        if (file.endsWith(".gz")) {
            is = new GZIPInputStream(is);
        }
        return new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8));
    }

    /** A URL to store, with the values to pass to the bolt. */
    record Item(String url, Status status, Metadata metadata, Optional<Date> nextFetch) {}

    /** Returns the item for a line of input or null if it is invalid. */
    static Item parse(String line) {
        String input = line.trim();
        if (!input.startsWith("{")) {
            return new Item(input, Status.DISCOVERED, new Metadata(), Optional.of(new Date()));
        }

        JsonNode root;
        try {
            root = MAPPER.readTree(input);
        } catch (IOException e) {
            return null;
        }

        JsonNode node = root.path("discovered");
        boolean discovered = !node.isMissingNode();
        if (!discovered) {
            node = root.path("known");
        }

        JsonNode url = node.path("info").path("url");
        if (!url.isTextual()) {
            return null;
        }

        Metadata metadata = new Metadata();
        for (Map.Entry<String, JsonNode> entry : node.path("info").path("metadata").properties()) {
            for (JsonNode value : entry.getValue().path("values")) {
                metadata.addValue(entry.getKey(), value.asText());
            }
        }

        if (discovered) {
            return new Item(url.asText(), Status.DISCOVERED, metadata, Optional.of(new Date()));
        }

        // int64 values can be written as strings, either name is accepted
        long refetch =
                node.has("refetchableFromDate")
                        ? node.get("refetchableFromDate").asLong()
                        : node.path("refetchable_from_date").asLong();
        Optional<Date> nextFetch =
                refetch > 0
                        ? Optional.of(Date.from(Instant.ofEpochSecond(refetch)))
                        : Optional.empty();
        return new Item(url.asText(), Status.FETCHED, metadata, nextFetch);
    }

    /** Counts the acks and fails, which can come from any thread. */
    private static class CountingCollector implements IOutputCollector {

        final AtomicLong acked = new AtomicLong();
        final AtomicLong failed = new AtomicLong();

        @Override
        public void ack(Tuple input) {
            acked.incrementAndGet();
        }

        @Override
        public void fail(Tuple input) {
            failed.incrementAndGet();
        }

        @Override
        public List<Integer> emit(String streamId, Collection<Tuple> anchors, List<Object> tuple) {
            return Collections.emptyList();
        }

        @Override
        public void emitDirect(
                int taskId, String streamId, Collection<Tuple> anchors, List<Object> tuple) {}

        @Override
        public void resetTimeout(Tuple input) {}

        @Override
        public void flush() {}

        @Override
        public void reportError(Throwable error) {
            error.printStackTrace();
        }
    }
}
