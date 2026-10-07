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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;
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
 * writes to, without running a topology. One or more instances of the bolt are created from its
 * class name, prepared with the configuration and given the URLs from a file, after which the
 * utility waits for all of them to be acked or failed. The number of URLs acked and the throughput
 * are printed every minute and at the end; only the warnings and errors are logged so that they
 * stay readable.
 *
 * <p>The URLs are passed directly to the {@code store} method of the bolt instead of {@code
 * execute}, so that only the storage is measured, without the effect of the cache of discovered
 * URLs, of the scheduling or of the filtering of the metadata done by {@link
 * AbstractStatusUpdaterBolt}. The cache is disabled whatever the configuration says.
 *
 * <p>Running several instances shows what a topology with a higher parallelism for the status
 * updater would get. As Storm would give each of them its own executor, every instance has its own
 * task and is called from its own thread. The URLs are routed to the instances based on their hash,
 * as a fields grouping on the URL does, so that a given URL always goes to the same instance. The
 * input is read and parsed by a separate thread, which caps the total throughput at what it can
 * deliver; a run with the {@link MemoryStatusUpdater} on a sample of the input gives an idea of
 * that limit.
 *
 * <p>The arguments are, in this order, the class name of the bolt, optionally the number of
 * instances to run (1 by default), one or more configuration files, which are loaded after
 * crawler-default.yaml, and the file containing the URLs. A single configuration file with the
 * settings of the backend is usually all that is needed, whatever its name: the settings of the
 * rest of a crawl don't apply to the bolt on its own. The arguments are positional as the {@code
 * storm} command would take options such as {@code -c} for its own.
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
 *   org.apache.stormcrawler.opensearch.persistence.StatusUpdaterBolt 4 \
 *   backend.yaml urls.txt.gz
 * </pre>
 */
public class StatusUpdaterBenchmark {

    private static final String SOURCE = "benchmark";
    private static final int SOURCE_TASK = 0;
    private static final String BOLT = "status";
    private static final int FIRST_BOLT_TASK = 1;

    /** number of URLs handed to an instance at a time, so that the threads rarely synchronize */
    private static final int HANDOFF_BATCH_SIZE = 1000;

    /** number of batches of URLs waiting for each instance */
    private static final int HANDOFF_QUEUE_SIZE = 10;

    /** how long to wait for the acks once all the URLs have been sent */
    private static final long ACK_TIMEOUT_MSEC = 5 * 60 * 1000;

    /** how often the intermediate throughput is displayed */
    private static final long REPORT_INTERVAL_SEC = 60;

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        // the number of instances is optional
        int instances = 1;
        int firstConf = 1;
        if (args.length > 1 && args[1].matches("\\d+")) {
            instances = Integer.parseInt(args[1]);
            firstConf = 2;
        }
        if (args.length - firstConf < 2 || instances < 1) {
            System.err.println(
                    "Usage: StatusUpdaterBenchmark <bolt class> [<instances>] <conf file>..."
                            + " <URL file>");
            System.exit(1);
        }

        // log4j is what storm local uses, the INFO messages of the bolts would drown the reports
        Configurator.setAllLevels(LogManager.ROOT_LOGGER_NAME, Level.WARN);

        Config conf = new Config();
        conf.putAll(
                ConfUtils.extractConfigElement(
                        Utils.findAndReadConfigFile("crawler-default.yaml", false)));
        for (int i = firstConf; i < args.length - 1; i++) {
            ConfUtils.loadConf(args[i], conf);
        }
        // store() bypasses the cache, the URLs would be added to it but never looked up
        conf.put(AbstractStatusUpdaterBolt.useCacheParamName, false);

        CountingCollector collector = new CountingCollector();

        // one task per instance, each called from its own thread as with Storm executors
        List<Integer> tasks =
                IntStream.range(FIRST_BOLT_TASK, FIRST_BOLT_TASK + instances).boxed().toList();
        List<AbstractStatusUpdaterBolt> bolts = new ArrayList<>(instances);
        List<BlockingQueue<List<Item>>> queues = new ArrayList<>(instances);
        List<Thread> threads = new ArrayList<>(instances);
        for (int task : tasks) {
            AbstractStatusUpdaterBolt bolt =
                    Class.forName(args[0])
                            .asSubclass(AbstractStatusUpdaterBolt.class)
                            .getDeclaredConstructor()
                            .newInstance();
            TopologyContext context = createContext(conf, task, tasks);
            bolt.prepare(conf, context, new OutputCollector(collector));
            BlockingQueue<List<Item>> queue = new ArrayBlockingQueue<>(HANDOFF_QUEUE_SIZE);
            Thread thread =
                    new Thread(
                            () -> storeAll(bolt, context, queue, collector),
                            "StatusUpdaterBenchmark-bolt-" + task);
            // must not keep the JVM alive if reading the input fails
            thread.setDaemon(true);
            bolts.add(bolt);
            queues.add(queue);
            threads.add(thread);
        }

        long start = System.currentTimeMillis();
        long sent = 0;
        threads.forEach(Thread::start);

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

        // the input is read and parsed by this thread, which hands the URLs to the instances
        List<List<Item>> batches = new ArrayList<>(instances);
        for (int i = 0; i < instances; i++) {
            batches.add(new ArrayList<>(HANDOFF_BATCH_SIZE));
        }
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
                // a URL always goes to the same instance, as with a fields grouping on the URL
                int target = Math.floorMod(item.url().hashCode(), instances);
                List<Item> batch = batches.get(target);
                batch.add(item);
                if (batch.size() == HANDOFF_BATCH_SIZE) {
                    queues.get(target).put(batch);
                    batches.set(target, new ArrayList<>(HANDOFF_BATCH_SIZE));
                }
                sent++;
            }
        }

        // hand over what is left, then an empty batch to mark the end of the input
        for (int i = 0; i < instances; i++) {
            if (!batches.get(i).isEmpty()) {
                queues.get(i).put(batches.get(i));
            }
            queues.get(i).put(List.of());
        }
        for (Thread thread : threads) {
            thread.join();
        }

        // the acks can be done asynchronously by the bolts
        long deadline = System.currentTimeMillis() + ACK_TIMEOUT_MSEC;
        while (collector.acked.get() + collector.failed.get() < sent
                && System.currentTimeMillis() < deadline) {
            Thread.sleep(10);
        }

        long elapsed = Math.max(1, System.currentTimeMillis() - start);
        reporter.shutdownNow();

        for (AbstractStatusUpdaterBolt bolt : bolts) {
            bolt.cleanup();
        }

        System.out.println("Instances: " + instances);
        System.out.println("Sent: " + sent);
        System.out.println("Acked: " + collector.acked.get());
        System.out.println("Failed: " + collector.failed.get());
        System.out.println("Total time: " + elapsed + " msec");
        System.out.printf(
                Locale.ROOT, "Average OPS: %.2f%n", collector.acked.get() * 1000.0 / elapsed);

        // some backends leave non-daemon threads behind
        System.exit(collector.acked.get() == sent ? 0 : 1);
    }

    /**
     * Passes the URLs handed over by the reading thread to one instance of the bolt, until an empty
     * batch marks the end of the input.
     */
    private static void storeAll(
            AbstractStatusUpdaterBolt bolt,
            TopologyContext context,
            BlockingQueue<List<Item>> queue,
            CountingCollector collector) {
        try {
            while (true) {
                List<Item> batch = queue.take();
                if (batch.isEmpty()) {
                    return;
                }
                for (Item item : batch) {
                    // needed by the bolt to ack or fail
                    Tuple tuple =
                            new TupleImpl(
                                    context,
                                    new Values(item.url(), item.metadata(), item.status()),
                                    SOURCE,
                                    SOURCE_TASK,
                                    Utils.DEFAULT_STREAM_ID);
                    // store() is protected but accessible from this package
                    try {
                        bolt.store(
                                item.url(),
                                item.status(),
                                item.metadata(),
                                item.nextFetch(),
                                tuple);
                    } catch (Exception e) {
                        System.err.println(
                                "Exception caught when storing " + item.url() + ": " + e);
                        collector.fail(tuple);
                    }
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /** Context of one of the tasks of the bolt, all of them running in the same worker. */
    private static TopologyContext createContext(
            Map<String, Object> conf, int task, List<Integer> tasks) {
        Map<Integer, String> taskToComponent = new HashMap<>();
        taskToComponent.put(SOURCE_TASK, SOURCE);
        for (int t : tasks) {
            taskToComponent.put(t, BOLT);
        }
        return new TopologyContext(
                new StormTopology(),
                conf,
                taskToComponent,
                Map.of(SOURCE, List.of(SOURCE_TASK), BOLT, tasks),
                Map.of(
                        SOURCE,
                        Map.of(Utils.DEFAULT_STREAM_ID, new Fields("url", "metadata", "status"))),
                new HashMap<>(),
                "status-updater-benchmark",
                null,
                null,
                task,
                0,
                tasks,
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
