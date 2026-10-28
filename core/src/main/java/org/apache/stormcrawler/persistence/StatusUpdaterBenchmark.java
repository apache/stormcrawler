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

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
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
 * input is read and decompressed by one thread and parsed by a pool of threads, 2 by default or the
 * value of {@code status.updater.benchmark.parsers} in the configuration. With more than one parser
 * the URLs are not handed to the instances in the exact order of the input, which can matter if it
 * contains the same URL more than once, e.g. as discovered then as known; a single parser keeps the
 * order. The parsers use CPU which is then not available to the instances, the number of parsers
 * should be the same for all the runs being compared. Reading and parsing cap the total throughput
 * at what they can deliver. Passing {@code baseline} instead of a class name measures that limit:
 * the URLs are acked as soon as they are received, without being stored, so the result is the
 * highest throughput the utility can report on that input and machine. The throughput of a backend
 * can then be expressed as a fraction of the baseline obtained with the same input, number of
 * instances and machine. Unlike the raw figures, which depend largely on the hardware, these ratios
 * can be compared across environments, e.g. to compare results obtained on different machines or to
 * track the performance of a backend over time.
 *
 * <p>The arguments are, in this order, the class name of the bolt or {@code baseline}, optionally
 * the number of instances to run (1 by default), one or more configuration files, which are loaded
 * after crawler-default.yaml, and the file containing the URLs. A single configuration file with
 * the settings of the backend is usually all that is needed, whatever its name: the settings of the
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
 *
 * <p>The baseline for the same input is obtained with:
 *
 * <pre>
 * storm local target/crawler-1.0-SNAPSHOT.jar \
 *   org.apache.stormcrawler.persistence.StatusUpdaterBenchmark \
 *   baseline 4 backend.yaml urls.txt.gz
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

    /** size of the blocks of input handed to the parsers */
    private static final int READ_BLOCK_SIZE = 1024 * 1024;

    /** size of the buffers used for reading and decompressing the input */
    private static final int INPUT_BUFFER_SIZE = 64 * 1024;

    /** number of threads parsing the input */
    public static final String PARSERS_PARAM_NAME = "status.updater.benchmark.parsers";

    /**
     * fixed rather than derived from the number of cores, so that the baselines obtained on
     * different machines are comparable
     */
    private static final int DEFAULT_PARSERS = 2;

    private static final JsonFactory JSON = new JsonFactory();

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
                    "Usage: StatusUpdaterBenchmark <bolt class>|baseline [<instances>]"
                            + " <conf file>... <URL file>");
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

        String className = args[0];

        // acks without storing, to measure the overhead of the utility
        if ("baseline".equalsIgnoreCase(className)) {
            className = BaselineStatusUpdater.class.getName();
        }

        for (int task : tasks) {
            AbstractStatusUpdaterBolt bolt =
                    Class.forName(className)
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

        int parsers = ConfUtils.getInt(conf, PARSERS_PARAM_NAME, DEFAULT_PARSERS);
        if (parsers < 1) {
            throw new IllegalArgumentException(
                    PARSERS_PARAM_NAME + " must be at least 1, got " + parsers);
        }

        long start = System.currentTimeMillis();
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

        // the input is read by this thread, which hands blocks of whole lines to the parsers,
        // which in turn hand the URLs to the instances: routing needs the URL, so the lines must
        // be parsed before an instance can be picked
        BlockingQueue<Block> blocks = new ArrayBlockingQueue<>(parsers * 2);
        AtomicLong sentCounter = new AtomicLong();
        List<Thread> parserThreads = new ArrayList<>(parsers);
        for (int i = 0; i < parsers; i++) {
            Thread thread =
                    new Thread(
                            () -> parseAll(blocks, queues, sentCounter),
                            "StatusUpdaterBenchmark-parser-" + i);
            thread.setDaemon(true);
            thread.start();
            parserThreads.add(thread);
        }

        try (InputStream in = openInput(args[args.length - 1])) {
            readBlocks(in, READ_BLOCK_SIZE, blocks);
        }

        // an empty block tells each parser that the input is over, they then hand over what they
        // have left
        for (int i = 0; i < parsers; i++) {
            blocks.put(new Block(new byte[0], 0));
        }
        for (Thread thread : parserThreads) {
            thread.join();
        }
        long sent = sentCounter.get();

        // an empty batch marks the end of the input for the instances
        for (BlockingQueue<List<Item>> queue : queues) {
            queue.put(List.of());
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
        System.out.println("Parsers: " + parsers);
        System.out.println("Sent: " + sent);
        System.out.println("Acked: " + collector.acked.get());
        System.out.println("Failed: " + collector.failed.get());
        System.out.println("Total time: " + elapsed + " msec");
        System.out.printf(
                Locale.ROOT, "Average OPS: %.2f%n", collector.acked.get() * 1000.0 / elapsed);

        // some backends leave non-daemon threads behind
        System.exit(collector.acked.get() == sent ? 0 : 1);
    }

    /** Lines of input, the first {@code length} bytes of {@code data} ending with a full line. */
    record Block(byte[] data, int length) {}

    /** Receives a line of input, without leading or trailing whitespace. */
    interface LineHandler {
        void line(byte[] data, int off, int len) throws InterruptedException;
    }

    /**
     * Reads the input and puts it on the queue as blocks of whole lines, of {@code blockSize} bytes
     * at most unless a line is longer than that, in which case the blocks grow to fit it. The last
     * line doesn't need to end with a line feed.
     */
    static void readBlocks(InputStream in, int blockSize, BlockingQueue<Block> blocks)
            throws IOException, InterruptedException {
        byte[] block = new byte[blockSize];
        int filled = 0;
        while (true) {
            filled += in.readNBytes(block, filled, block.length - filled);
            if (filled < block.length) {
                // end of the input
                if (filled > 0) {
                    blocks.put(new Block(block, filled));
                }
                return;
            }
            int end = lastLineEnd(block, filled);
            if (end == 0) {
                // a line longer than the block
                block = Arrays.copyOf(block, block.length * 2);
                continue;
            }
            // the incomplete line at the end goes to the next block
            byte[] next = new byte[block.length];
            System.arraycopy(block, end, next, 0, filled - end);
            filled -= end;
            blocks.put(new Block(block, end));
            block = next;
        }
    }

    /**
     * Passes each line of the block to the handler, trimmed as {@link String#trim()} does, which
     * also removes the carriage return of CRLF line endings. Blank lines are skipped.
     */
    static void forEachLine(Block block, LineHandler handler) throws InterruptedException {
        byte[] data = block.data();
        int lineStart = 0;
        while (lineStart < block.length()) {
            int lineEnd = lineStart;
            while (lineEnd < block.length() && data[lineEnd] != '\n') {
                lineEnd++;
            }
            int next = lineEnd + 1;
            while (lineStart < lineEnd && (data[lineStart] & 0xff) <= ' ') {
                lineStart++;
            }
            while (lineEnd > lineStart && (data[lineEnd - 1] & 0xff) <= ' ') {
                lineEnd--;
            }
            if (lineEnd > lineStart) {
                handler.line(data, lineStart, lineEnd - lineStart);
            }
            lineStart = next;
        }
    }

    /** Returns the position after the last line feed of the block or 0 if there is none. */
    private static int lastLineEnd(byte[] block, int length) {
        for (int i = length - 1; i >= 0; i--) {
            if (block[i] == '\n') {
                return i + 1;
            }
        }
        return 0;
    }

    /**
     * Parses the blocks of lines handed over by the reading thread and routes the URLs to the
     * instances of the bolt, until an empty block marks the end of the input.
     */
    private static void parseAll(
            BlockingQueue<Block> blocks, List<BlockingQueue<List<Item>>> queues, AtomicLong sent) {
        int instances = queues.size();
        List<List<Item>> batches = new ArrayList<>(instances);
        for (int i = 0; i < instances; i++) {
            batches.add(new ArrayList<>(HANDOFF_BATCH_SIZE));
        }
        // counted locally, the parsers would otherwise contend on the counter for every URL
        long[] routed = {0};
        try {
            while (true) {
                Block block = blocks.take();
                if (block.length() == 0) {
                    break;
                }
                forEachLine(
                        block,
                        (data, off, len) -> {
                            if (route(data, off, len, batches, queues)) {
                                routed[0]++;
                            }
                        });
            }
            // hand over what is left
            for (int i = 0; i < instances; i++) {
                if (!batches.get(i).isEmpty()) {
                    queues.get(i).put(batches.get(i));
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        sent.addAndGet(routed[0]);
    }

    /** Parses a line and adds it to the batch of its instance, returns false if it is invalid. */
    private static boolean route(
            byte[] data,
            int off,
            int len,
            List<List<Item>> batches,
            List<BlockingQueue<List<Item>>> queues)
            throws InterruptedException {
        Item item = parse(data, off, len);
        if (item == null) {
            System.err.println(
                    "Invalid input line: " + new String(data, off, len, StandardCharsets.UTF_8));
            return false;
        }
        // a URL always goes to the same instance, as with a fields grouping on the URL
        int target = Math.floorMod(item.url().hashCode(), queues.size());
        List<Item> batch = batches.get(target);
        batch.add(item);
        if (batch.size() == HANDOFF_BATCH_SIZE) {
            queues.get(target).put(batch);
            batches.set(target, new ArrayList<>(HANDOFF_BATCH_SIZE));
        }
        return true;
    }

    /**
     * Passes the URLs handed over by the parsers to one instance of the bolt, until an empty batch
     * marks the end of the input.
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

    private static InputStream openInput(String file) throws IOException {
        // the stream returned by Files is not buffered and GZIPInputStream inflates 512 bytes at a
        // time by default
        InputStream is =
                new BufferedInputStream(Files.newInputStream(Paths.get(file)), INPUT_BUFFER_SIZE);
        if (file.endsWith(".gz")) {
            is = new GZIPInputStream(is, INPUT_BUFFER_SIZE);
        }
        return is;
    }

    /** A URL to store, with the values to pass to the bolt. */
    record Item(String url, Status status, Metadata metadata, Optional<Date> nextFetch) {}

    /** Returns the item for a line of input or null if it is invalid. */
    static Item parse(String line) {
        byte[] bytes = line.getBytes(StandardCharsets.UTF_8);
        int start = 0;
        int end = bytes.length;
        while (start < end && (bytes[start] & 0xff) <= ' ') {
            start++;
        }
        while (end > start && (bytes[end - 1] & 0xff) <= ' ') {
            end--;
        }
        return parse(bytes, start, end - start);
    }

    /**
     * Returns the item for a line of input, given as UTF-8 bytes without leading or trailing
     * whitespace, or null if it is invalid. The JSON is read token by token rather than as a tree,
     * as only a few of its fields are needed.
     */
    static Item parse(byte[] buf, int off, int len) {
        if (len == 0) {
            return null;
        }
        if (buf[off] != '{') {
            String url = new String(buf, off, len, StandardCharsets.UTF_8);
            return new Item(url, Status.DISCOVERED, new Metadata(), Optional.of(new Date()));
        }

        boolean discovered = false;
        Entry entry = null;
        Entry known = null;
        try (JsonParser parser = JSON.createParser(buf, off, len)) {
            if (parser.nextToken() != JsonToken.START_OBJECT) {
                return null;
            }
            while (parser.nextToken() == JsonToken.FIELD_NAME) {
                String name = parser.currentName();
                JsonToken token = parser.nextToken();
                if ("discovered".equals(name)) {
                    discovered = true;
                    entry = token == JsonToken.START_OBJECT ? parseEntry(parser) : skip(parser);
                } else if ("known".equals(name)) {
                    known = token == JsonToken.START_OBJECT ? parseEntry(parser) : skip(parser);
                } else {
                    parser.skipChildren();
                }
            }
        } catch (IOException e) {
            return null;
        }

        // a discovered item takes precedence over a known one
        if (!discovered) {
            entry = known;
        }
        if (entry == null || entry.url == null) {
            return null;
        }

        if (discovered) {
            return new Item(entry.url, Status.DISCOVERED, entry.metadata, Optional.of(new Date()));
        }

        Optional<Date> nextFetch =
                entry.refetch > 0
                        ? Optional.of(Date.from(Instant.ofEpochSecond(entry.refetch)))
                        : Optional.empty();
        return new Item(entry.url, Status.FETCHED, entry.metadata, nextFetch);
    }

    /** The fields of a discovered or known item, read from its JSON object. */
    private static final class Entry {
        String url;
        final Metadata metadata = new Metadata();
        long refetch;
        boolean refetchCamelCase;
    }

    private static Entry skip(JsonParser parser) throws IOException {
        parser.skipChildren();
        return null;
    }

    /** Reads a discovered or known item, the parser being on the start of its object. */
    private static Entry parseEntry(JsonParser parser) throws IOException {
        Entry entry = new Entry();
        while (parser.nextToken() == JsonToken.FIELD_NAME) {
            String name = parser.currentName();
            JsonToken token = parser.nextToken();
            switch (name) {
                case "info" -> {
                    if (token == JsonToken.START_OBJECT) {
                        parseInfo(parser, entry);
                    } else {
                        parser.skipChildren();
                    }
                }
                // either name is accepted, the camel case one takes precedence
                case "refetchableFromDate" -> {
                    entry.refetch = readLong(parser, token);
                    entry.refetchCamelCase = true;
                }
                case "refetchable_from_date" -> {
                    long refetch = readLong(parser, token);
                    if (!entry.refetchCamelCase) {
                        entry.refetch = refetch;
                    }
                }
                default -> parser.skipChildren();
            }
        }
        return entry;
    }

    /** Reads the URL and metadata of an item, the parser being on the start of the info object. */
    private static void parseInfo(JsonParser parser, Entry entry) throws IOException {
        while (parser.nextToken() == JsonToken.FIELD_NAME) {
            String name = parser.currentName();
            JsonToken token = parser.nextToken();
            if ("url".equals(name)) {
                entry.url = token == JsonToken.VALUE_STRING ? parser.getText() : null;
                parser.skipChildren();
            } else if ("metadata".equals(name) && token == JsonToken.START_OBJECT) {
                parseMetadata(parser, entry.metadata);
            } else {
                parser.skipChildren();
            }
        }
    }

    /** Reads the metadata, the parser being on the start of their object. */
    private static void parseMetadata(JsonParser parser, Metadata metadata) throws IOException {
        while (parser.nextToken() == JsonToken.FIELD_NAME) {
            String key = parser.currentName();
            if (parser.nextToken() != JsonToken.START_OBJECT) {
                parser.skipChildren();
                continue;
            }
            while (parser.nextToken() == JsonToken.FIELD_NAME) {
                String name = parser.currentName();
                JsonToken token = parser.nextToken();
                if (!"values".equals(name) || token != JsonToken.START_ARRAY) {
                    parser.skipChildren();
                    continue;
                }
                while ((token = parser.nextToken()) != JsonToken.END_ARRAY) {
                    if (token.isStructStart()) {
                        parser.skipChildren();
                    } else {
                        metadata.addValue(key, parser.getText());
                    }
                }
            }
        }
    }

    /** int64 values can be written as strings, anything that is not a number is 0. */
    private static long readLong(JsonParser parser, JsonToken token) throws IOException {
        switch (token) {
            case VALUE_NUMBER_INT, VALUE_NUMBER_FLOAT:
                return parser.getValueAsLong();
            case VALUE_STRING:
                try {
                    return Long.parseLong(parser.getText().trim());
                } catch (NumberFormatException e) {
                    return 0;
                }
            default:
                parser.skipChildren();
                return 0;
        }
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

    /**
     * Acks the URLs without storing them, selected with {@code baseline} as class name. Gives the
     * throughput of the utility itself, against which the results of the real backends can be
     * normalised so that they are comparable across machines.
     */
    static class BaselineStatusUpdater extends AbstractStatusUpdaterBolt {

        @Override
        public void store(
                String url, Status status, Metadata metadata, Optional<Date> nextFetch, Tuple t)
                throws Exception {
            ack(t, url);
        }
    }
}
