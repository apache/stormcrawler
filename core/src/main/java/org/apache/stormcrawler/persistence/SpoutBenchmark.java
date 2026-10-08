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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.storm.Config;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.metrics2.StormMetricRegistry;
import org.apache.storm.spout.ISpoutOutputCollector;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.GeneralTopologyContext;
import org.apache.storm.task.IOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.TupleImpl;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.apache.stormcrawler.Constants;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.util.ConfUtils;

/**
 * Simple utility for measuring the read performance of a spout and of the backend it queries,
 * without running a topology. The backend is expected to have been populated beforehand, e.g. with
 * {@link StatusUpdaterBenchmark}. One or more instances of the spout and of a status updater bolt
 * are created from their class names and prepared with the configuration. Every URL emitted by the
 * spouts is passed straight to the status updaters as FETCHED, as if it had been fetched
 * successfully, so that it is rescheduled in the backend and acked to the spout, which frees its
 * slot and lets it query further. The utility runs for a given number of minutes, then reports the
 * number of URLs emitted and the number of unique ones among them; only the warnings and errors are
 * logged so that the reports stay readable.
 *
 * <p>The number of unique URLs is what matters: a spout which emits the same URLs again, e.g.
 * because the backend has not made the updates visible yet by the time the URLs leave the purgatory
 * of the spout ({@code spout.ttl.purgatory}), has a higher number of emitted URLs but does not do
 * more useful work. The unique URLs are kept in memory, the heap must be large enough for them.
 *
 * <p>The tuples are given to the status updaters with {@code execute}, as they would be in a crawl,
 * so the next fetch date is computed by the scheduler, with {@code fetchInterval.default} by
 * default. That interval should be longer than the duration of the run so that the URLs are not due
 * again before it ends.
 *
 * <p>As Storm would give each of them its own executor, every instance of the spout and of the
 * status updater has its own task and is called from its own thread. A spout instance is only
 * called from its thread, for {@code nextTuple} as well as for {@code ack} and {@code fail}, and is
 * not asked for more tuples while it has {@code topology.max.spout.pending} of them in flight, if
 * that value is set. The URLs are routed to the status updaters based on their hash, as a fields
 * grouping on the URL does. Unlike in Storm, the tuples never time out.
 *
 * <p>The arguments are, in this order, the class name of the spout, the class name of the status
 * updater bolt, the duration of the run in minutes, optionally the number of instances of the spout
 * then of the status updater (1 by default), and one or more configuration files, which are loaded
 * after crawler-default.yaml. The arguments are positional as the {@code storm} command would take
 * options such as {@code -c} for its own.
 *
 * <p>Once the time is up, the spouts are not asked for more tuples and the utility waits for the
 * ones in flight to be acked or failed before closing the components. Only the URLs emitted during
 * the run are counted.
 *
 * <p>As with {@link StatusUpdaterBenchmark}, the utility must be run with {@code storm local}, from
 * the jar of a crawl project which has the dependencies for the modules of the spout and the bolt.
 *
 * <pre>
 * storm local target/crawler-1.0-SNAPSHOT.jar \
 *   org.apache.stormcrawler.persistence.SpoutBenchmark \
 *   org.apache.stormcrawler.opensearch.persistence.AggregationSpout \
 *   org.apache.stormcrawler.opensearch.persistence.StatusUpdaterBolt 10 4 4 \
 *   backend.yaml
 * </pre>
 */
public class SpoutBenchmark {

    private static final String SPOUT = "spout";
    private static final String SOURCE = "fetcher";
    private static final int SOURCE_TASK = 0;
    private static final String BOLT = "status";

    /** number of tuples waiting for each instance of the bolt */
    private static final int HANDOFF_QUEUE_SIZE = 10000;

    /** how long to wait for the acks once the time is up */
    private static final long ACK_TIMEOUT_MSEC = 5 * 60 * 1000;

    /** how often the intermediate throughput is displayed */
    private static final long REPORT_INTERVAL_SEC = 60;

    /** marks the end of the input for an instance of the bolt */
    private static final Emitted END = new Emitted(null, null, null, null, null);

    private static final AtomicBoolean running = new AtomicBoolean(true);

    private static final Set<String> unique = ConcurrentHashMap.newKeySet();
    private static final AtomicLong emitted = new AtomicLong();
    private static final AtomicLong acked = new AtomicLong();
    private static final AtomicLong failed = new AtomicLong();

    public static void main(String[] args) throws Exception {
        if (args.length < 4 || !args[2].matches("\\d+")) {
            usage();
        }
        int minutes = Integer.parseInt(args[2]);
        // the numbers of instances are optional
        int spoutInstances = 1;
        int boltInstances = 1;
        int firstConf = 3;
        if (args[firstConf].matches("\\d+")) {
            spoutInstances = Integer.parseInt(args[firstConf++]);
            if (firstConf < args.length && args[firstConf].matches("\\d+")) {
                boltInstances = Integer.parseInt(args[firstConf++]);
            }
        }
        if (firstConf >= args.length || minutes < 1 || spoutInstances < 1 || boltInstances < 1) {
            usage();
        }

        // log4j is what storm local uses, the INFO messages of the components would drown the
        // reports
        Configurator.setAllLevels(LogManager.ROOT_LOGGER_NAME, Level.WARN);

        Config conf = new Config();
        conf.putAll(
                ConfUtils.extractConfigElement(
                        Utils.findAndReadConfigFile("crawler-default.yaml", false)));
        for (int i = firstConf; i < args.length; i++) {
            ConfUtils.loadConf(args[i], conf);
        }
        int maxPending = ConfUtils.getInt(conf, Config.TOPOLOGY_MAX_SPOUT_PENDING, -1);

        // the task of the fetcher is 0, then come the spouts and the bolts
        List<Integer> spoutTasks = IntStream.rangeClosed(1, spoutInstances).boxed().toList();
        List<Integer> boltTasks =
                IntStream.rangeClosed(spoutInstances + 1, spoutInstances + boltInstances)
                        .boxed()
                        .toList();
        Map<String, List<Integer>> componentToTasks =
                Map.of(SOURCE, List.of(SOURCE_TASK), SPOUT, spoutTasks, BOLT, boltTasks);

        List<IRichSpout> spouts = new ArrayList<>(spoutInstances);
        List<AbstractStatusUpdaterBolt> bolts = new ArrayList<>(boltInstances);
        List<BlockingQueue<Emitted>> queues = new ArrayList<>(boltInstances);
        List<Thread> spoutThreads = new ArrayList<>(spoutInstances);
        List<Thread> boltThreads = new ArrayList<>(boltInstances);

        BoltCollector boltCollector = new BoltCollector();
        for (int task : boltTasks) {
            AbstractStatusUpdaterBolt bolt =
                    Class.forName(args[1])
                            .asSubclass(AbstractStatusUpdaterBolt.class)
                            .getDeclaredConstructor()
                            .newInstance();
            TopologyContext context = createContext(conf, task, componentToTasks);
            bolt.prepare(conf, context, new OutputCollector(boltCollector));
            BlockingQueue<Emitted> queue = new ArrayBlockingQueue<>(HANDOFF_QUEUE_SIZE);
            Thread thread =
                    new Thread(
                            () -> executeAll(bolt, context, queue), "SpoutBenchmark-bolt-" + task);
            // must not keep the JVM alive if something goes wrong
            thread.setDaemon(true);
            bolts.add(bolt);
            queues.add(queue);
            boltThreads.add(thread);
        }

        for (int task : spoutTasks) {
            IRichSpout spout =
                    Class.forName(args[0])
                            .asSubclass(IRichSpout.class)
                            .getDeclaredConstructor()
                            .newInstance();
            SpoutCollector spoutCollector = new SpoutCollector(queues, boltTasks);
            spout.open(
                    conf,
                    createContext(conf, task, componentToTasks),
                    new SpoutOutputCollector(spoutCollector));
            spout.activate();
            Thread thread =
                    new Thread(
                            () -> runSpout(spout, spoutCollector, maxPending),
                            "SpoutBenchmark-spout-" + task);
            thread.setDaemon(true);
            spouts.add(spout);
            spoutThreads.add(thread);
        }

        long start = System.currentTimeMillis();
        boltThreads.forEach(Thread::start);
        spoutThreads.forEach(Thread::start);

        // reports the throughput at regular intervals
        ScheduledExecutorService reporter =
                Executors.newSingleThreadScheduledExecutor(
                        r -> {
                            Thread t = new Thread(r, "SpoutBenchmark-reporter");
                            t.setDaemon(true);
                            return t;
                        });
        AtomicLong lastUnique = new AtomicLong();
        AtomicLong lastReport = new AtomicLong(start);
        reporter.scheduleAtFixedRate(
                () -> {
                    long now = System.currentTimeMillis();
                    long elapsed = now - lastReport.getAndSet(now);
                    long current = unique.size();
                    long delta = current - lastUnique.getAndSet(current);
                    System.out.printf(
                            Locale.ROOT,
                            "Emitted: %d - Unique: %d - Acked: %d - Unique URLs/sec over the last"
                                    + " %d sec: %.2f%n",
                            emitted.get(),
                            current,
                            acked.get(),
                            Math.round(elapsed / 1000.0),
                            delta * 1000.0 / Math.max(1, elapsed));
                },
                REPORT_INTERVAL_SEC,
                REPORT_INTERVAL_SEC,
                TimeUnit.SECONDS);

        Thread.sleep(TimeUnit.MINUTES.toMillis(minutes));

        // the spouts are not asked for more tuples from now on
        running.set(false);
        long elapsed = System.currentTimeMillis() - start;
        long totalEmitted = emitted.get();
        long totalUnique = unique.size();
        reporter.shutdownNow();

        // the spouts wait for the tuples in flight to be acked or failed
        for (Thread thread : spoutThreads) {
            thread.join();
        }
        for (BlockingQueue<Emitted> queue : queues) {
            queue.put(END);
        }
        for (Thread thread : boltThreads) {
            thread.join();
        }

        for (AbstractStatusUpdaterBolt bolt : bolts) {
            bolt.cleanup();
        }
        for (IRichSpout spout : spouts) {
            spout.deactivate();
            spout.close();
        }

        System.out.println("Spout instances: " + spoutInstances);
        System.out.println("Bolt instances: " + boltInstances);
        System.out.println("Max spout pending: " + (maxPending > 0 ? maxPending : "none"));
        System.out.println("Total time: " + elapsed + " msec");
        System.out.println("Emitted: " + totalEmitted);
        System.out.println("Unique: " + totalUnique);
        System.out.println("Acked: " + acked.get());
        System.out.println("Failed: " + failed.get());
        System.out.printf(
                Locale.ROOT, "Average unique URLs/sec: %.2f%n", totalUnique * 1000.0 / elapsed);

        // some backends leave non-daemon threads behind
        System.exit(0);
    }

    private static void usage() {
        System.err.println(
                "Usage: SpoutBenchmark <spout class> <bolt class> <minutes> [<spout instances>"
                        + " [<bolt instances>]] <conf file>...");
        System.exit(1);
    }

    /**
     * Calls one instance of the spout until the time is up, then waits for its tuples in flight to
     * be acked or failed. The acks and fails are passed to the spout from this thread, as Storm
     * does.
     */
    private static void runSpout(IRichSpout spout, SpoutCollector collector, int maxPending) {
        long deadline = Long.MAX_VALUE;
        try {
            while (true) {
                Ack ack;
                while ((ack = collector.acks.poll()) != null) {
                    collector.pending--;
                    if (ack.ok()) {
                        spout.ack(ack.msgId());
                    } else {
                        spout.fail(ack.msgId());
                    }
                }
                if (!running.get()) {
                    if (deadline == Long.MAX_VALUE) {
                        deadline = System.currentTimeMillis() + ACK_TIMEOUT_MSEC;
                    }
                    if (collector.pending == 0 || System.currentTimeMillis() > deadline) {
                        return;
                    }
                } else if (maxPending <= 0 || collector.pending < maxPending) {
                    spout.nextTuple();
                    continue;
                }
                // nothing to do until a tuple is acked or failed
                ack = collector.acks.poll(10, TimeUnit.MILLISECONDS);
                if (ack != null) {
                    collector.acks.add(ack);
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Passes the URLs emitted by the spouts to one instance of the bolt as FETCHED, until the end
     * of the run.
     */
    private static void executeAll(
            AbstractStatusUpdaterBolt bolt, TopologyContext context, BlockingQueue<Emitted> queue) {
        try {
            while (true) {
                Emitted item = queue.take();
                if (item == END) {
                    return;
                }
                Tuple tuple =
                        new AnchoredTuple(
                                context,
                                new Values(item.url(), item.metadata(), item.status()),
                                item.spout(),
                                item.msgId());
                try {
                    bolt.execute(tuple);
                } catch (Exception e) {
                    System.err.println("Exception caught when storing " + item.url() + ": " + e);
                    bolt.collector.fail(tuple);
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /** Context of one of the tasks, all of them running in the same worker. */
    private static TopologyContext createContext(
            Map<String, Object> conf, int task, Map<String, List<Integer>> componentToTasks) {
        Map<Integer, String> taskToComponent = new HashMap<>();
        componentToTasks.forEach(
                (component, tasks) -> tasks.forEach(t -> taskToComponent.put(t, component)));
        Fields statusFields = new Fields("url", "metadata", "status");
        return new TopologyContext(
                new StormTopology(),
                conf,
                taskToComponent,
                componentToTasks,
                Map.of(
                        SOURCE,
                        Map.of(Utils.DEFAULT_STREAM_ID, statusFields),
                        SPOUT,
                        Map.of(
                                Utils.DEFAULT_STREAM_ID,
                                new Fields("url", "metadata"),
                                Constants.StatusStreamName,
                                statusFields)),
                new HashMap<>(),
                "spout-benchmark",
                null,
                null,
                task,
                0,
                componentToTasks.get(taskToComponent.get(task)),
                new HashMap<>(),
                new HashMap<>(),
                new HashMap<>(),
                new HashMap<>(),
                new AtomicBoolean(false),
                new StormMetricRegistry());
    }

    /** A tuple emitted by a spout, with what is needed to ack or fail it. */
    private record Emitted(
            String url, Metadata metadata, Status status, SpoutCollector spout, Object msgId) {}

    private record Ack(Object msgId, boolean ok) {}

    /** A tuple given to the bolt, which knows which spout to ack or fail it to. */
    private static class AnchoredTuple extends TupleImpl {

        final SpoutCollector spout;
        final Object msgId;

        AnchoredTuple(
                GeneralTopologyContext context,
                List<Object> values,
                SpoutCollector spout,
                Object msgId) {
            super(context, values, SOURCE, SOURCE_TASK, Utils.DEFAULT_STREAM_ID);
            this.spout = spout;
            this.msgId = msgId;
        }
    }

    /**
     * Collector of one instance of the spout. The URLs on the default stream are counted and handed
     * to the bolts as FETCHED; the ones on the status stream, which the spout refuses to emit, are
     * handed over with their status and are not tracked.
     */
    private static class SpoutCollector implements ISpoutOutputCollector {

        private final List<BlockingQueue<Emitted>> queues;
        private final List<Integer> boltTasks;

        /** acks and fails from the bolts, passed to the spout by its own thread */
        final BlockingQueue<Ack> acks = new LinkedBlockingQueue<>();

        /** only used by the thread of the spout */
        int pending;

        SpoutCollector(List<BlockingQueue<Emitted>> queues, List<Integer> boltTasks) {
            this.queues = queues;
            this.boltTasks = boltTasks;
        }

        @Override
        public List<Integer> emit(String streamId, List<Object> tuple, Object messageId) {
            String url = tuple.get(0).toString();
            Metadata metadata = (Metadata) tuple.get(1);
            Emitted item;
            if (Utils.DEFAULT_STREAM_ID.equals(streamId)) {
                emitted.incrementAndGet();
                unique.add(url);
                if (messageId != null) {
                    pending++;
                }
                item = new Emitted(url, metadata, Status.FETCHED, this, messageId);
            } else if (Constants.StatusStreamName.equals(streamId)) {
                item = new Emitted(url, metadata, (Status) tuple.get(2), null, null);
            } else {
                return Collections.emptyList();
            }
            // a URL always goes to the same instance, as with a fields grouping on the URL
            int target = Math.floorMod(url.hashCode(), queues.size());
            try {
                queues.get(target).put(item);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return Collections.emptyList();
            }
            return List.of(boltTasks.get(target));
        }

        @Override
        public void emitDirect(int taskId, String streamId, List<Object> tuple, Object messageId) {}

        @Override
        public long getPendingCount() {
            return pending;
        }

        @Override
        public void flush() {}

        @Override
        public void reportError(Throwable error) {
            error.printStackTrace();
        }
    }

    /** Counts the acks and fails of the bolts, which can come from any thread. */
    private static class BoltCollector implements IOutputCollector {

        @Override
        public void ack(Tuple input) {
            notifySpout(input, true);
        }

        @Override
        public void fail(Tuple input) {
            notifySpout(input, false);
        }

        /** the tuples from the status stream of the spouts are not tracked */
        private static void notifySpout(Tuple input, boolean ok) {
            if (input instanceof AnchoredTuple t && t.spout != null && t.msgId != null) {
                (ok ? acked : failed).incrementAndGet();
                t.spout.acks.add(new Ack(t.msgId, ok));
            }
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
