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

package org.apache.stormcrawler.metrics;

import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.storm.metric.api.IMetric;
import org.apache.storm.metric.api.MeanReducer;
import org.apache.storm.metric.api.MultiCountMetric;
import org.apache.storm.metric.api.MultiReducedMetric;
import org.apache.storm.metric.api.ReducedMetric;
import org.apache.storm.task.TopologyContext;
import org.apache.stormcrawler.metrics.v1.V1CounterMetric;
import org.apache.stormcrawler.metrics.v1.V1ReducedMetric;
import org.apache.stormcrawler.metrics.v2.V2CounterMetric;
import org.apache.stormcrawler.metrics.v2.V2HistogramReducedMetric;
import org.apache.stormcrawler.metrics.v2.V2MeterReducedMetric;
import org.apache.stormcrawler.util.CollectionMetric;
import org.apache.stormcrawler.util.ConfUtils;
import org.apache.stormcrawler.util.PerSecondReducer;

/**
 * Factory for creating metric instances that route to Storm V1, V2, or both metric APIs. The
 * metrics version is controlled by the configuration property {@value #METRICS_VERSION_KEY}.
 *
 * <ul>
 *   <li>{@code "v1"} (default) — uses the legacy Storm V1 metrics API
 *   <li>{@code "v2"} — uses the Codahale/Dropwizard V2 metrics API
 *   <li>{@code "both"} — registers with both V1 and V2 simultaneously for parallel operation during
 *       migration
 * </ul>
 */
public final class CrawlerMetrics {

    /** Configuration key for selecting the metrics version: "v1", "v2", or "both". */
    public static final String METRICS_VERSION_KEY = "stormcrawler.metrics.version";

    private static final String VERSION_V1 = "v1";
    private static final String VERSION_V2 = "v2";
    private static final String VERSION_BOTH = "both";

    private CrawlerMetrics() {}

    /**
     * Registers a scoped counter metric.
     *
     * @param context the Storm topology context
     * @param stormConf the Storm configuration map
     * @param name the metric name (e.g. "fetcher_counter")
     * @param timeBucketSecs the V1 reporting interval in seconds (ignored for V2-only mode)
     * @return a {@link ScopedCounter} that can be used via {@code scope("x").incrBy(n)}
     */
    public static ScopedCounter registerCounter(
            TopologyContext context,
            Map<String, Object> stormConf,
            String name,
            int timeBucketSecs) {

        String version = getVersion(stormConf);

        switch (version) {
            case VERSION_V2:
                return new V2CounterMetric(name, context);

            case VERSION_BOTH:
                ScopedCounter v1 = createV1Counter(context, name, timeBucketSecs);
                ScopedCounter v2 = new V2CounterMetric(name, context);
                return new DualCounterMetric(v1, v2);

            case VERSION_V1:
            default:
                return createV1Counter(context, name, timeBucketSecs);
        }
    }

    /**
     * Registers a scoped reduced metric backed by a {@link MeanReducer}. In V2 mode, this uses a
     * Codahale {@link com.codahale.metrics.Histogram}.
     *
     * @param context the Storm topology context
     * @param stormConf the Storm configuration map
     * @param name the metric name (e.g. "fetcher_average")
     * @param timeBucketSecs the V1 reporting interval in seconds (ignored for V2-only mode)
     * @return a {@link ScopedReducedMetric} that can be used via {@code scope("x").update(val)}
     */
    public static ScopedReducedMetric registerMeanMetric(
            TopologyContext context,
            Map<String, Object> stormConf,
            String name,
            int timeBucketSecs) {

        String version = getVersion(stormConf);

        switch (version) {
            case VERSION_V2:
                return new V2HistogramReducedMetric(name, context);

            case VERSION_BOTH:
                ScopedReducedMetric v1 = createV1MeanMetric(context, name, timeBucketSecs);
                ScopedReducedMetric v2 = new V2HistogramReducedMetric(name, context);
                return new DualReducedMetric(v1, v2);

            case VERSION_V1:
            default:
                return createV1MeanMetric(context, name, timeBucketSecs);
        }
    }

    /**
     * Registers a scoped reduced metric backed by a {@link PerSecondReducer}. In V2 mode, this uses
     * a Codahale {@link com.codahale.metrics.Meter} which natively tracks rate per second.
     *
     * @param context the Storm topology context
     * @param stormConf the Storm configuration map
     * @param name the metric name (e.g. "fetcher_average_persec")
     * @param timeBucketSecs the V1 reporting interval in seconds (ignored for V2-only mode)
     * @return a {@link ScopedReducedMetric} that can be used via {@code scope("x").update(val)}
     */
    public static ScopedReducedMetric registerPerSecMetric(
            TopologyContext context,
            Map<String, Object> stormConf,
            String name,
            int timeBucketSecs) {

        String version = getVersion(stormConf);

        switch (version) {
            case VERSION_V2:
                return new V2MeterReducedMetric(name, context);

            case VERSION_BOTH:
                ScopedReducedMetric v1 = createV1PerSecMetric(context, name, timeBucketSecs);
                ScopedReducedMetric v2 = new V2MeterReducedMetric(name, context);
                return new DualReducedMetric(v1, v2);

            case VERSION_V1:
            default:
                return createV1PerSecMetric(context, name, timeBucketSecs);
        }
    }

    /**
     * Registers a gauge metric.
     *
     * @param context the Storm topology context
     * @param stormConf the Storm configuration map
     * @param name the metric name (e.g. "activethreads")
     * @param supplier a supplier providing the current gauge value
     * @param timeBucketSecs the V1 reporting interval in seconds (ignored for V2-only mode)
     * @param <T> the gauge value type
     */
    public static <T> void registerGauge(
            TopologyContext context,
            Map<String, Object> stormConf,
            String name,
            Supplier<T> supplier,
            int timeBucketSecs) {

        String version = getVersion(stormConf);

        boolean registerV1 = VERSION_V1.equals(version) || VERSION_BOTH.equals(version);
        boolean registerV2 = VERSION_V2.equals(version) || VERSION_BOTH.equals(version);

        if (registerV1) {
            context.registerMetric(
                    name,
                    new IMetric() {
                        @Override
                        public Object getValueAndReset() {
                            return supplier.get();
                        }
                    },
                    timeBucketSecs);
        }

        if (registerV2) {
            context.registerGauge(name, supplier::get);
        }
    }

    /**
     * Registers a single (non-scoped) mean metric, e.g. for tracking average processing time. In V1
     * mode, this uses a {@link ReducedMetric} with {@link MeanReducer}. In V2 mode, this uses a
     * Codahale {@link com.codahale.metrics.Histogram}.
     *
     * @param context the Storm topology context
     * @param stormConf the Storm configuration map
     * @param name the metric name (e.g. "sitemap_average_processing_time")
     * @param timeBucketSecs the V1 reporting interval in seconds (ignored for V2-only mode)
     * @return a {@link Consumer} that accepts numeric values to update the metric
     */
    public static Consumer<Number> registerSingleMeanMetric(
            TopologyContext context,
            Map<String, Object> stormConf,
            String name,
            int timeBucketSecs) {

        String version = getVersion(stormConf);
        boolean useV1 = VERSION_V1.equals(version) || VERSION_BOTH.equals(version);
        boolean useV2 = VERSION_V2.equals(version) || VERSION_BOTH.equals(version);

        Consumer<Number> v1Consumer = null;
        Consumer<Number> v2Consumer = null;

        if (useV1) {
            ReducedMetric metric =
                    context.registerMetric(
                            name, new ReducedMetric(new MeanReducer()), timeBucketSecs);
            v1Consumer = value -> metric.update(value);
        }

        if (useV2) {
            com.codahale.metrics.Histogram histogram = context.registerHistogram(name);
            v2Consumer = value -> histogram.update(value.longValue());
        }

        if (useV1 && useV2) {
            final Consumer<Number> f1 = v1Consumer;
            final Consumer<Number> f2 = v2Consumer;
            return value -> {
                f1.accept(value);
                f2.accept(value);
            };
        }
        return useV2 ? v2Consumer : v1Consumer;
    }

    /**
     * Registers a collection metric for tracking timing measurements. In V1 mode, this uses {@link
     * CollectionMetric}. In V2 mode, this uses a Codahale {@link com.codahale.metrics.Histogram}.
     *
     * @param context the Storm topology context
     * @param stormConf the Storm configuration map
     * @param name the metric name (e.g. "spout_query_time_msec")
     * @param timeBucketSecs the V1 reporting interval in seconds (ignored for V2-only mode)
     * @return a {@link Consumer} that accepts long measurement values
     */
    public static Consumer<Long> registerCollectionMetric(
            TopologyContext context,
            Map<String, Object> stormConf,
            String name,
            int timeBucketSecs) {

        String version = getVersion(stormConf);
        boolean useV1 = VERSION_V1.equals(version) || VERSION_BOTH.equals(version);
        boolean useV2 = VERSION_V2.equals(version) || VERSION_BOTH.equals(version);

        Consumer<Long> v1Consumer = null;
        Consumer<Long> v2Consumer = null;

        if (useV1) {
            CollectionMetric metric = new CollectionMetric();
            context.registerMetric(name, metric, timeBucketSecs);
            v1Consumer = metric::addMeasurement;
        }

        if (useV2) {
            com.codahale.metrics.Histogram histogram = context.registerHistogram(name);
            v2Consumer = histogram::update;
        }

        if (useV1 && useV2) {
            final Consumer<Long> f1 = v1Consumer;
            final Consumer<Long> f2 = v2Consumer;
            return value -> {
                f1.accept(value);
                f2.accept(value);
            };
        }
        return useV2 ? v2Consumer : v1Consumer;
    }

    private static ScopedCounter createV1Counter(
            TopologyContext context, String name, int timeBucketSecs) {
        MultiCountMetric metric =
                context.registerMetric(name, new MultiCountMetric(), timeBucketSecs);
        return new V1CounterMetric(metric);
    }

    private static ScopedReducedMetric createV1MeanMetric(
            TopologyContext context, String name, int timeBucketSecs) {
        MultiReducedMetric metric =
                context.registerMetric(
                        name, new MultiReducedMetric(new MeanReducer()), timeBucketSecs);
        return new V1ReducedMetric(metric);
    }

    private static ScopedReducedMetric createV1PerSecMetric(
            TopologyContext context, String name, int timeBucketSecs) {
        MultiReducedMetric metric =
                context.registerMetric(
                        name, new MultiReducedMetric(new PerSecondReducer()), timeBucketSecs);
        return new V1ReducedMetric(metric);
    }

    private static String getVersion(Map<String, Object> stormConf) {
        return ConfUtils.getString(stormConf, METRICS_VERSION_KEY, VERSION_V1);
    }
}
