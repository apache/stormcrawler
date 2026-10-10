# stormcrawler-prometheus

A [Storm V2 metrics](https://storm.apache.org/releases/current/metrics_v2.html) reporter which serves the metrics of each worker over HTTP for [Prometheus](https://prometheus.io/) to scrape.

Unlike the generic JMX or Graphite reporters, it uses the dimensions Storm keeps for each task as labels, so a metric reported by several tasks ends up in one family:

```
fetcher_counter_total{topology="crawl",host="worker1",port="6700",component="fetcher",task="3",scope="status_200"} 1234.0
status_count{topology="crawl",host="worker1",port="6700",component="status",task="4",key="FETCHED"} 56789.0
storm_ack_count_total{topology="crawl",host="worker1",port="6700",component="status",task="4",source_component="spout",stream="default"} 42.0
```

## Configuration

Add the dependency to your topology and set the following in the configuration:

```yaml
  # StormCrawler's metrics must be registered with the V2 API
  stormcrawler.metrics.version: "v2"

  topology.metrics.reporters:
    - class: "org.apache.stormcrawler.prometheus.PrometheusReporter"
      # each worker listens on its port + offset, e.g. 7700 for 6700
      port.offset: 1000
```

| Key | Default | Description |
|---|---|---|
| `port` | | Port to listen on, takes precedence over `port.offset`. Used as is in local mode, where it defaults to 9400. |
| `port.offset` | 1000 | Added to the worker port to get the port to listen on. |
| `host` | all interfaces | Address to bind to. |
| `jvm.metrics` | true | Also expose the JVM metrics (memory, GC, threads, `process_start_time_seconds`). |
| `filter` | | Storm metrics filter, as for the other V2 reporters. |

The workers of a local cluster share a single endpoint.

## Naming

* Scoped metrics (`name.scope`) become the family `name` with a `scope` label. Only the first dot separates the scope, so `fetcher_counter.robots.fetched` has `scope="robots.fetched"`.
* Gauges whose value is a map, such as `status.count`, become one family named after the whole metric (`status_count`) with a `key` label per entry.
* Storm's built-in metrics (`__ack-count-spout:default`) become `storm_ack_count` with `source_component` and `stream` labels. The `.m1_rate` gauges Storm registers next to its counters are skipped, use `rate()` on the counter instead.
* Counters and meters are exposed as counters, so `fetcher_average_persec.bytes_fetched_perSec` becomes `fetcher_average_persec_total{scope="bytes_fetched_perSec"}` and `rate()` gives the bytes per second.
* Histograms and timers are exposed as summaries plus a `_max` and a `_mean` gauge. Timers are in seconds.

## Prometheus

Scrape every worker port of every supervisor, e.g. with 4 slots per supervisor:

```yaml
scrape_configs:
  - job_name: stormcrawler
    static_configs:
      - targets: ["supervisor1:7700", "supervisor1:7701", "supervisor1:7702", "supervisor1:7703"]
```

A target is only up while a worker runs on the corresponding slot.
