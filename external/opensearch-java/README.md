stormcrawler-opensearch-java
===========================

A collection of resources for [OpenSearch](https://opensearch.org/) built on the
[OpenSearch Java Client 3.x](https://opensearch.org/docs/latest/clients/java/) and
Apache HttpClient 5:

* [IndexerBolt](https://github.com/apache/stormcrawler/blob/master/external/opensearch-java/src/main/java/org/apache/stormcrawler/opensearch/bolt/IndexerBolt.java) for indexing documents crawled with StormCrawler
* [Spouts](https://github.com/apache/stormcrawler/blob/master/external/opensearch-java/src/main/java/org/apache/stormcrawler/opensearch/persistence/AggregationSpout.java) and [StatusUpdaterBolt](https://github.com/apache/stormcrawler/blob/master/external/opensearch-java/src/main/java/org/apache/stormcrawler/opensearch/persistence/StatusUpdaterBolt.java) for persisting URL information in recursive crawls
* [MetricsConsumer](https://github.com/apache/stormcrawler/blob/master/external/opensearch-java/src/main/java/org/apache/stormcrawler/opensearch/metrics/MetricsConsumer.java)
* [StatusMetricsBolt](https://github.com/apache/stormcrawler/blob/master/external/opensearch-java/src/main/java/org/apache/stormcrawler/opensearch/metrics/StatusMetricsBolt.java) for sending the breakdown of URLs per status as metrics and display its evolution over time.

This module is functionally equivalent to the legacy `external/opensearch` module
(which is based on the deprecated `RestHighLevelClient` and HttpClient 4), but
uses the typed `OpenSearchClient` and the `ApacheHttpClient5TransportBuilder`
transport. Unlike the legacy client, the Java Client 3.x no longer ships a
sniffer nor a built-in `BulkProcessor`; this module provides an internal
`AsyncBulkProcessor` that preserves the same semantics (size/count/time based
flushing, back-pressure, listener callbacks).

Getting started
---------------------

Add the dependency to your crawler project:

```xml
<dependency>
    <groupId>org.apache.stormcrawler</groupId>
    <artifactId>stormcrawler-opensearch-java</artifactId>
    <version>${stormcrawler.version}</version>
</dependency>
```

You will of course need to have both Storm and OpenSearch installed. For the
latter, see the [OpenSearch documentation](https://opensearch.org/docs/latest/install-and-configure/install-opensearch/docker/)
for Docker-based setups.

Schemas are automatically created by the bolts on first use; you can override
them by providing your own index definitions before starting the topology.

Configuration and dashboards
---------------------

For a ready-to-use crawler configuration, example Flux topologies, index
initialization scripts and OpenSearch Dashboards exports, refer to the
[`external/opensearch`](../opensearch) module: all of those resources are
compatible with this module and have not been duplicated here.

Differences from the legacy `external/opensearch` module
---------------------

* `opensearch.<bolt>.responseBufferSize` is no longer supported. The legacy
  module used the HC4-based low-level REST client and set a heap response
  buffer via `HeapBufferedResponseConsumerFactory`. The HC5-based async
  transport used here does not expose an equivalent per-request override, so
  the key is ignored. A `WARN` is logged at startup if it is found in the
  configuration; remove it when migrating.
* `opensearch.<bolt>.sniff` is no longer supported. The legacy module enabled
  node auto-discovery by default via the low-level REST client `Sniffer`. The
  OpenSearch Java Client 3.x does not ship a sniffer equivalent, so this
  feature is dropped. Keep the `addresses` list up to date manually or put a
  load balancer in front of the cluster. A `WARN` is logged at startup if the
  key is found in the configuration; remove it when migrating.
* Date fields (`nextFetchDate` in the status index and `timestamp` in the
  metrics index) are serialized as ISO-8601 strings produced by
  `Instant#toString()` (for example `2026-01-01T00:00:00Z`). The legacy module
  serializes the same fields through `XContentBuilder#timeField`, which emits
  ISO-8601 strings via `ISODateTimeFormat.dateTime()` (for example
  `2026-01-01T00:00:00.000Z`). Both representations are accepted by the
  default OpenSearch `date` mapping and by the `date_optional_time` /
  `strict_date_optional_time` formats used in the example mappings under
  `src/test/resources/`. If you run a custom mapping that restricts the field
  `format` to `epoch_millis`, update it to accept ISO-8601 (for example
  `strict_date_optional_time||epoch_millis`) before writing with this module.
