name: "crawler"

includes:
    - resource: true
      file: "/crawler-default.yaml"
      override: false

    - resource: false
      file: "crawler-conf.yaml"
      override: true

    - resource: false
      file: "opensearch-conf.yaml"
      override: true

spouts:
  - id: "spout"
    className: "org.apache.stormcrawler.opensearch.persistence.AggregationSpout"
    parallelism: 10

bolts:
  - id: "partitioner"
    className: "org.apache.stormcrawler.bolt.URLPartitionerBolt"
    parallelism: 1
  - id: "fetcher"
    className: "org.apache.stormcrawler.bolt.FetcherBolt"
    parallelism: 1
  - id: "sitemap"
    className: "org.apache.stormcrawler.bolt.SiteMapParserBolt"
    parallelism: 1
  - id: "parse"
    className: "org.apache.stormcrawler.bolt.JSoupParserBolt"
    parallelism: 1
  - id: "shunt"
    className: "org.apache.stormcrawler.tika.RedirectionBolt"
    parallelism: 1
  - id: "tika"
    className: "org.apache.stormcrawler.tika.ParserBolt"
    parallelism: 1
  - id: "index"
    className: "org.apache.stormcrawler.opensearch.bolt.IndexerBolt"
    parallelism: 1
  - id: "status"
    className: "org.apache.stormcrawler.opensearch.persistence.StatusUpdaterBolt"
    parallelism: 1
  - id: "deleter"
    className: "org.apache.stormcrawler.opensearch.bolt.DeletionBolt"
    parallelism: 1
  - id: "status_metrics"
    className: "org.apache.stormcrawler.opensearch.metrics.StatusMetricsBolt"
    parallelism: 1
  - id: "queues"
    className: "org.apache.stormcrawler.opensearch.persistence.QueueBolt"
    parallelism: 1

streams:
  - from: "spout"
    to: "partitioner"
    grouping:
      type: SHUFFLE

  - from: "__system"
    to: "status_metrics"
    grouping:
      type: SHUFFLE
      streamId: "__tick"

  - from: "partitioner"
    to: "fetcher"
    grouping:
      type: FIELDS
      args: ["key"]

  - from: "fetcher"
    to: "sitemap"
    grouping:
      type: LOCAL_OR_SHUFFLE

  - from: "sitemap"
    to: "parse"
    grouping:
      type: LOCAL_OR_SHUFFLE

  - from: "parse"
    to: "shunt"
    grouping:
      type: LOCAL_OR_SHUFFLE

  - from: "shunt"
    to: "tika"
    grouping:
      type: LOCAL_OR_SHUFFLE
      streamId: "tika"

  - from: "tika"
    to: "index"
    grouping:
      type: LOCAL_OR_SHUFFLE

  - from: "shunt"
    to: "index"
    grouping:
      type: LOCAL_OR_SHUFFLE

  - from: "fetcher"
    to: "status"
    grouping:
      type: FIELDS
      args: ["url"]
      streamId: "status"

  - from: "sitemap"
    to: "status"
    grouping:
      type: FIELDS
      args: ["url"]
      streamId: "status"

  - from: "parse"
    to: "status"
    grouping:
      type: FIELDS
      args: ["url"]
      streamId: "status"

  - from: "tika"
    to: "status"
    grouping:
      type: FIELDS
      args: ["url"]
      streamId: "status"

  - from: "index"
    to: "status"
    grouping:
      type: FIELDS
      args: ["url"]
      streamId: "status"

  - from: "status"
    to: "deleter"
    grouping:
      type: LOCAL_OR_SHUFFLE
      streamId: "deletion"

  - from: "status"
    to: "queues"
    grouping:
      type: FIELDS
      args: ["key"]
      streamId: "queue"
