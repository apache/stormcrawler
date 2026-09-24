# URLFRONTIER

This module contains Spout and StatusUpdaterBolt implementations to communicate with a [URLFrontier](https://github.com/crawler-commons/url-frontier) service.

## Run the service

The easiest way to run the Frontier is to use Docker and do

```
 docker pull crawlercommons/url-frontier:1.0
 docker run --rm --name frontier -p 7071:7071 crawlercommons/url-frontier:1.0
```

## Configuration


Below are the configuration elements and their default values

```
urlfrontier.host: localhost
urlfrontier.port: 7071

urlfrontier.max.buckets: 10
urlfrontier.max.urls.per.bucket:10
```

## Transport security

The gRPC channels to the frontier are plaintext unless TLS is enabled. Plaintext is kept as the
default so that existing deployments keep working. The channel carries the URLs and their metadata, so enable TLS whenever the
frontier runs on another host.

URLFrontier 2.6 only listens in plaintext, so until
[crawler-commons/url-frontier#219](https://github.com/crawler-commons/url-frontier/pull/219) is
released, the frontier needs a TLS terminating proxy in front of it, and `urlfrontier.address` or
`urlfrontier.host` and `urlfrontier.port` must point at that proxy. Once the frontier supports TLS
itself, the same settings connect to it directly:

```yaml
urlfrontier.tls.enabled: true
# PEM file with the certificates trusted to sign the server certificate;
# the JVM trust store is used if not set
urlfrontier.tls.trust.cert.collection: /etc/stormcrawler/frontier-ca.pem
# client certificate and PKCS#8 private key for mutual TLS, both or neither
urlfrontier.tls.client.cert.chain: /etc/stormcrawler/crawler.pem
urlfrontier.tls.client.private.key: /etc/stormcrawler/crawler-key.pem
# only needed if the private key is encrypted
urlfrontier.tls.client.private.key.password: changeit
```

The server certificate must be valid for the host name in `urlfrontier.address` or
`urlfrontier.host`. Setting only one of `urlfrontier.tls.client.cert.chain` and
`urlfrontier.tls.client.private.key`, or pointing a key at a file which cannot be read, fails
the component at startup. A failed TLS handshake, on the other hand, does not: the channel keeps
reconnecting, `Spout` and `StatusUpdaterBolt` wait for it without a deadline, and the topology
runs without fetching or updating anything. The settings apply to `Spout`, `StatusUpdaterBolt`
and `QueueRegulatorBolt`.

## Depth metric

The spout counts the URLs handed out by the frontier per depth level, so that the shape of the
crawl (broad first, or sinking into a few sites) can be observed. The counter is named `depth`
and has one scope per depth value read from the `depth` metadata written when
`metadata.track.depth` is enabled: `depth.0`, `depth.1`, ... URLs without a numeric depth are
counted under `depth.unknown`. To keep the number of scopes bounded when no `MaxDepthFilter` is
configured, depths at or beyond

```
urlfrontier.depth.metric.max: 10
```

share a single `depth.10+` scope.

A second counter, `depth_le`, holds the cumulative counts, in the style of Prometheus histograms:
`depth_le.X` is the number of URLs with a depth at most X and `depth_le.inf` the number of URLs
with a valid depth, so `depth_le.X / depth_le.inf` is the cumulative distribution P(depth ≤ X).
URLs beyond the cap only count in `depth_le.inf`, and `unknown` ones in neither. The spout exposes
the same ratio, computed in constant time since it was opened, through
`probabilityDepthAtMost(int depth)`; it returns NaN until the first URL with a depth arrives.

`windowProbabilityDepthAtMost(int depth)` gives the same ratio restricted to the URLs handed out
within the last

```
urlfrontier.depth.window.secs: 300
```

so that it follows the current shape of the crawl rather than its whole history, which is what a
control loop reacting to a drift into depth needs. The window is a ring of ten slots, so the
oldest data it includes is between nine and ten tenths of the window old. It returns NaN when
nothing was received within the window.

## Sending discovered URLs in batches

`StatusUpdaterBolt` sends known URLs (fetched, redirections, errors...) to the frontier's
`PutURLs` endpoint one message at a time, and discovered URLs, which are the bulk of what a
crawl writes, through the batched `PutDiscovered` endpoint introduced in
[URLFrontier 2.6](https://github.com/crawler-commons/url-frontier/releases/tag/2.6). Up to

```
urlfrontier.batch.size: 100
```

discovered URLs are grouped into one message, which amortises the per-message cost that limits
the ingestion rate. A partially filled batch is sent after a second at the latest, so that acks
are not delayed when the crawl tails off. Set `urlfrontier.batch.size` to 0 to send every URL
individually. If the frontier predates 2.6 and does not implement `PutDiscovered`, the bolt
detects it and falls back to sending discovered URLs individually on the streaming endpoint.

## Robots crawl-delay pacing

`QueueRegulatorBolt` can pace host queues when a robots.txt Crawl-delay exceeds the fetcher's local
limit. Wire it to the `queue` stream emitted by `StatusUpdaterBolt`:

```yaml
bolts:
  - id: "queue-regulator"
    className: "org.apache.stormcrawler.urlfrontier.QueueRegulatorBolt"
    parallelism: 1

streams:
  - from: "status"
    to: "queue-regulator"
    grouping:
      type: FIELDS
      args: ["key"]
      streamId: "queue"
```

Robots pacing is opt-in. It requires host partitioning, one URL per frontier hand-out, a positive
delay cap, and a persist-only control signal:

```yaml
partition.url.mode: byHost
fetcher.max.crawl.delay.force: true
urlfrontier.robots.crawl.delay.enabled: true
urlfrontier.max.urls.per.bucket: 1
# cap for the forwarded delay, seconds (default 86400)
urlfrontier.robots.delay.max.secs: 86400
# dedupe window after which an unchanged delay is re-sent, seconds (default 1800)
urlfrontier.robots.delay.decay.secs: 1800

metadata.persist:
  - robots.crawl.delay
```

The forwarded value is conservative: the maximum observed delay wins, and a lower delay can only
be applied after the previous maximum expires from the decay window. `setDelay(key, 0)` is
not sent if a site later removes its Crawl-delay, so that host can remain slower than necessary but
is not made less polite.

Do not include `robots.crawl.delay` in `metadata.transfer`, directly or through a wildcard such as
`robots.*`: an outlink must not inherit its parent's host delay. The bolt rejects an unsafe robots
configuration at startup. A custom `metadata.transfer.class` must preserve this contract for every
URL and value; the startup probe can only exercise representative metadata. A batch size of one
bounds each hand-out; it does not recall URLs already emitted.

Robots pacing also requires a single URLFrontier endpoint — at most one `urlfrontier.address`
entry; the `urlfrontier.host`/`urlfrontier.port` fallback is fine. Keyed `setDelay` calls are
not propagated across URLFrontier nodes
([crawler-commons/url-frontier#146](https://github.com/crawler-commons/url-frontier/issues/146)),
so with several nodes only the queues owned by the connected node would be paced. The bolt fails
fast on multiple configured addresses when pacing is enabled, and only warns otherwise (rate-limit
blocks stay best-effort). A cluster behind a single load-balanced address cannot be detected and
has the same limitation. With several concurrent `getURLs` clients (multiple Spout tasks, or
several topologies on the same frontier) the frontier's per-queue politeness gate is not atomic
([crawler-commons/url-frontier#147](https://github.com/crawler-commons/url-frontier/issues/147))
and concurrent requests can each be served a URL from the same queue inside the delay window;
prefer a single Spout task per frontier when pacing must be strict.

Your StormCrawler topology requires the following dependency in its pom.xml (just like with any other module)

```
 <dependency>
  <groupId>org.apache.stormcrawler</groupId>
  <artifactId>stormcrawler-urlfrontier</artifactId>
  <version>${stormcrawler.version}</version>
 </dependency>
 ```
 
 but can also include
 
 ```
<dependency>
 <groupId>com.github.crawler-commons</groupId>
 <artifactId>urlfrontier-client</artifactId>
 <version>1.2</version>
</dependency>
```

so that the [URLFrontier client](https://github.com/crawler-commons/url-frontier/client) gets added to the uber-jar.

This way you will be able to interact with the Frontier from the command line, e.g. to inject seeds

```
java -cp target/*.jar crawlercommons.urlfrontier.client.Client PutUrls -f seeds.txt
```


