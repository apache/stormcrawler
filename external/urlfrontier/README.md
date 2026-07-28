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


