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
urlfrontier.backoff.max.secs: 86400

metadata.persist:
  - robots.crawl.delay
```

Do not include `robots.crawl.delay` in `metadata.transfer`, directly or through a wildcard such as
`robots.*`: an outlink must not inherit its parent's host delay. The bolt rejects an unsafe robots
configuration at startup. A custom `metadata.transfer.class` must preserve this contract for every
URL and value; the startup probe can only exercise representative metadata. `setDelay(key, 0)` is
not sent if a site later removes its Crawl-delay, so that host can remain slower than necessary but
is not made less polite. A batch size of one bounds each hand-out; it does not recall URLs already
emitted or eliminate concurrent requests while the new delay is reaching the frontier.

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


