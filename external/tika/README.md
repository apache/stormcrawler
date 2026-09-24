# Tika module

Contains a bolt implementation which uses [Apache Tika](http://tika.apache.org/) to parse documents. This bolt can be used as a drop-in replacement for the JSoup-based on from the core module.

To use it alongside the JSoup parser i.e. let JSoup handle HTML content and Tika do everything else, you need to configure the JSoupParser with `jsoup.treat.non.html.as.error: false` so that documents that are not HTML don't get failed but passed on.

The next step is to use a [RedirectionBolt](https://github.com/apache/stormcrawler/blob/master/external/tika/src/main/java/org/apache/stormcrawler/tika/RedirectionBolt.java) to send documents which have not been parsed with Jsoup to Tika on a bespoke stream called `tika`, finally the IndexingBolt needs to be connected to the outputs of both `shunt` and `tika` on the default stream. `tika` must also be connected to the StatusUpdaterBolt on the _status_ stream.

```
  builder.setBolt("jsoup", new JSoupParserBolt()).localOrShuffleGrouping(
          "sitemap");
  
  builder.setBolt("shunt", new RedirectionBolt()).localOrShuffleGrouping("jsoup");
  
  builder.setBolt("tika", new ParserBolt()).localOrShuffleGrouping("shunt",
          "tika");
  
  builder.setBolt("indexer", new IndexingBolt(), numWorkers)
          .localOrShuffleGrouping("shunt").localOrShuffleGrouping("tika");
 ```
 
 To restrict the parsing to certain mime-types, provide a list of regular expressions as values to the configuration `parser.mimetype.whitelist`, for instance:
 
 ```
  parser.mimetype.whitelist:
  - application/.+word.*
  - application/.+excel.*
  - application/.+powerpoint.*
  - application/.*pdf.*
 ```
 
## Configure Tika

The Tika parser bolt loads a Tika configuration file from the Java classpath. The default file name (path) is `tika-config.json` and can be changed by the configuration `parser.tika.config.file`. Since Tika 4, configurations are written in JSON instead of XML - see [configuring Tika](https://tika.apache.org/docs/4.0.x/configuration/index.html) and the default configuration file [tika-config.json](./src/main/resources/tika-config.json).

Note that a configuration which is present on the classpath but invalid is treated as an error: the bolt fails to start instead of silently falling back to the default Tika configuration.

Embedded documents are only parsed when `parser.extract.embedded` is set to `true` (default `false`).

The length of the text extracted from a document can be limited with `parser.tika.text.maxlength` (number of characters, default `-1`, any negative value means no limit). When the limit is reached the parse stops, the text and outlinks extracted so far are kept and the document is emitted with the metadata `parse.text.trimmed` set to `true`.

The time spent parsing a document can be limited with `parser.tika.timeout` (milliseconds, default `-1`, 0 or less means no limit). When set, the parse runs in a forked JVM via [Tika Pipes](https://tika.apache.org/docs/4.0.x/pipes/index.html), one per bolt instance, started with the same `java` and classpath as the Storm worker: a document that takes longer than the timeout is killed outright (not merely asked to stop) and sent to the status stream as an `ERROR` with the message `parse timeout`, and the fork restarts before the next document. A forked JVM that dies while parsing, e.g. running out of memory, is reported as an `ERROR` with the message `parse crash` and restarted as well; the Storm worker is not affected. Keep the timeout below `topology.message.timeout.secs` so that the tuple is not replayed while it is being parsed.

Each forked JVM needs memory on top of the worker, so size the hosts for the number of `ParserBolt` executors. A handful of related keys tune the forked JVMs: `parser.tika.pipes.jvmargs` (e.g. `-Xmx512m`), `parser.tika.pipes.maxfilesperprocess` (restart a fork after this many documents, to bound slow leaks in parsing libraries), and `parser.tika.pipes.plugins.dir` (Tika Pipes plugins, not needed by default). Documents over 10MB are handed to the forked JVM through a temporary file in the worker's `java.io.tmpdir`.

`parser.htmlmapper.classname` is not applied to parses running under `parser.tika.timeout`: an `HtmlMapper` cannot be passed to the forked JVM, which always uses Tika's `DefaultHtmlMapper`. That mapper drops the elements it does not consider safe from the DOM given to the parse filters, so XPath expressions written against the default `IdentityHtmlMapper` output may need adjusting.

Since Tika 4, Tika metadata keys use namespaced names, which surface as renamed `parse.*` keys, e.g. `parse.resourceName` is now `parse.tk:resource-name`.
