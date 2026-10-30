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

The number of pages parsed in a PDF document can be limited with `parser.tika.pdf.maxpages` (default `-1`, 0 or less keeps the `maxPages` of the Tika configuration, no limit by default). The pages after the limit are skipped, so their text and outlinks are lost, and parsing a very large PDF costs much less CPU. This sets `maxPages` of the PDF parser, other settings of `pdf-parser` in the `parse-context` section of the Tika configuration are kept.

The time spent parsing a document can be limited with `parser.tika.timeout` (milliseconds, default `-1`, 0 or less means no limit). When set, the parse runs in a forked JVM via [Tika Pipes](https://tika.apache.org/docs/4.0.x/pipes/index.html), one per bolt instance. ParserBolt starts the fork during `prepare()` using a small test parse. A parse that reaches the hard deadline is reported as `parse timeout`; a partial result returned between embedded documents is kept and marked `parse.text.trimmed`. When `parser.tika.text.maxlength` is negative (the default), the text is limited to 20,000,000 characters in this mode. A forked JVM that dies while parsing, e.g. running out of memory, is reported as an `ERROR` with the message `parse crash` and restarted; the Storm worker is not affected. By default, Tika closes an idle fork after 60 seconds and starts another for the next document. Keep the timeout well below `topology.message.timeout.secs`: the time a tuple waits behind a slow parse or a fork restart counts against that too.

Each `ParserBolt` instance runs one fork, so a host needs the worker heap plus one fork heap per `ParserBolt` instance on it, and some overhead for each JVM. When several forks share a host, consider `-XX:ActiveProcessorCount` as well as a heap limit. A handful of related keys tune the forked JVMs: `parser.tika.pipes.jvmargs` (`-Xmx512m` is added when they set no maximum heap), `parser.tika.pipes.maxfilesperprocess` (restart a fork after this many documents, to bound slow leaks in parsing libraries; Tika's default is 100000), and `parser.tika.pipes.plugins.dir` (Tika Pipes plugins, not needed by default). Documents over 10MB are handed to the forked JVM through a temporary file in the worker's `java.io.tmpdir`.

## JSoup filters

The JSoupFilters configured with `jsoup.filters.config.file` are applied to every document parsed by Tika, not only HTML, the same way as with the `JSoupParserBolt`; they run before the parse filters. The JSoup document is built from the XHTML Tika produces, not from the original content: a PDF becomes `<div class="page">` and `<p>` elements, and the `<head>` of an HTML page holds Tika's metadata (e.g. `<meta name="dc:subject">` instead of `<meta name="keywords">`), so expressions written for the original HTML may need adjusting. Element names are lowercased. The document is only built if at least one JSoupFilter is configured.

Under `parser.tika.timeout`, the default `IdentityHtmlMapper` is passed to the forked JVM, so the JSoup and parse filters get the same documents as with an in-process parse. Tika can only send the components registered in a `META-INF/tika/*.idx` file to the forked JVM: this module registers `IdentityHtmlMapper` in its `META-INF/tika/parse-context.idx`, which must be merged with the ones from Tika when building a shaded jar (see the `AppendingTransformer` entries in the archetype's `pom.xml`). If it is missing, the bolt logs a warning and the forked JVM uses Tika's `DefaultHtmlMapper`. Any other value of `parser.htmlmapper.classname` is ignored in this mode and the forked JVM uses Tika's `DefaultHtmlMapper`, which drops the elements it does not consider safe.

Since Tika 4, Tika metadata keys use namespaced names, which surface as renamed `parse.*` keys, e.g. `parse.resourceName` is now `parse.tk:resource-name`.
