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

The Tika parser bolt loads a Tika configuration file from the Java classpath. The default file name (path) is `tika-config.json` and can be changed by the configuration `parser.tika.config.file`. Since Tika 4, configurations are written in JSON instead of XML - see [configuring Tika](https://tika.apache.org/4.0.0/config.html) and the default configuration file [tika-config.json](./src/main/resources/tika-config.json).
