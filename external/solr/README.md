# stormcrawler-solr

Set of [Apache Solr](https://solr.apache.org/) resources for StormCrawler that allows you to create topologies that consume from a Solr collection and store metrics, status or parsed content into Solr.

## Getting started

The easiest way is currently to use the archetype for Solr with:

`mvn archetype:generate -DarchetypeGroupId=org.apache.stormcrawler -DarchetypeArtifactId=stormcrawler-solr-archetype -DarchetypeVersion=3.4.0`

You'll be asked to enter a groupId (e.g. com.mycompany.crawler), an artefactId (e.g. stormcrawler), a version, a package name and details about the user agent to use.

This will not only create a fully formed project containing a POM with the dependency above but also a set of resources, configuration files and sample topology classes. Enter the directory you just created (should be the same as the artefactId you specified earlier) and follow the instructions on the README file.

You will of course need to have both Apache Storm (2.8.3) and Apache Solr (9.8.0) installed.

Official references:
* [Apache Storm: Setting Up a Development Environment](https://storm.apache.org/releases/current/Setting-up-development-environment.html)
* [Apache Solr: Installation & Deployment](https://solr.apache.org/guide/solr/latest/deployment-guide/installing-solr.html)

## Available resources

* [IndexerBolt](https://github.com/apache/stormcrawler/blob/main/external/solr/src/main/java/org/apache/stormcrawler/solr/bolt/IndexerBolt.java): Implementation of [AbstractIndexerBolt](https://github.com/apache/stormcrawler/blob/main/core/src/main/java/org/apache/stormcrawler/indexing/AbstractIndexerBolt.java) that allows to index the parsed data and metadata into a specified Solr collection.

* [MetricsConsumer](https://github.com/apache/stormcrawler/blob/main/external/solr/src/main/java/org/apache/stormcrawler/solr/metrics/MetricsConsumer.java): Class that allows to store Storm metrics in Solr.

* [SolrSpout](https://github.com/apache/stormcrawler/blob/main/external/solr/src/main/java/org/apache/stormcrawler/solr/persistence/SolrSpout.java): Spout that allows to get URLs from a specified Solr collection.

* [StatusUpdaterBolt](https://github.com/apache/stormcrawler/blob/main/external/solr/src/main/java/org/apache/stormcrawler/solr/persistence/StatusUpdaterBolt.java): Implementation of [AbstractStatusUpdaterBolt](https://github.com/apache/stormcrawler/blob/main/core/src/main/java/org/apache/stormcrawler/persistence/AbstractStatusUpdaterBolt.java) that allows to store the status of each URL along with the serialized metadata in Solr.

