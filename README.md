[![StormCrawler](https://stormcrawler.apache.org/img/Logo-small.jpg)](https://stormcrawler.apache.org/)
=============

[![license](https://img.shields.io/github/license/apache/stormcrawler.svg?maxAge=2592000?style=plastic)](http://www.apache.org/licenses/LICENSE-2.0)
![Build Status](https://github.com/apache/stormcrawler/actions/workflows/maven.yml/badge.svg)
[![javadoc](https://javadoc.io/badge2/apache/stormcrawler-core/javadoc.svg)](https://javadoc.io/doc/org.apache.stormcrawler/stormcrawler-core/)

Apache StormCrawler is an open source collection of resources for building low-latency, scalable web crawlers on [Apache Storm](http://storm.apache.org/). It is provided under [Apache License](http://www.apache.org/licenses/LICENSE-2.0) and is written mostly in Java.

## Quickstart

NOTE: These instructions assume that you have [Apache Maven](https://maven.apache.org/install.html) installed. You will need to install [Apache Storm 2.8.3](http://storm.apache.org/) to run the crawler.

StormCrawler requires Java 17 or above. To execute tests, it requires you to have a locally installed and working Docker environment.

Once Storm is installed, the easiest way to get started is to generate a new StormCrawler project following the instructions below:

```shell
mvn archetype:generate -DarchetypeGroupId=org.apache.stormcrawler -DarchetypeArtifactId=stormcrawler-archetype -DarchetypeVersion=3.4.0

```

You'll be asked to enter a groupId (e.g. com.mycompany.crawler), an artefactId (e.g. stormcrawler), a version, a package name and details about the user agent to use.

This will not only create a fully formed project containing a POM with the dependency above but also the default resource files, a default CrawlTopology class and a configuration file. Enter the directory you just created (should be the same as the artefactId you specified earlier) and follow the instructions on the README file.

Alternatively if you can't or don't want to use the Maven archetype above, you can simply copy the files from [archetype-resources](https://github.com/apache/stormcrawler/tree/master/archetype/src/main/resources/archetype-resources).

Have a look at [crawler.flux](https://github.com/apache/stormcrawler/blob/master/archetype/src/main/resources/archetype-resources/crawler.flux), the [crawler-conf.yaml](https://github.com/apache/stormcrawler/blob/master/archetype/src/main/resources/archetype-resources/crawler-conf.yaml) file as well as the files in [src/main/resources/](https://github.com/apache/stormcrawler/tree/master/archetype/src/main/resources/archetype-resources/src/main/resources), they are all that is needed to run a crawl topology : all the other components come from the core module.

## Getting help

The [WIKI](https://github.com/apache/stormcrawler/wiki) is a good place to start your investigations but if you are stuck please use the tag [stormcrawler](http://stackoverflow.com/questions/tagged/stormcrawler) on StackOverflow or ask a question in the [discussions](https://github.com/apache/stormcrawler/discussions) section.

The project website has a page listing companies providing [commercial support](https://stormcrawler.apache.org/support/) for Apache StormCrawler.

## Note for developers

Please format your code before submitting a PR with

```
mvn git-code-format:format-code -Dgcf.globPattern="**/*" -Dskip.format.code=false
```

You can enable pre-commit format hooks by running:

```
mvn clean install -Dskip.format.code=false
```

### Building from source

The requirements for building from source are as follows

- JDK 17+
- Apache Maven 3
- Docker (if you want to run tests)

The build itself is straightforward:

```
mvn clean install
```

Note: We use some **binary files** for testing advanced crawler functionality. These files are located exclusively in the `src/test` directories of the respective modules.

## Thanks

![alt tag](https://www.yourkit.com/images/yklogo.png)

YourKit supports open source projects with its full-featured Java Profiler.
YourKit, LLC is the creator of <a href="https://www.yourkit.com/java/profiler/index.jsp">YourKit Java Profiler</a>
and <a href="https://www.yourkit.com/.net/profiler/index.jsp">YourKit .NET Profiler</a>,
innovative and intelligent tools for profiling Java and .NET applications.
