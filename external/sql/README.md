# SQL module for StormCrawler

 Contains a spout implementation as well as a status updater bolt and a MetricsConsumer.

The [tableCreation.script](https://github.com/apache/stormcrawler/blob/main/external/sql/tableCreation.script) is based on MySQL and is used for the creation of the tables.

Check that you have specified a configuration file such as [sql-conf.yaml](https://github.com/apache/stormcrawler/blob/master/external/sql/sql-conf.yaml) and have a Java driver in the dependencies of your POM

```
        <dependency>
            <groupId>com.mysql</groupId>
            <artifactId>mysql-connector-j</artifactId>
            <version>VERSION</version>   
        </dependency>
```

You can either inject the seeds directly as done in the tutorial or reuse the injection topology from the elasticsearch module and specify the statusupdaterbolt from the mysql module instead. Another approach could be to simply to add a MemorySpout to the topology alongside the SQLSpout.
