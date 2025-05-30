#  Resources for generating and consuming WARC files with StormCrawler

First, you need to add the WARC module to the dependencies of your project.

```
		<dependency>
			<groupId>org.apache.stormcrawler</groupId>
			<artifactId>stormcrawler-warc</artifactId>
			<version>${stormcrawler.version}</version>
		</dependency>
```

## Generating WARC files

Archiving the crawled content and metadata in [WARC](https://iipc.github.io/warc-specifications/specifications/warc-format/warc-1.1/) files is done by WARCHdfsBolt: for every incoming tuple one or two WARC records are written:
- (if enabled) the WARC "request" record containing the request metadata and the HTTP request headers
- the WARC "response" record holding response metadata and HTTP headers and the content payload
  - note: if HTTP headers are not stored by the HTTP protocol implementation WARC "resource" records are written instead. See below, how to enable that HTTP headers are stored.

The WARCHdfsBolt writes WARC files
- with [record-level compression](https://iipc.github.io/warc-specifications/specifications/warc-format/warc-1.1/#record-at-time-compression)
- and a configurable rotation policy (when to finish a WARC file and start the next one)

To configure the WARCHdfsBolt, include the following snippet in your crawl topology:

```java 
        String warcFilePath = "/warc";

        FileNameFormat fileNameFormat = new WARCFileNameFormat()
                .withPath(warcFilePath);

        Map<String,String> fields = new HashMap<>();
        fields.put("software:", "Apache StormCrawler 3.3.0 http://stormcrawler.net/");
        fields.put("format", "WARC File Format 1.0");
        fields.put("conformsTo:",
                "https://iipc.github.io/warc-specifications/specifications/warc-format/warc-1.0/");
        
        WARCHdfsBolt warcbolt = (WARCHdfsBolt) new WARCHdfsBolt()
                .withFileNameFormat(fileNameFormat);
        warcbolt.withHeader(fields);

        // enable WARC request records
        warcbolt.withRequestRecords();

        // can specify the filesystem - will use the local FS by default
        String fsURL = "hdfs://localhost:9000";
        warcbolt.withFsUrl(fsURL);
        
        // a custom max length can be specified - 1 GB will be used as a default
        FileSizeRotationPolicy rotpol = new FileSizeRotationPolicy(50.0f,
                Units.MB);
        warcbolt.withRotationPolicy(rotpol);

        builder.setBolt("warc", warcbolt).localOrShuffleGrouping("fetch");
```

If you use Flux, you could add it like so:

```
components:
  - id: "WARCFileNameFormat"
    className: "org.apache.stormcrawler.warc.WARCFileNameFormat"
    configMethods:
      - name: "withPath"
        args:
          - "/path/to/warc"
      - name: "withPrefix"
        args:
          - "my-warc-prefix"

  - id: "WARCFileRotationPolicy"
    className: "org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy"
    constructorArgs:
      - 50.0
      - MB

  - id: "WARCInfo"
    className: "java.util.LinkedHashMap"
    configMethods:
      - name: "put"
        args:
         - "software"
         - "Apache StormCrawler 3.3.0 http://stormcrawler.net/"
      - name: "put"
        args:
         - "format"
         - "WARC File Format 1.0"
      - name: "put"
        args:
         - "conformsTo"
         - "https://iipc.github.io/warc-specifications/specifications/warc-format/warc-1.0/"

[...]

bolts:
 - id: "warc"
    className: "org.apache.stormcrawler.warc.WARCHdfsBolt"
    parallelism: 1
    configMethods:
      - name: "withFileNameFormat"
        args:
          - ref: "WARCFileNameFormat"
      - name: "withRotationPolicy"
        args:
          - ref: "WARCFileRotationPolicy"
      - name: "withRequestRecords"
      - name: "withHeader"
        args:
          - ref: "WARCInfo"
```

Each instance of the bolt will generate a WARC file and close it once it has reached the required size.

Please note that the WARCHdfsBolt is a dead-end and does not output tuples to subsequent bolts.

The tuples are acked based on the sync policy, which is based on either of:
* an explicit sync as set in the sync policy which we have by default at 10 tuples
* an automatic one which happens via tick tuples every 15 secs by default

With the local file system, you need to specify 

```
  warcbolt.withConfigKey("warc");
  Map<String, Object> hdfsConf = new HashMap<>();
  hdfsConf.put("fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem");
  getConf().put("warc", hdfsConf);
```

This uses the RawLocalFileSystem, which unlike the checksum one used by default does a proper sync of the content to the file.

Using Flux the RawLocalFileSystem is enabled by adding the following statements to `config` and the `warc` bolt:

```
config:
  warc: {"fs.file.impl": "org.apache.hadoop.fs.RawLocalFileSystem"}

bolts:
 - id: "warc"
    ...
    configMethods:
      ...
      - name: "withConfigKey"
        args:
          - "warc"
```

Writing complete and valid WARC requires that HTTP headers, IP address and capture time are stored by the HTTP protocol implementation in the metadata. Only the okhttp protocol package stores all necessary information. To enable the okhttp protocol implementation and let them save the HTTP headers, you need to add to your configuration:

```
  http.store.headers: true

  http.protocol.implementation: org.apache.stormcrawler.protocol.okhttp.HttpProtocol
  https.protocol.implementation: org.apache.stormcrawler.protocol.okhttp.HttpProtocol
```

A note on the recording of HTTP requests and responses with StormCrawler and the WARC module:
- the WARC file format is derived from the HTTP message format ([RFC 2616](https://www.ietf.org/rfc/rfc2616.txt)) and the WARC format as well as WARC readers require that HTTP requests and responses are recorded as HTTP/1.1 or HTTP/1.0. Therefore, the WARC WARCHdfsBolt writes binary HTTP formats (eg. [HTTP/2](https://en.wikipedia.org/wiki/HTTP/2)) as if they were HTTP/1.1. There is no need to limit the supported HTTP protocol versions to HTTP/1.0 or HTTP/1.1.
- HTTP transfer and content encodings are not preserved in WARC records. In order to avoid that WARC readers fail on parsing HTTP messages,
  - the HTTP response headers `Transfer-Encoding`, `Content-Encoding` and `Content-Length` are masked with the prefix `X-Crawler-`
  - a new `Content-Length` header is always appended with the actual payload length.

You can specify in the configuration which metadata key/values to store as WARC metadata using `warc.metadata.keys`.

## Consuming WARC files

Web archives harvested in the [WARC format](https://iipc.github.io/warc-specifications/specifications/warc-format/warc-1.1/) can be used as input for StormCrawler – instead of fetching content from remote servers, the WARCSpout reads WARC files and emits the archive web page captures as tuples into the topology.

The WARCSpout is configured similar as FileSpout:
- input files are defined by
  - read from the configured folder (available as local file system path)
  - a pattern matching valid file names
- every line in the input files specifies one input WARC file as file path or URL.

The non-http input paths are loaded via HDFS, which can be configured using the key `hdfs` e.g.

```
  hdfs:  
    fs.s3a.access.key: ${awsAccessKeyId}
    fs.s3a.secret.key: ${awsSecretAccessKey}
```

Please note that in order to access WARC files on AWS S3, you will need to add the following dependency to your project

```
 <dependency>
   <groupId>org.apache.hadoop</groupId>
   <artifactId>hadoop-aws</artifactId>
   <version>2.10.1</version>
 </dependency>
```

where the version should match the one used by Apache Storm. In doubt, you can check with 

```
mvn dependency:tree | grep "org.apache.hadoop:hadoop-hdfs:jar"
```


To use the WARCSpout reading `*.paths` or `*.txt` files from the folder `input/`, you simply start to build your topology as

```java
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("spout", new WARCSpout("input/", "*.{paths,txt}"));
```

Or, if Flux is used:

```
spouts:
  - id: "spout"
    className: "org.apache.stormcrawler.warc.WARCSpout"
    parallelism: 1
    constructorArgs:
      - "input/"
      - "*.{paths,txt}"
```

