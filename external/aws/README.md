# stormcrawler-aws
================================

AWS resources for StormCrawler, currently contains an indexer bolt for [CloudSearch](https://aws.amazon.com/cloudsearch/) and another bolt for storing and retrieving web pages to/from [S3](https://aws.amazon.com/s3/).

This module is built on the [AWS SDK for Java 2.x](https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/home.html). Credentials are resolved through the SDK's [default credentials provider chain](https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/credentials-chain.html) (environment variables, Java system properties, `~/.aws/credentials`, container/instance profiles, etc.).

## Prerequisites

Add stormcrawler-aws to the dependencies of your project\:

```xml
<dependency>
    <groupId>org.apache.stormcrawler</groupId>
    <artifactId>stormcrawler-aws</artifactId>
    <version>XXXX</version>
</dependency>
```
Edit `~/.aws/credentials`, see [http://docs.aws.amazon.com/cli/latest/userguide/cli-chap-getting-started.html] for details. Note that this should not be necessary when running on EC2.


## CloudSearch

* How to use?

Add stormcrawler-aws as a Maven dependency, use the class CloudSearchIndexWriter in your Storm topology alongside the core StormCrawler components and create a yaml configuration file (see below).

* AWS credentials 

Requires the AWS credentials to be stored in ~/.aws/credentials (see prerequisites above). You need to have a pre-existing search domain on CloudSearch 

* Create a CloudSearch domain

This can be done using the web console [https://eu-west-1.console.aws.amazon.com/cloudsearch/home?region=eu-west-1#] or the AWS CLI [http://docs.aws.amazon.com/cloudsearch/latest/developerguide/creating-domains.html]. You can use the temp file generated with `cloudsearch.batch.dump` (see below) to bootstrap the field definition. 

Note that the creation of the domain can take some time. Once it is complete, note the document endpoint and region name.

* Configuration

See file [aws-conf.yaml] for an example of configuration. 

You'll need to define `cloudsearch.endpoint` and `cloudsearch.region` , unless you set `cloudsearch.batch.dump` to `true` in which case the batch of documents to index in JSON format will be dumped on the default tmp directory. The files have the prefix "CloudSearch_" e.g. `/tmp/CloudSearch_4822180575734804454.json`. These temp files can be used as templates when defining the fields in the domain creation (see above).

There are two additional configurations for CloudSearch \:

cloudsearch.batch.maxSize \: number of documents to buffer before sending as batch to CloudSearch. Default value -1.
cloudsearch.batch.max.time.buffered \: max time allowed before flushing the buffer of documents to CloudSearch, in seconds. Default value 10.

In both cases the restriction set by CloudSearch on the size of a batch will take precedence. 
  
* General behaviour

In case of an exception while sending a batch to CloudSearch, the corresponding tuples will be failed. The behaviour of the topology depends on the fail logic of the spouts. Any errors will be logged and the topology should continue without interruption.

Any fields not defined in the CloudSearch domain will be ignored by the CloudSearchIndexWriter. Again, the logs will contain a trace of any field names skipped. It is advisable to check the logs and modify the indexing options for your CloudSearch domain accordingly.

## S3

Add `S3ContentCacher` or `S3CacheChecker` to your crawl topology.

* Configuration

| key | default | description |
| --- | --- | --- |
| `s3.bucket` | - | S3 bucket name used for content caching. The bucket must already exist. |
| `s3.region` | - | AWS region for S3 operations. |
| `s3.endpoint` | - | Custom S3 endpoint (optional, for S3-compatible services). |
| `s3.path.style.access` | `false` | Use path-style access (`http://host/bucket`) instead of virtual-hosted style. Required for S3-compatible endpoints such as LocalStack or MinIO. |

## Testing

The S3 bolts are covered by integration tests that run against [LocalStack](https://localstack.cloud/) via [Testcontainers](https://www.testcontainers.org/), so a running Docker daemon is required. Tests are skipped automatically when Docker is not available.


