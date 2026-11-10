# stormcrawler-aws
================================

AWS resources for StormCrawler, currently contains bolts for storing and retrieving web pages to/from [S3](https://aws.amazon.com/s3/).

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


