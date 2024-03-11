# TL;DR

A simple Scala Spark application that generates random mock data with a normal distribution. The resulting data is written to Google BigQuery using the Apache Spark connector for BigQuery. See connector details [here](https://github.com/GoogleCloudDataproc/spark-bigquery-connector).

This project can also be used as a template for building Scala Spark CI/CD pipelines with [sbt](https://www.scala-sbt.org/) _(Simple Build Tool)_ and Github Actions.

# Setup

This Scala project is designed to run as a standalone fat Jar. A yaml file in `.github/workflows/` automates the assembly process using Github Actions and the [sbt](https://www.scala-sbt.org/) build tool _(compile and assemble uber Jar)_. Sbt is a build tool for Scala/Java _(similar to Maven)_.

The compiled Uber Jar is then copied into Google Cloud Storage using [google-github-actions/upload-cloud-storage](https://github.com/google-github-actions/upload-cloud-storage).

Github uses a Service Account Key to authenticate with GCP _(when copying the Jar to gcs)_. The yaml script expects this value stored as a Github repo secret _(listed below)_. Set this secret prior to deployment.

| Action Secret | Value                                                          |
| ------------- | -------------------------------------------------------------- |
| GCP_SA_KEY    | Service Account Key used to authenticate GitHub to GCP Project |

> //TODO switch from Service Account Key JSON authentication to Workload Identity Federation https://github.com/google-github-actions/upload-cloud-storage#via-google-github-actionsauth

Next a table is required in Google BigQuery as a destination for the mock data. Create a table with the following schema:

```json
[
  {
    "mode": "REQUIRED",
    "name": "id",
    "type": "INTEGER"
  },
  {
    "mode": "REQUIRED",
    "name": "even_distribution",
    "type": "FLOAT"
  },
  {
    "mode": "REQUIRED",
    "name": "normal_distribution",
    "type": "FLOAT"
  }
]
```

Finally the Dataproc job can be started using gcloud.

_Ex:_

```shell
gcloud dataproc jobs wait job-aab3d442 --project cf-data-analytics --region us-central1
```

# Random Data

The random mock data is generated using Scala Spark Functions. Two different functions are used:

1. [rand()](<https://spark.apache.org/docs/3.2.1/api/scala/org/apache/spark/sql/functions$.html#rand():org.apache.spark.sql.Column>) - Even distribution
2. [randn()](<https://spark.apache.org/docs/3.2.1/api/scala/org/apache/spark/sql/functions$.html#rand():org.apache.spark.sql.Column>) - Normal distribution

# Local Development

When developing locally using [Metals](https://scalameta.org/metals/) or IntelliJ, credentials must be available to authenticate with BigQuery. Set the `GOOGLE_APPLICATION_CREDENTIALS` environment variable to the service account key json file path. More info [here](https://cloud.google.com/docs/authentication/application-default-credentials).

> //TODO switch credential configuration file to workload identity federation.

_Ex:_

```bash
export GOOGLE_APPLICATION_CREDENTIALS=/Users/chasf/df-credentials.json
```

Next the master URL for the cluster must be defined as _local_. Make sure the `.config()` parameters are commented out when deploying to production.

_Ex:_

```scala
 val spark = SparkSession.builder
      .appName("Bq Demo")
      .config("spark.master", "local[*]") // comment out when deploying
      .getOrCreate()
```

The `local[*]` configuration sets the worker threads equal to the logical cores on your machine. More information [here](https://spark.apache.org/docs/latest/submitting-applications.html#master-urls).

There are two options for writing data BigQuery using the Spark connector, Direct Mode and Indirect Mode.

Indirect relies on the [GCS Spark Connector](https://github.com/GoogleCloudDataproc/hadoop-connectors/tree/master/gcs) which is NOT included with the connector. The Maven package for this connector is available here:

> > // https://mvnrepository.com/artifact/com.google.cloud.bigdataoss/gcs-connector
> > libraryDependencies += "com.google.cloud.bigdataoss" % "gcs-connector" % "hadoop3-2.2.10"

Several Spark settings are also required when using the GCS connector locally. Comment these out before deploying to Dataproc _(see below)_. More information on these settings can be found [here](https://github.com/GoogleCloudDataproc/hadoop-connectors/tree/master/gcs).

```scala
  val spark = SparkSession.builder
     .appName("Bq Demo")
     // .config("spark.master", "local[*]")
     // .config(
     //   "spark.hadoop.fs.AbstractFileSystem.gs.impl",
     //   "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS"
     // )
     // .config("spark.hadoop.fs.gs.project.id", "cf-data-analytics")
     // .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
     // .config(
     //   "spark.hadoop.google.cloud.auth.service.account.json.keyfile",
     //   "/Users/chasf/Desktop/cf-data-analytics-1ff73e9e3f7a.json"
     // )
     .getOrCreate()
```

# Reference

This application was built using several articles as reference. These sources are listed below.

## GCS Actions

https://github.com/google-github-actions/upload-cloud-storage

This library was used in the deployment script to move the fat Jar into Google Cloud Storage. The Jar can be deployed to Dataproc using `gcloud`.

_For example_:

```shell
gcloud dataproc jobs submit spark
```

Considerations:

1. Turn off gzip content encoding. Spark isn't configured to read compressed Jar files.

```yaml
gzip: false
```

2. Specify a Glob Pattern _(wildcard)_ to reference the latest version number declared in `build.sbt`.

```yaml
glob: "*.jar"
```

## Setup Java Runner for SBT Deployment

https://github.com/actions/setup-java

This repo documents yaml instructions for setting up a Java runner. The sbt Scala build takes place on the Java runner.

## Configure SBT Build

These two articles document `build.sbt` and `plugins.sbt` configurations.

GCP instructions for setting up SBT:

https://cloud.google.com/dataproc/docs/guides/manage-spark-dependencies

Article where SBT was used with Github Actions to deploy uber Jars to S3:

https://medium.com/alterway/building-a-ci-cd-pipeline-for-a-spark-project-using-github-actions-sbt-and-aws-s3-part-1-c7d43658832d
