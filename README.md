# SPARK-LAB

This project aims to ingest and clean airbnb public data via AWS Glue and Local PySpark 

## Glue Configurations
You can access to aws console to create a new glue job or deploy via an IaC tool like Cloudformation or Terraform...

I created a service role which is able to access to s3 bucket and Cloudwatch Log in Read and Write Mode.
I have allocated 2 workers for my glue job. I am using Glue 3.0
## Using glue dynamic frame

Result: Ingest CSV File of 14 MB in 1 Minutes 5 Seconds
## Using spark dataframe
Result: Ingest CSV File of 14 MB in 1 Minutes 16 Seconds

## Run Spark in Local Standalone Mode

Requirements:
``
* Java 8
* Spark-3.1.3-bin-hadoop3.2
* Python 3.9 (Anaconda if you are familiar with conda)
* pyspark from 
* py4j
* findspark (not required)
``
 
[Run Spark Scala/Python in Local](https://phoenixnap.com/kb/install-spark-on-windows-10)

You can test the installation with my juypter notebook

## Get AWS Credentials to interact with aws resources
[Official Docs](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-files.html)


