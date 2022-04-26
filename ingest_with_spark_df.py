import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

df = spark.read\
    .format('csv')\
    .option("header", "true")\
    .option("delimiter", ",")\
    .option("quote", '"')\
    .option("inferSchema", "true")\
    .csv("s3://mylab-2022/listings.csv")


df.show(5)

job = Job(glueContext)
job.init(args['JOB_NAME'], args)
job.commit()