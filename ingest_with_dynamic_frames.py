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

df = glueContext.create_dynamic_frame_from_options(
    connection_type= "s3",
    connection_options= {'paths': ['s3://mylab-2022/listings.csv']},
    format = 'csv',
    format_options = {'withHeader': True  }
    )

# Print Spark Dataframe
df.toDF().show(5)

# Print Pandas Dataframe
# df_pd = df.toDF().toPandas()

job = Job(glueContext)
job.init(args['JOB_NAME'], args)
job.commit()