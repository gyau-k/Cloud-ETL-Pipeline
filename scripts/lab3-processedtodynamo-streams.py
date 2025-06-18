import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Load cleaned data from S3
cleaned_streams_dyf = glueContext.create_dynamic_frame.from_options(
    format_options={"withHeader": True},
    connection_type="s3",
    format="parquet",
    connection_options={"paths": ["s3://lab3-bucket/processed/streams/"]},
    transformation_ctx="read_cleaned_streams"
)

# Write to DynamoDB
glueContext.write_dynamic_frame_from_options(
    frame=cleaned_streams_dyf,
    connection_type="dynamodb",
    connection_options={
        "dynamodb.output.tableName": "lab3",
        "dynamodb.throughput.write.percent": "1.0"
    }
)

job.commit()