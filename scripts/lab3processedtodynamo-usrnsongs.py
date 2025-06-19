import boto3
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import SparkSession
from pyspark.sql.functions import concat_ws,lit,col

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = SparkSession(sc)
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Create table if needed (in this script or separately)
dynamodb = boto3.resource('dynamodb', region_name='eu-north-1')
try:
    table_status = dynamodb.create_table(
        TableName="lab3",
        KeySchema=[{'AttributeName': 'uuid', 'KeyType': 'HASH'}],
        AttributeDefinitions=[{'AttributeName': 'uuid', 'AttributeType': 'S'}],
        ProvisionedThroughput={'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5}
    )
    table_status.meta.client.get_waiter('table_exists').wait(TableName="lab3")
except dynamodb.meta.client.exceptions.ResourceInUseException:
    pass

# Read transformed data from S3
songs_df = spark.read.parquet("s3://lab3-bucket/processed/songs/")
users_df = spark.read.parquet("s3://lab3-bucket/processed/users/")

# Add UUIDs for DynamoDB key
songs_df = songs_df.withColumn("uuid", concat_ws("_", lit("song"), col("track_id")))
users_df = users_df.withColumn("uuid", concat_ws("_", lit("user"), col("user_id")))

# Combine and convert to DynamicFrame
combined_df = songs_df.unionByName(users_df, allowMissingColumns=True)
combined_dyf = DynamicFrame.fromDF(combined_df, glueContext, "combined")

# Write to DynamoDB
glueContext.write_dynamic_frame_from_options(
    frame=combined_dyf,
    connection_type="dynamodb",
    connection_options={
        "dynamodb.output.tableName": "lab3",
        "dynamodb.throughput.write.percent": "1.0"
    }
)

job.commit()