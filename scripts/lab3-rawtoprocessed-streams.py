import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame, DynamicFrameCollection
from pyspark.sql.functions import col, trim, lower, to_timestamp, concat_ws, lit

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

def MyTransform_streams(glueContext, dfc) -> DynamicFrameCollection:
    df = dfc.select(list(dfc.keys())[0]).toDF()
    df = df.withColumn("user_id", col("user_id").cast("int")) \
           .withColumn("track_id", trim(lower(col("track_id")))) \
           .withColumn("listen_time", to_timestamp(col("listen_time"))) \
           .dropna(subset=["user_id", "track_id", "listen_time"]) \
           .dropDuplicates(["user_id", "track_id", "listen_time"]) \
           .withColumn("record_type", lit("stream")) \
           .withColumn("uuid", concat_ws("_", lit("stream"), col("user_id"), col("track_id")))

    return DynamicFrameCollection({"cleanedstreams": DynamicFrame.fromDF(df, glueContext, "cleanedstreams")}, glueContext)

# Load from S3
raw_streams_dyf = glueContext.create_dynamic_frame.from_options(
    format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False},
    connection_type="s3",
    format="csv",
    connection_options={"paths": ["s3://lab3-bucket/raw/streams/"], "recurse": True},
    transformation_ctx="raw_streams_dyf"
)

transformed_dyf_coll = MyTransform_streams(glueContext, DynamicFrameCollection({"streams": raw_streams_dyf}, glueContext))
cleaned_streams_dyf = SelectFromCollection.apply(dfc=transformed_dyf_coll, key="cleanedstreams")

if cleaned_streams_dyf.count() >= 1:
    cleaned_streams_dyf = cleaned_streams_dyf.coalesce(1)

glueContext.write_dynamic_frame.from_options(
    frame=cleaned_streams_dyf,
    connection_type="s3",
    format="glueparquet",
    connection_options={"path": "s3://lab3-bucket/processed/streams/"},
    format_options={"compression": "uncompressed"},
    transformation_ctx="write_to_s3"
)

job.commit()
