import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame, DynamicFrameCollection
from pyspark.sql.functions import col, trim, lower, to_date, current_date, datediff, when, concat_ws, lit

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Transform SONGS
def MyTransform_songs(glueContext, dfc):
    df = dfc.select(list(dfc.keys())[0]).toDF()
    df = df.withColumn("explicit", when(lower(trim(col("explicit"))) == "true", True)
                                   .when(lower(trim(col("explicit"))) == "false", False)
                                   .otherwise(col("explicit").cast("boolean")))

    string_cols = ["track_id", "artists", "album_name", "track_name", "track_genre"]
    for col_name in string_cols:
        df = df.withColumn(col_name, trim(lower(col(col_name))))

    df = df.withColumn("popularity", col("popularity").cast("int")) \
           .withColumn("duration_ms", col("duration_ms").cast("int")) \
           .withColumn("danceability", col("danceability").cast("double")) \
           .withColumn("energy", col("energy").cast("double")) \
           .withColumn("key", col("key").cast("int")) \
           .withColumn("loudness", col("loudness").cast("double")) \
           .withColumn("mode", col("mode").cast("int")) \
           .withColumn("speechiness", col("speechiness").cast("double")) \
           .withColumn("acousticness", col("acousticness").cast("double")) \
           .withColumn("instrumentalness", col("instrumentalness").cast("double")) \
           .withColumn("liveness", col("liveness").cast("double")) \
           .withColumn("valence", col("valence").cast("double")) \
           .withColumn("tempo", col("tempo").cast("double")) \
           .withColumn("time_signature", col("time_signature").cast("int"))

    df = df.dropDuplicates(["track_id"]).dropna(subset=["track_id", "track_name"])
    df = df.withColumn("record_type", lit("song"))
    return DynamicFrame.fromDF(df, glueContext, "songs_cleaned")

# Transform USERS
def MyTransform_users(glueContext, dfc):
    df = dfc.select(list(dfc.keys())[0]).toDF()
    df = df.withColumn("user_id", col("user_id").cast("int")) \
           .withColumn("user_age", col("user_age").cast("int")) \
           .withColumn("user_name", trim(lower(col("user_name")))) \
           .withColumn("user_country", trim(lower(col("user_country")))) \
           .withColumn("created_at", to_date(col("created_at"))) \
           .dropna(subset=["user_id", "user_name", "created_at"]) \
           .dropDuplicates(["user_id"]) \
           .withColumn("account_age_days", datediff(current_date(), col("created_at"))) \
           .withColumn("record_type", lit("user"))
    return DynamicFrame.fromDF(df, glueContext, "users_cleaned")

# Read from raw CSV
songs_dyf = glueContext.create_dynamic_frame.from_options(
    format_options={"withHeader": True},
    connection_type="s3",
    format="csv",
    connection_options={"paths": ["s3://lab3-bucket/raw/songs/songs.csv"]},
    transformation_ctx="songs_dyf"
)
users_dyf = glueContext.create_dynamic_frame.from_options(
    format_options={"withHeader": True},
    connection_type="s3",
    format="csv",
    connection_options={"paths": ["s3://lab3-bucket/raw/users/users.csv"]},
    transformation_ctx="users_dyf"
)

# Transform
songs_cleaned = MyTransform_songs(glueContext, DynamicFrameCollection({"default": songs_dyf}, glueContext))
users_cleaned = MyTransform_users(glueContext, DynamicFrameCollection({"default": users_dyf}, glueContext))

# Save transformed data to S3 (Parquet)
glueContext.write_dynamic_frame.from_options(
    frame=songs_cleaned,
    connection_type="s3",
    connection_options={"path": "s3://lab3-bucket/processed/songs/"},
    format="parquet"
)
glueContext.write_dynamic_frame.from_options(
    frame=users_cleaned,
    connection_type="s3",
    connection_options={"path": "s3://lab3-bucket/processed/users/"},
    format="parquet"
)

job.commit()