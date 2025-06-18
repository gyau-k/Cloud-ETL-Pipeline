import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node MySQL
MySQL_node1750071163463 = glueContext.create_dynamic_frame.from_catalog(database="lab2-intermediate-db2", table_name="rental_db_apartments", transformation_ctx="MySQL_node1750071163463")

# Script generated for node MySQL
MySQL_node1749916282824 = glueContext.create_dynamic_frame.from_catalog(database="lab2-intermediate-db2", table_name="rental_db_apartment_attributes", transformation_ctx="MySQL_node1749916282824")

# Script generated for node MySQL
MySQL_node1750071164194 = glueContext.create_dynamic_frame.from_catalog(database="lab2-intermediate-db2", table_name="rental_db_bookings", transformation_ctx="MySQL_node1750071164194")

# Script generated for node MySQL
MySQL_node1750071164928 = glueContext.create_dynamic_frame.from_catalog(database="lab2-intermediate-db2", table_name="rental_db_user_viewing", transformation_ctx="MySQL_node1750071164928")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=MySQL_node1750071163463, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1750071142679", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
if (MySQL_node1750071163463.count() >= 1):
   MySQL_node1750071163463 = MySQL_node1750071163463.coalesce(1)
AmazonS3_node1750071205780 = glueContext.write_dynamic_frame.from_options(frame=MySQL_node1750071163463, connection_type="s3", format="glueparquet", connection_options={"path": "s3://lab2-intermediate-bucket/raw/apartments/", "partitionKeys": []}, format_options={"compression": "uncompressed"}, transformation_ctx="AmazonS3_node1750071205780")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=MySQL_node1749916282824, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1749916241003", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
if (MySQL_node1749916282824.count() >= 1):
   MySQL_node1749916282824 = MySQL_node1749916282824.coalesce(1)
AmazonS3_node1749916308825 = glueContext.write_dynamic_frame.from_options(frame=MySQL_node1749916282824, connection_type="s3", format="glueparquet", connection_options={"path": "s3://lab2-intermediate-bucket/raw/apartment_attributes/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3_node1749916308825")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=MySQL_node1750071164194, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1750071142679", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
if (MySQL_node1750071164194.count() >= 1):
   MySQL_node1750071164194 = MySQL_node1750071164194.coalesce(1)
AmazonS3_node1750071210596 = glueContext.write_dynamic_frame.from_options(frame=MySQL_node1750071164194, connection_type="s3", format="glueparquet", connection_options={"path": "s3://lab2-intermediate-bucket/raw/bookings/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3_node1750071210596")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=MySQL_node1750071164928, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1750071142679", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
if (MySQL_node1750071164928.count() >= 1):
   MySQL_node1750071164928 = MySQL_node1750071164928.coalesce(1)
AmazonS3_node1750071216113 = glueContext.write_dynamic_frame.from_options(frame=MySQL_node1750071164928, connection_type="s3", format="glueparquet", connection_options={"path": "s3://lab2-intermediate-bucket/raw/user_viewing/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3_node1750071216113")

job.commit()