import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Amazon S3-usrview
AmazonS3usrview_node1750071884136 = glueContext.create_dynamic_frame.from_options(format_options={}, connection_type="s3", format="parquet", connection_options={"paths": ["s3://lab2-intermediate-bucket/raw/user_viewing/"], "recurse": True}, transformation_ctx="AmazonS3usrview_node1750071884136")

# Script generated for node Amazon S3 attr
AmazonS3attr_node1749958340085 = glueContext.create_dynamic_frame.from_options(format_options={}, connection_type="s3", format="parquet", connection_options={"paths": ["s3://lab2-intermediate-bucket/raw/apartment_attributes/"], "recurse": True}, transformation_ctx="AmazonS3attr_node1749958340085")

# Script generated for node Amazon S3-booking
AmazonS3booking_node1750071883503 = glueContext.create_dynamic_frame.from_options(format_options={}, connection_type="s3", format="parquet", connection_options={"paths": ["s3://lab2-intermediate-bucket/raw/bookings/"], "recurse": True}, transformation_ctx="AmazonS3booking_node1750071883503")

# Script generated for node Amazon S3
AmazonS3_node1750071882588 = glueContext.create_dynamic_frame.from_options(format_options={}, connection_type="s3", format="parquet", connection_options={"paths": ["s3://lab2-intermediate-bucket/raw/apartments/"], "recurse": True}, transformation_ctx="AmazonS3_node1750071882588")

# Script generated for node Change Schema-USER_VIEWING
ChangeSchemausrview_node1750103958588 = ApplyMapping.apply(frame=AmazonS3usrview_node1750071884136, mappings=[("user_id", "int", "user_id", "varchar"), ("apartment_id", "int", "apartment_id", "varchar"), ("viewed_at", "string", "viewed_at", "varchar"), ("is_wishlisted", "string", "is_wishlisted", "varchar"), ("call_to_action", "string", "call_to_action", "varchar")], transformation_ctx="ChangeSchemausrview_node1750103958588")

# Script generated for node Change Schema-apartments_attr
ChangeSchemaattr_node1750103793580 = ApplyMapping.apply(frame=AmazonS3attr_node1749958340085, mappings=[("id", "int", "id", "varchar"), ("category", "string", "category", "varchar"), ("body", "string", "body", "varchar"), ("amenities", "string", "amenities", "varchar"), ("bathrooms", "int", "bathrooms", "varchar"), ("bedrooms", "int", "bedrooms", "varchar"), ("fee", "decimal", "fee", "varchar"), ("has_photo", "boolean", "has_photo", "varchar"), ("pets_allowed", "boolean", "pets_allowed", "varchar"), ("price_display", "string", "price_display", "varchar"), ("price_type", "string", "price_type", "varchar"), ("square_feet", "int", "square_feet", "varchar"), ("address", "string", "address", "varchar"), ("cityname", "string", "cityname", "varchar"), ("state", "string", "state", "varchar"), ("latitude", "decimal", "latitude", "varchar"), ("longitude", "decimal", "longitude", "varchar")], transformation_ctx="ChangeSchemaattr_node1750103793580")

# Script generated for node Change Schema-booking
ChangeSchemabooking_node1750102983926 = ApplyMapping.apply(frame=AmazonS3booking_node1750071883503, mappings=[("booking_id", "int", "booking_id", "varchar"), ("user_id", "int", "user_id", "varchar"), ("apartment_id", "int", "apartment_id", "varchar"), ("booking_date", "string", "booking_date", "varchar"), ("checkin_date", "string", "checkin_date", "varchar"), ("checkout_date", "string", "checkout_date", "varchar"), ("total_price", "decimal", "total_price", "varchar"), ("currency", "string", "currency", "varchar"), ("booking_status", "string", "booking_status", "varchar")], transformation_ctx="ChangeSchemabooking_node1750102983926")

# Script generated for node Change Schema-apt
ChangeSchemaapt_node1750103124142 = ApplyMapping.apply(frame=AmazonS3_node1750071882588, mappings=[("id", "int", "id", "varchar"), ("title", "string", "title", "varchar"), ("source", "string", "source", "varchar"), ("price", "decimal", "price", "varchar"), ("currency", "string", "currency", "varchar"), ("listing_created_on", "string", "listing_created_on", "varchar"), ("is_active", "string", "is_active", "varchar"), ("last_modified_timestamp", "string", "last_modified_timestamp", "varchar")], transformation_ctx="ChangeSchemaapt_node1750103124142")

# Script generated for node Amazon Redshift-userviewing
AmazonRedshiftuserviewing_node1750072060289 = glueContext.write_dynamic_frame.from_options(frame=ChangeSchemausrview_node1750103958588, connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-520864643542-eu-west-2/temporary/", "useConnectionProperties": "true", "dbtable": "raws.user_viewing", "connectionName": "Redshift connection", "preactions": "CREATE TABLE IF NOT EXISTS raws.user_viewing (user_id VARCHAR, apartment_id VARCHAR, viewed_at VARCHAR, is_wishlisted VARCHAR, call_to_action VARCHAR);"}, transformation_ctx="AmazonRedshiftuserviewing_node1750072060289")

# Script generated for node Amazon Redshift- apartments_attr
AmazonRedshiftapartments_attr_node1749958422934 = glueContext.write_dynamic_frame.from_options(frame=ChangeSchemaattr_node1750103793580, connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-520864643542-eu-west-2/temporary/", "useConnectionProperties": "true", "dbtable": "raws.apartment_attributes", "connectionName": "Redshift connection", "preactions": "CREATE TABLE IF NOT EXISTS raws.apartment_attributes (id VARCHAR, category VARCHAR, body VARCHAR, amenities VARCHAR, bathrooms VARCHAR, bedrooms VARCHAR, fee VARCHAR, has_photo VARCHAR, pets_allowed VARCHAR, price_display VARCHAR, price_type VARCHAR, square_feet VARCHAR, address VARCHAR, cityname VARCHAR, state VARCHAR, latitude VARCHAR, longitude VARCHAR);"}, transformation_ctx="AmazonRedshiftapartments_attr_node1749958422934")

# Script generated for node Amazon Redshift-bookings
AmazonRedshiftbookings_node1750072055790 = glueContext.write_dynamic_frame.from_options(frame=ChangeSchemabooking_node1750102983926, connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-520864643542-eu-west-2/temporary/", "useConnectionProperties": "true", "dbtable": "raws.bookings", "connectionName": "Redshift connection", "preactions": "CREATE TABLE IF NOT EXISTS raws.bookings (booking_id VARCHAR, user_id VARCHAR, apartment_id VARCHAR, booking_date VARCHAR, checkin_date VARCHAR, checkout_date VARCHAR, total_price VARCHAR, currency VARCHAR, booking_status VARCHAR);"}, transformation_ctx="AmazonRedshiftbookings_node1750072055790")

# Script generated for node Amazon Redshift-apartments
AmazonRedshiftapartments_node1750071896187 = glueContext.write_dynamic_frame.from_options(frame=ChangeSchemaapt_node1750103124142, connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-520864643542-eu-west-2/temporary/", "useConnectionProperties": "true", "dbtable": "raws.apartments", "connectionName": "Redshift connection", "preactions": "CREATE TABLE IF NOT EXISTS raws.apartments (id VARCHAR, title VARCHAR, source VARCHAR, price VARCHAR, currency VARCHAR, listing_created_on VARCHAR, is_active VARCHAR, last_modified_timestamp VARCHAR);"}, transformation_ctx="AmazonRedshiftapartments_node1750071896187")

job.commit()