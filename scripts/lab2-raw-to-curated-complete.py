import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrameCollection
from awsglue.dynamicframe import DynamicFrame
from awsglue import DynamicFrame

# Script generated for node apartments transformation
def MyTransform_apt(glueContext, dfc) -> DynamicFrameCollection:
    from awsglue.dynamicframe import DynamicFrameCollection, DynamicFrame
    from pyspark.sql.functions import col, trim, to_date
    from pyspark.sql.types import BooleanType, DoubleType
       
    df = dfc.select(list(dfc.keys())[0]).toDF()

    # Cast and clean data
    df = df.withColumn("price", col("price").cast(DoubleType()))
    df = df.withColumn("is_active", col("is_active").cast(BooleanType()))
    df = df.withColumn("listing_created_on", to_date(col("listing_created_on"),"dd/MM/yyyy"))
    df = df.withColumn("last_modified_date", trim(col("last_modified_timestamp")))
    # Trim strings
    df = df.withColumn("title", trim(col("title")))
    df = df.withColumn("source", trim(col("source")))
    df = df.withColumn("currency", trim(col("currency")))
    # Drop duplicates based on id and listing_created_on
    df = df.dropDuplicates(["id", "listing_created_on"])
    # Convert back to DynamicFrame
    cleaned_dyf = DynamicFrame.fromDF(df, glueContext, "cleaned_apartments")
    return DynamicFrameCollection({"CleanedApartments": cleaned_dyf}, glueContext)
# Script generated for node apartments_attributes transformation
def MyTransform_attr(glueContext, dfc) -> DynamicFrameCollection:
    from pyspark.sql.functions import col, to_date, lower, when,trim,regexp_replace
    from pyspark.sql.types import IntegerType, DoubleType, BooleanType

     
     # Get the input DynamicFrame (assumes name is "apartment_attributes")
    df = dfc.select("apartment_attributes")
    df = dfc.select(list(dfc.keys())[0]).toDF()
       

    # Apply transformations
    df_cleaned = df \
        .withColumn("id", col("id").cast(IntegerType())) \
        .withColumn("category", trim(col("category"))) \
        .withColumn("body", trim(col("body"))) \
        .withColumn("amenities", trim(col("amenities"))) \
        .withColumn("bathrooms", col("bathrooms").cast(DoubleType())) \
        .withColumn("bedrooms", col("bedrooms").cast(DoubleType())) \
        .withColumn("fee", col("fee").cast(DoubleType())) \
        .withColumn("has_photo", col("has_photo").cast(BooleanType())) \
        .withColumn("pets_allowed", col("pets_allowed").cast(BooleanType())) \
        .withColumn("price_display", regexp_replace("price_display", "[$,]", "").cast(DoubleType())) \
        .withColumn("price_type", trim(lower(col("price_type")))) \
        .withColumn("square_feet", col("square_feet").cast(IntegerType())) \
        .withColumn("address", trim(col("address"))) \
        .withColumn("cityname", trim(col("cityname"))) \
        .withColumn("state", trim(col("state"))) \
        .withColumn("latitude", col("latitude").cast(DoubleType())) \
        .withColumn("longitude", col("longitude").cast(DoubleType()))

    # Convert back to DynamicFrame
    dyf_cleaned = DynamicFrame.fromDF(df_cleaned, glueContext, "cleaned_apartment_attributes")

    # Return as a DynamicFrameCollection
    return DynamicFrameCollection({"apartment_attributes": dyf_cleaned}, glueContext)
# Script generated for node bookings transformation
def MyTransform_booking(glueContext, dfc) -> DynamicFrameCollection:
    from pyspark.sql.functions import col, to_date, trim
    from pyspark.sql.types import IntegerType, DoubleType
    from awsglue.dynamicframe import DynamicFrame, DynamicFrameCollection



    df = dfc.select("bookings")
    df = dfc.select(list(dfc.keys())[0]).toDF()
    # Convert to DataFrame for transformation


    df_cleaned = df \
        .withColumn("booking_id", col("booking_id").cast(IntegerType())) \
        .withColumn("user_id", col("user_id").cast(IntegerType())) \
        .withColumn("apartment_id", col("apartment_id").cast(IntegerType())) \
        .withColumn("booking_date", to_date(trim(col("booking_date")),"dd/MM/yyyy")) \
        .withColumn("checkin_date", to_date(trim(col("checkin_date")),"dd/MM/yyyy")) \
        .withColumn("checkout_date", to_date(trim(col("checkout_date")),"dd/MM/yyyy")) \
        .withColumn("total_price", col("total_price").cast(DoubleType())) \
        .withColumn("currency", trim(col("currency")).cast("string")) \
        .withColumn("booking_status", trim(col("booking_status")).cast("string"))

    dyf_cleaned = DynamicFrame.fromDF(df_cleaned, glueContext, "cleaned_bookings")
    return DynamicFrameCollection({"bookings": dyf_cleaned}, glueContext)
# Script generated for node user_viewing Transformation
def MyTransformusr_view(glueContext, dfc) -> DynamicFrameCollection:
    from pyspark.sql.functions import col, to_date, when,lower,trim
    from pyspark.sql.types import IntegerType, DoubleType
    from awsglue.dynamicframe import DynamicFrame, DynamicFrameCollection

    df = dfc.select(list(dfc.keys())[0]).toDF()
    df = (
        df.withColumn("user_id", col("user_id").cast(IntegerType()))
          .withColumn("apartment_id", col("apartment_id").cast(IntegerType()))
          .withColumn("viewed_at", to_date(trim(col("viewed_at")), "dd/MM/yyyy"))
          .withColumn("is_wishlisted",when(col("is_wishlisted").isNull(), False).otherwise(lower(col("is_wishlisted")).isin("true", "1", "yes")))
          .withColumn("call_to_action", when(col("call_to_action").isNull(), "Unknown").otherwise(col("call_to_action")))
    )
    # Drop duplicates where user_id and viewed_at are the same
    df_cleaned = df.dropDuplicates(["user_id", "viewed_at"])
    cleaned_dyf = DynamicFrame.fromDF(df_cleaned, glueContext, "cleaned_user_viewing")
    return DynamicFrameCollection({"CleanedUserViewing": cleaned_dyf}, glueContext)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Amazon Redshift-usr
AmazonRedshiftusr_node1750086728331 = glueContext.create_dynamic_frame.from_options(connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-520864643542-eu-west-2/temporary/", "useConnectionProperties": "true", "dbtable": "raws.user_viewing", "connectionName": "Redshift connection-new"}, transformation_ctx="AmazonRedshiftusr_node1750086728331")

# Script generated for node Amazon Redshift-apt
AmazonRedshiftapt_node1750086727093 = glueContext.create_dynamic_frame.from_options(connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-520864643542-eu-west-2/temporary/", "useConnectionProperties": "true", "dbtable": "raws.apartments", "connectionName": "Redshift connection-new"}, transformation_ctx="AmazonRedshiftapt_node1750086727093")

# Script generated for node Amazon Redshift-booking
AmazonRedshiftbooking_node1750086728107 = glueContext.create_dynamic_frame.from_options(connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-520864643542-eu-west-2/temporary/", "useConnectionProperties": "true", "dbtable": "raws.bookings", "connectionName": "Redshift connection-new"}, transformation_ctx="AmazonRedshiftbooking_node1750086728107")

# Script generated for node Amazon Redshift-attr
AmazonRedshiftattr_node1750086727716 = glueContext.create_dynamic_frame.from_options(connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-520864643542-eu-west-2/temporary/", "useConnectionProperties": "true", "dbtable": "raws.apartment_attributes", "connectionName": "Redshift connection-new"}, transformation_ctx="AmazonRedshiftattr_node1750086727716")

# Script generated for node user_viewing Transformation
user_viewingTransformation_node1750074250723 = MyTransformusr_view(glueContext, DynamicFrameCollection({"AmazonRedshiftusr_node1750086728331": AmazonRedshiftusr_node1750086728331}, glueContext))

# Script generated for node apartments transformation
apartmentstransformation_node1750074150139 = MyTransform_apt(glueContext, DynamicFrameCollection({"AmazonRedshiftapt_node1750086727093": AmazonRedshiftapt_node1750086727093}, glueContext))

# Script generated for node bookings transformation
bookingstransformation_node1750074319936 = MyTransform_booking(glueContext, DynamicFrameCollection({"AmazonRedshiftbooking_node1750086728107": AmazonRedshiftbooking_node1750086728107}, glueContext))

# Script generated for node apartments_attributes transformation
apartments_attributestransformation_node1750073470825 = MyTransform_attr(glueContext, DynamicFrameCollection({"AmazonRedshiftattr_node1750086727716": AmazonRedshiftattr_node1750086727716}, glueContext))

# Script generated for node Select From Collection-user_viewing
SelectFromCollectionuser_viewing_node1750074255820 = SelectFromCollection.apply(dfc=user_viewingTransformation_node1750074250723, key=list(user_viewingTransformation_node1750074250723.keys())[0], transformation_ctx="SelectFromCollectionuser_viewing_node1750074255820")

# Script generated for node Select From Collection- apartments
SelectFromCollectionapartments_node1750074200030 = SelectFromCollection.apply(dfc=apartmentstransformation_node1750074150139, key=list(apartmentstransformation_node1750074150139.keys())[0], transformation_ctx="SelectFromCollectionapartments_node1750074200030")

# Script generated for node Select From Collection-bookings
SelectFromCollectionbookings_node1750074313241 = SelectFromCollection.apply(dfc=bookingstransformation_node1750074319936, key=list(bookingstransformation_node1750074319936.keys())[0], transformation_ctx="SelectFromCollectionbookings_node1750074313241")

# Script generated for node Select From Collection- apartment attribute
SelectFromCollectionapartmentattribute_node1750073782442 = SelectFromCollection.apply(dfc=apartments_attributestransformation_node1750073470825, key=list(apartments_attributestransformation_node1750073470825.keys())[0], transformation_ctx="SelectFromCollectionapartmentattribute_node1750073782442")

# Script generated for node Amazon Redshift-userviewing
AmazonRedshiftuserviewing_node1750072060289 = glueContext.write_dynamic_frame.from_options(frame=SelectFromCollectionuser_viewing_node1750074255820, connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-520864643542-eu-west-2/temporary/", "useConnectionProperties": "true", "dbtable": "curated.user_viewing", "connectionName": "Redshift connection-new", "preactions": "CREATE TABLE IF NOT EXISTS curated.user_viewing (id VARCHAR, title VARCHAR, source VARCHAR, price DOUBLE PRECISION, currency VARCHAR, listing_created_on DATE, is_active BOOLEAN, last_modified_timestamp VARCHAR, last_modified_date VARCHAR);"}, transformation_ctx="AmazonRedshiftuserviewing_node1750072060289")

# Script generated for node Amazon Redshift-apartments
AmazonRedshiftapartments_node1750071896187 = glueContext.write_dynamic_frame.from_options(frame=SelectFromCollectionapartments_node1750074200030, connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-520864643542-eu-west-2/temporary/", "useConnectionProperties": "true", "dbtable": "curated.apartments", "connectionName": "Redshift connection-new", "preactions": "CREATE TABLE IF NOT EXISTS curated.apartments (id VARCHAR, title VARCHAR, source VARCHAR, price DOUBLE PRECISION, currency VARCHAR, listing_created_on DATE, is_active BOOLEAN, last_modified_date DATE);"}, transformation_ctx="AmazonRedshiftapartments_node1750071896187")

# Script generated for node Amazon Redshift-bookings
AmazonRedshiftbookings_node1750072055790 = glueContext.write_dynamic_frame.from_options(frame=SelectFromCollectionbookings_node1750074313241, connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-520864643542-eu-west-2/temporary/", "useConnectionProperties": "true", "dbtable": "curated.bookings", "connectionName": "Redshift connection-new", "preactions": "CREATE TABLE IF NOT EXISTS curated.bookings (booking_id INTEGER, user_id INTEGER, apartment_id INTEGER, booking_date DATE, checkin_date DATE, checkout_date DATE, total_price DOUBLE PRECISION, currency VARCHAR, booking_status VARCHAR);"}, transformation_ctx="AmazonRedshiftbookings_node1750072055790")

# Script generated for node Amazon Redshift- apartments_attr
AmazonRedshiftapartments_attr_node1749958422934 = glueContext.write_dynamic_frame.from_options(frame=SelectFromCollectionapartmentattribute_node1750073782442, connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-520864643542-eu-west-2/temporary/", "useConnectionProperties": "true", "dbtable": "curated.apartment_attributes", "connectionName": "Redshift connection-new", "preactions": "CREATE TABLE IF NOT EXISTS curated.apartment_attributes (id INTEGER, category VARCHAR, body VARCHAR, amenities VARCHAR, bathrooms DOUBLE PRECISION, bedrooms DOUBLE PRECISION, fee DOUBLE PRECISION, has_photo BOOLEAN, pets_allowed BOOLEAN, price_display DOUBLE PRECISION, price_type VARCHAR, square_feet INTEGER, address VARCHAR, cityname VARCHAR, state VARCHAR, latitude DOUBLE PRECISION, longitude DOUBLE PRECISION);"}, transformation_ctx="AmazonRedshiftapartments_attr_node1749958422934")

job.commit()