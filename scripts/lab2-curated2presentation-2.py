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

# Script generated for node Amazon Redshift-popular_locations_weekly
AmazonRedshiftpopular_locations_weekly_node1750166218775 = glueContext.create_dynamic_frame.from_options(connection_type="redshift", connection_options={"sampleQuery": "SELECT   CAST(DATE_TRUNC('week', a.booking_date) AS DATE) AS week_start,   b.cityname,   COUNT(*) AS total_bookings FROM curated.bookings a LEFT JOIN curated.apartment_attributes b   ON a.apartment_id = b.id WHERE a.booking_status LIKE '%confirmed%' GROUP BY week_start, b.cityname ORDER BY week_start, total_bookings DESC", "redshiftTmpDir": "s3://aws-glue-assets-520864643542-eu-west-2/temporary/", "useConnectionProperties": "true", "connectionName": "Redshift connection-new"}, transformation_ctx="AmazonRedshiftpopular_locations_weekly_node1750166218775")

# Script generated for node Amazon Redshift-total_bookings_per_user_weekly
AmazonRedshifttotal_bookings_per_user_weekly_node1750165332010 = glueContext.create_dynamic_frame.from_options(connection_type="redshift", connection_options={"sampleQuery": "SELECT      CAST(DATE_TRUNC('week', booking_date) AS DATE) AS week_start,user_id,     COUNT(*) AS total_bookings FROM curated.bookings WHERE booking_status LIKE '%confirmed%' GROUP BY CAST(DATE_TRUNC('week', booking_date) AS DATE),user_id", "redshiftTmpDir": "s3://aws-glue-assets-520864643542-eu-west-2/temporary/", "useConnectionProperties": "true", "connectionName": "Redshift connection-new"}, transformation_ctx="AmazonRedshifttotal_bookings_per_user_weekly_node1750165332010")

# Script generated for node Amazon Redshift-Average Booking Duration Weekly
AmazonRedshiftAverageBookingDurationWeekly_node1750165969895 = glueContext.create_dynamic_frame.from_options(connection_type="redshift", connection_options={"sampleQuery": "SELECT      CAST(DATE_TRUNC('week', checkin_date) AS DATE) AS week_start,     ROUND(AVG(DATEDIFF(day, checkin_date, checkout_date)), 2) AS avg_booking_duration_days FROM curated.bookings WHERE booking_status LIKE '%confirmed%' GROUP BY 1 ORDER BY 1", "redshiftTmpDir": "s3://aws-glue-assets-520864643542-eu-west-2/temporary/", "useConnectionProperties": "true", "connectionName": "Redshift connection-new"}, transformation_ctx="AmazonRedshiftAverageBookingDurationWeekly_node1750165969895")

# Script generated for node Amazon Redshift-top weeklylistings
AmazonRedshifttopweeklylistings_node1750160516761 = glueContext.create_dynamic_frame.from_options(connection_type="redshift", connection_options={"sampleQuery": "SELECT      CAST(DATE_TRUNC('week', booking_date) AS DATE) AS week_start,     apartment_id,     SUM(total_price) AS weekly_revenue FROM curated.bookings WHERE booking_status LIKE '%confirmed%' GROUP BY CAST(DATE_TRUNC('week', booking_date) AS DATE), apartment_id ORDER BY week_start", "redshiftTmpDir": "s3://aws-glue-assets-520864643542-eu-west-2/temporary/", "useConnectionProperties": "true", "connectionName": "Redshift connection-new"}, transformation_ctx="AmazonRedshifttopweeklylistings_node1750160516761")

# Script generated for node Amazon Redshift-avglistingprice
AmazonRedshiftavglistingprice_node1750165084063 = glueContext.create_dynamic_frame.from_options(connection_type="redshift", connection_options={"sampleQuery": "SELECT      CAST(DATE_TRUNC('week', listing_created_on) AS DATE) AS week_start,     ROUND(AVG(price), 2) AS avg_listing_price FROM curated.apartments WHERE is_active = TRUE GROUP BY 1 ORDER BY 1", "redshiftTmpDir": "s3://aws-glue-assets-520864643542-eu-west-2/temporary/", "useConnectionProperties": "true", "connectionName": "Redshift connection-new"}, transformation_ctx="AmazonRedshiftavglistingprice_node1750165084063")

# Script generated for node Amazon Redshift
AmazonRedshift_node1750167765011 = glueContext.create_dynamic_frame.from_options(connection_type="redshift", connection_options={"sampleQuery": "WITH user_bookings AS (     SELECT         user_id,         booking_date::DATE AS booking_date,         LEAD(booking_date::DATE) OVER (             PARTITION BY user_id              ORDER BY booking_date::DATE         ) AS next_booking_date     FROM curated.bookings     WHERE booking_status LIKE '%confirmed%' ), repeat_customers AS (     SELECT DISTINCT user_id     FROM user_bookings     WHERE next_booking_date IS NOT NULL       AND DATEDIFF(day, booking_date, next_booking_date) <= 30 ) SELECT     TO_CHAR(DATE_TRUNC('month', b.booking_date::DATE), 'YYYY-MM')   AS month,     COUNT(DISTINCT rc.user_id)                                      AS repeat_customers FROM curated.bookings b JOIN repeat_customers rc ON b.user_id = rc.user_id GROUP BY 1 ORDER BY 1", "redshiftTmpDir": "s3://aws-glue-assets-520864643542-eu-west-2/temporary/", "useConnectionProperties": "true", "connectionName": "Redshift connection-new"}, transformation_ctx="AmazonRedshift_node1750167765011")

# Script generated for node Amazon Redshift
AmazonRedshift_node1750167644183 = glueContext.create_dynamic_frame.from_options(connection_type="redshift", connection_options={"sampleQuery": "WITH bookings_calc AS (     SELECT         DATE_TRUNC('month', checkin_date)      AS month_start_date,         DATEDIFF(day, checkin_date, checkout_date) AS booked_nights     FROM curated.bookings     WHERE booking_status LIKE '%confirmed%' ) SELECT     month_start_date,     SUM(booked_nights)                       AS total_booked_nights,     COUNT(*)                                 AS num_bookings,     ROUND(         SUM(booked_nights)::DECIMAL / (COUNT(*) * 30.0),         4     ) AS occupancy_rate FROM bookings_calc GROUP BY 1 ORDER BY 1", "redshiftTmpDir": "s3://aws-glue-assets-520864643542-eu-west-2/temporary/", "useConnectionProperties": "true", "connectionName": "Redshift connection-new"}, transformation_ctx="AmazonRedshift_node1750167644183")

# Script generated for node Amazon Redshift-popular_locations_weekly
AmazonRedshiftpopular_locations_weekly_node1750166444287 = glueContext.write_dynamic_frame.from_options(frame=AmazonRedshiftpopular_locations_weekly_node1750166218775, connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-520864643542-eu-west-2/temporary/", "useConnectionProperties": "true", "dbtable": "presentation.popular_locations_weekly", "connectionName": "Redshift connection-new", "preactions": "CREATE TABLE IF NOT EXISTS presentation.popular_locations_weekly (week_start DATE, cityname VARCHAR, total_bookings BIGINT);"}, transformation_ctx="AmazonRedshiftpopular_locations_weekly_node1750166444287")

# Script generated for node Amazon Redshift-total_bookings_per_user_weekly
AmazonRedshifttotal_bookings_per_user_weekly_node1750165809516 = glueContext.write_dynamic_frame.from_options(frame=AmazonRedshifttotal_bookings_per_user_weekly_node1750165332010, connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-520864643542-eu-west-2/temporary/", "useConnectionProperties": "true", "dbtable": "presentation.total_bookings_per_user_weekly", "connectionName": "Redshift connection-new", "preactions": "CREATE TABLE IF NOT EXISTS presentation.total_bookings_per_user_weekly (week_start DATE, user_id INTEGER, total_bookings BIGINT);"}, transformation_ctx="AmazonRedshifttotal_bookings_per_user_weekly_node1750165809516")

# Script generated for node Amazon Redshift-Average Booking Duration Weekly
AmazonRedshiftAverageBookingDurationWeekly_node1750166173793 = glueContext.write_dynamic_frame.from_options(frame=AmazonRedshiftAverageBookingDurationWeekly_node1750165969895, connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-520864643542-eu-west-2/temporary/", "useConnectionProperties": "true", "dbtable": "presentation.avg_booking_duration_weekly", "connectionName": "Redshift connection-new", "preactions": "CREATE TABLE IF NOT EXISTS presentation.avg_booking_duration_weekly (week_start DATE, avg_booking_duration_days DECIMAL);"}, transformation_ctx="AmazonRedshiftAverageBookingDurationWeekly_node1750166173793")

# Script generated for node Amazon Redshift-listingweekly
AmazonRedshiftlistingweekly_node1750164747244 = glueContext.write_dynamic_frame.from_options(frame=AmazonRedshifttopweeklylistings_node1750160516761, connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-520864643542-eu-west-2/temporary/", "useConnectionProperties": "true", "dbtable": "presentation.avg_listing_price_weekly", "connectionName": "Redshift connection-new", "preactions": "CREATE TABLE IF NOT EXISTS presentation.avg_listing_price_weekly (week_start DATE, apartment_id INTEGER, weekly_revenue DOUBLE PRECISION);"}, transformation_ctx="AmazonRedshiftlistingweekly_node1750164747244")

# Script generated for node Amazon Redshift
AmazonRedshift_node1750165136660 = glueContext.write_dynamic_frame.from_options(frame=AmazonRedshiftavglistingprice_node1750165084063, connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-520864643542-eu-west-2/temporary/", "useConnectionProperties": "true", "dbtable": "presentation.avg_listing_price_weekly", "connectionName": "Redshift connection-new", "preactions": "CREATE TABLE IF NOT EXISTS presentation.avg_listing_price_weekly (week_start DATE, avg_listing_price DOUBLE PRECISION);"}, transformation_ctx="AmazonRedshift_node1750165136660")

# Script generated for node Amazon Redshift
AmazonRedshift_node1750168410041 = glueContext.write_dynamic_frame.from_options(frame=AmazonRedshift_node1750167765011, connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-520864643542-eu-west-2/temporary/", "useConnectionProperties": "true", "dbtable": "presentation.repeat_customer_rate", "connectionName": "Redshift connection-new", "preactions": "CREATE TABLE IF NOT EXISTS presentation.repeat_customer_rate (month VARCHAR, repeat_customers BIGINT);"}, transformation_ctx="AmazonRedshift_node1750168410041")

# Script generated for node Amazon Redshift
AmazonRedshift_node1750167726429 = glueContext.write_dynamic_frame.from_options(frame=AmazonRedshift_node1750167644183, connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-520864643542-eu-west-2/temporary/", "useConnectionProperties": "true", "dbtable": "presentation.occupancy_rate_monthly", "connectionName": "Redshift connection-new", "preactions": "CREATE TABLE IF NOT EXISTS presentation.occupancy_rate_monthly (month_start_date TIMESTAMP, total_booked_nights BIGINT, num_bookings BIGINT, occupancy_rate DECIMAL);"}, transformation_ctx="AmazonRedshift_node1750167726429")

job.commit()