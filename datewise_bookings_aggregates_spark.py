from pyspark.sql import SparkSession
from pyspark.sql.functions import col,to_date, count
from pyspark.sql.functions import unix_timestamp, from_unixtime
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark import SparkContext, SparkConf, SQLContext

# Initialize Spark Session

spark = SparkSession \
        .builder \
        .appName("datewise_booking")\
        .getOrCreate()

spark.sparkContext.setLogLevel("Error")


# read the batch data 
batch_df = spark.read.csv('/user/ec2-user/Data/Booking_Batch_Data/part-m-00000',inferSchema=True)

batch_df = batch_df.withColumnRenamed("_c0","booking_id")\
	   .withColumnRenamed("_c1","customer_id") \
	   .withColumnRenamed("_c2","driver_id") \
	   .withColumnRenamed("_c3","customer_app_version")  \
	   .withColumnRenamed("_c4","customer_phone_os_version") \
	   .withColumnRenamed("_c5","pickup_lat") \
	   .withColumnRenamed("_c6","pickup_lon") \
	   .withColumnRenamed("_c7","drop_lat") \
	   .withColumnRenamed("_c8","drop_lon") \
	   .withColumnRenamed("_c9","pickup_timestamp")  \
	   .withColumnRenamed("_c10","drop_timestamp")  \
	   .withColumnRenamed("_c11","trip_fare") \
	   .withColumnRenamed("_c12","tip_amount")  \
	   .withColumnRenamed("_c13","currency_code") \
	   .withColumnRenamed("_c14","cab_color")  \
	   .withColumnRenamed("_c15","cab_registration_no") \
	   .withColumnRenamed("_c16","customer_rating_by_driver")  \
	   .withColumnRenamed("_c17","rating_by_customer")  \
	   .withColumnRenamed("_c18","passenger_count")

# add new column date
batch_df = batch_df.withColumn("date", date_format('pickup_timestamp', "yyyy-MM-dd"))

#Aggregate Data
datewise_total_booking = batch_df.groupBy("date").count()


# Save Aggregated Data
datewise_total_booking.write.format('com.databricks.spark.csv').mode('overwrite').save('/user/ec2-user/Data/Datewise_Total_Bookings', header = 'true')

spark.stop()