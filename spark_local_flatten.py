from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


# Intialize Spark Session 
spark = SparkSession \
        .builder \
        .appName("CleanAndStructureClicksteamData")\
        .getOrCreate()


spark.sparkContext.setLogLevel('ERROR')


# Define schema
schema = StructType([
    StructField("customer_id",StringType()),
    StructField("app_version",StringType()),
    StructField("os_version",StringType()),
    StructField("lat",StringType()),
    StructField("lon",StringType()),
    StructField("page_id",StringType()),
    StructField("button_id",StringType()),
    StructField("is_button_click",StringType()),
    StructField("is_page_view",StringType()),
    StructField("is_scroll_up",StringType()),
    StructField("is_scroll_down",StringType()),
    StructField("timestamp",StringType()),
])


# Read data from local file system 

clickStream = spark.read.csv("/user/ec2-user/Data/ClickStream",schema= schema,header=True)


# Data Cleaning And Transformation

cleanedClickStream = clickStream \
                    .withColumn("customer_id",col("customer_id").cast("Integer"))\
                    .withColumn("lat", col("lat").cast("Double"))\
                    .withColumn("lon", col("lon").cast("Double"))\
                    .withColumn("is_button_click",col("is_button_click")==lit("Yes"))\
                    .withColumn("is_page_view",col("is_page_view")==lit("Yes"))\
                    .withColumn("is_scroll_up",col("is_scroll_up")==lit("Yes"))\
                    .withColumn("is_scroll_down",col("is_scroll_down")==lit("Yes"))\
                    .withColumn("timestamp",to_timestamp(col("timestamp")))\
                    .filter(col("customer_id").isNotNull())\
                    .dropDuplicates()


# Write Cleaned Data back to local file system

cleanedClickStream.write.csv("/user/ec2-user/Data/CleanedClickStream",mode="overwrite",header=True)

spark.stop()
                    
