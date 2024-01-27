from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


# Initialize Spark Session

spark = SparkSession \
        .builder \
        .appName("clickstream_data_from_Kafka")\
        .getOrCreate()


spark.sparkContext.setLogLevel('ERROR')


# Kafka Configs

host1 = "18.211.252.152"
port1 = 9092
topic1= "de-capstone3" 


clickStream_data = spark.readStream \
                   .format('kafka') \
                   .option("kafka.bootstrap.servers",host1+":"+str(port1))\
                   .option("startingOffsets","earliest")\
                   .option("subscribe",topic1)\
                   .option("failOnDataLoss",False)\
                   .load()


json_schema = StructType([StructField("customer_id",StringType()),
                          StructField("app_version",StringType()),
                         StructField("os_version",StringType()),
                         StructField("lat",StringType()),
                         StructField("page_id",StringType()),
                         StructField("button_id",StringType()),
                         StructField("is_button_click",StringType()),
                         StructField("is_page_view",StringType()),
                         StructField("is_scroll_up",StringType()),
                         StructField("is_scroll_down",StringType()),
                         StructField("timestamp",StringType())])


clickStream = clickStream_data.select(from_json(col("value").cast("string"),json_schema).alias("data")).select("data.*")

query = clickStream \
        .writeStream\
        .format("csv")\
        .outputMode("append")\
        .option("path","/user/ec2-user/Data/ClickStream")\
        .option("checkpointLocation","/user/ec2-user/Data/ClickStream_Check_Point")\
        .trigger(processingTime ='1 minute')\
        .start()

query1 = clickStream \
         .writeStream\
         .outputMode("append")\
         .format("console")\
         .option("truncate",False)\
         .start()


query.awaitTermination()
query1.awaitTermination()
