
---

# Click Stream Data Processing with Kafka, Spark, and Hive

## Overview
This project is focused on processing click stream and booking data using Kafka, Spark, and Hive in an AWS EMR environment. It encompasses streaming data processing from Kafka, data transformations in HDFS, and batch data processing with comprehensive data analysis using Hive queries.

## Key Steps

1. **EMR Cluster Creation on AWS**: Set up an Elastic MapReduce (EMR) cluster on AWS for large-scale data processing.

2. **SSH Connection to EMR Cluster**: Access the EMR cluster securely through SSH.

3. **Spark-Kafka Script Execution**: Transfer and execute the `spark_kafka_to_local.py` script to read data from Kafka and write it to HDFS.

4. **Data Transformation**: Process Kafka data in HDFS using `spark_local_flatten.py`.

5. **Batch Data Ingestion with Sqoop**: Transfer batch data from AWS RDS to HDFS.

6. **Hive Table Creation and Data Loading**: 
   - Create Hive tables for bookings, clickstream, and datewise total bookings.
   - Load data from HDFS into these Hive tables.

7. **Data Analysis with Hive Queries**: Perform various analyses, including:
   - Calculating different drivers for each customer.
   - Counting total rides taken by each customer.
   - Extracting samples from clickstream data.
   - Determining conversion ratios from clickstream.
   - Counting trips by cab color.
   - Summarizing tips given date-wise.
   - Identifying bookings with low customer ratings.
   - Counting iOS users among customers.

## Technologies Used

- AWS EMR
- Apache Spark
- Apache Kafka
- Hadoop (HDFS)
- Apache Sqoop
- Apache Hive

---
