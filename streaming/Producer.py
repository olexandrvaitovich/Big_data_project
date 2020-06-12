#!/usr/bin/env python
import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5 pyspark-shell'
import findspark
findspark.init()

from kafka import KafkaConsumer
import pymongo
import json
import pyspark.sql.functions as f
from pyspark.sql.functions import col
from pyspark.sql.types import StructType,StringType,LongType,IntegerType,ArrayType, FloatType,StructField
from pyspark import SparkContext
from pyspark.sql import SparkSession

from schema import data_schema
from Transformers import *
from datetime import datetime



if __name__ == "__main__":
	spark = SparkSession.builder.appName("app").getOrCreate()

	df = spark.readStream.\
	           format("kafka").\
	           option("kafka.bootstrap.servers", '{ip1}:9092,{ip2}:9092,{ip3}:9092').\
	           option("startingOffsets", "earliest").\
	           option("subscribe", "meetups").\
	           load().\
	           select(f.from_json(f.col("value").cast("string"), data_schema).alias("parsed")).\
	           withColumn("human_time", f.to_timestamp(
				             f.from_unixtime(f.col("parsed.event.time")/1000)))

	df_1 = task_1(df)
	df_1 = df_1.start()

	df_2 = task_2(df)
	df_2 = df_2.start()

	df_3 = task_3(df)
	df_3 = df_3.start()

	df_4 = task_4(df)
	df_4 = df_4.start()

	df_5 = task_5(df)
	df_5 = df_5.start()

	df_1.awaitTermination()
	df_2.awaitTermination()
	df_3.awaitTermination()
	df_4.awaitTermination()
	df_5.awaitTermination()
	
	print("DONE")