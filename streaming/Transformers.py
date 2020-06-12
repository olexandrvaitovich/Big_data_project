import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5 pyspark-shell'
import findspark
findspark.init()

import pyspark.sql.functions as f

def task_1(df):
 return df.select("parsed.group.group_country").\
          distinct().alias("unique").\
          select(f.struct(f.col("unique.group_country").alias("countrys")).alias("c")).\
   select(f.to_json("c").alias("value")).\
      writeStream.\
      format("kafka").\
      option("kafka.bootstrap.servers", '{ip1}:9092,{ip2}:9092,{ip3}:9092').\
      option("topic", "data_1").\
      option("checkpointLocation", "yab")

def task_2(df):
	return df.select("parsed.group.group_country", "parsed.group.group_city", "parsed.event.time").\
			withColumn("human_time", f.to_timestamp(
				             f.from_unixtime(f.col("time")/1000))).\
	        withWatermark("human_time", "10 hours").\
	        groupBy("group_country", f.window("human_time", "10 hours")).\
	        agg(f.struct(f.first("group_country").alias("country"), f.collect_set(f.col("group_city")).alias("citys")).alias("all")).\
	        select(f.to_json("all").alias("value")).\
	        writeStream.\
    		format("kafka").\
    		option("kafka.bootstrap.servers", '{ip1}:9092,{ip2}:9092,{ip3}:9092').\
    		option("topic", "data_2").\
    		option("checkpointLocation", "lok")

def task_3(df):
	return df.withColumn("time", f.to_timestamp(f.from_unixtime(f.col("parsed.event.time")/1000))).\
			select(
		           f.struct(
		           		 f.col("parsed.event.event_id").alias("event_id"),
		           	     f.col("parsed.event.event_name").alias("event_name"),
		           	     f.col("time"),
		           	     f.concat(f.col("parsed.group.group_topics.topic_name")).alias("topics"),
		           	     f.col("parsed.group.group_name").alias("group_name"),
		           	     f.col("parsed.group.group_city").alias("group_city"),
		           	     f.col("parsed.group.group_country").alias("group_country")
		           	    ).alias("all")
		            ).\
			select(f.to_json("all").alias("value")).\
			        writeStream.\
		    		format("kafka").\
		    		option("kafka.bootstrap.servers", '{ip1}:9092,{ip2}:9092,{ip3}:9092').\
		    		option("topic", "data_3").\
		    		option("checkpointLocation", "tovo903")

def task_4(df):
	return df.withWatermark("human_time", "10 hours").\
	        groupBy("parsed.group.group_city", f.window("human_time", "10 hours")).\
	        agg(
	        	f.struct(
		        			 f.first(f.col("parsed.group.group_city")).alias("city"),
		        		     f.collect_set(f.col("parsed.group.group_id")).alias("group_ids"),
		        		     f.collect_set(f.col("parsed.group.group_name")).alias("group_names")
	        	).alias("all")
	        ).\
	        select(f.to_json("all").alias("value")).\
			        writeStream.\
		    		format("kafka").\
		    		option("kafka.bootstrap.servers", '{ip1}:9092,{ip2}:9092,{ip3}:9092').\
		    		option("topic", "data_4").\
		    		option("checkpointLocation", "t7oj89")

def task_5(df):
	return df.withColumn("human_time", f.to_timestamp(
				             f.from_unixtime(f.col("parsed.event.time")/1000))).\
	        withWatermark("human_time", "10 hours").\
	        groupBy("parsed.group.group_id", f.window("human_time", "10 hours")).\
	        agg(
	        	f.struct(
	        			f.first(f.col("parsed.group.group_id")).alias("group_id"),
	        			f.collect_list(
		        			 f.struct(
			           		 f.col("parsed.event.event_id").alias("event_id"),
			           	     f.col("parsed.event.event_name").alias("event_name"),
			           	     f.col("human_time"),
			           	     f.concat(f.col("parsed.group.group_topics.topic_name")).alias("topics"),
			           	     f.col("parsed.group.group_name").alias("group_name"),
			           	     f.col("parsed.group.group_city").alias("group_city"),
			           	     f.col("parsed.group.group_country").alias("group_country")
			           	    )
	        			).alias("event")
	        		).alias("all")
	        	).\
	        select(f.to_json("all").alias("value")).\
			        writeStream.\
		    		format("kafka").\
		    		option("kafka.bootstrap.servers", '{ip1}:9092,{ip2}:9092,{ip3}:9092').\
		    		option("topic", "data_5").\
		    		option("checkpointLocation", "t50dsfsdf9024230")