import requests
import json
import collections
import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5,org.mongodb.spark:mongo-spark-connector_2.11:2.4.1 --conf "spark.mongodb.input.uri=mongodb://127.0.0.1/mydata.data?readPreference=primaryPreferred" --conf "spark.mongodb.output.uri=mongodb://127.0.0.1/mydata.data" pyspark-shell'
import findspark
findspark.init()
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, from_unixtime, to_timestamp, collect_set, flatten, array, month, dayofmonth, hour, to_json, struct, count, collect_list, udf
from pyspark.sql.types import StringType
from datetime import datetime, timedelta
from schema import data_schema
from time import sleep
from states import states


sc = SparkContext()


spark = SparkSession\
          .builder\
          .appName('spark_streaming_app')\
          .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/mydata.data") \
          .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/mydata.data")\
          .getOrCreate()


def read_mongo(database, collection):

    return spark.read.format('mongo').options(host="mongo:27017", database=database, collection=collection).load()


def write_mongo(df, database, collection, mode):
    
    df.write\
      .format("mongo")\
      .options(host="mongo:27017", database=database, collection=collection)\
      .mode(mode).save()


def query1(df, beg, end):

    return df.filter(col('time').between(beg, end)).groupBy(col('group_country').alias('country')).agg(count('*').alias('count'))


def query2(df, beg, end):
  
    to_full_name = udf(lambda x: states[x.upper()], StringType())

    return df.filter(col('time').between(beg, end)).filter(col('group_country')=='us')\
             .withColumn('state', to_full_name('group_state')).groupBy(col('state').alias('state'))\
             .agg(collect_set('group_name'))


def query3(df, beg, end):

    most_common_topic = udf(lambda x: max(set(x), key=x.count))

    count_entrances = udf(lambda x: x.count(max(set(x), key=x.count)))

    return df.filter(col('time').between(beg, end)).groupBy(col('group_country').alias('country'))\
             .agg(flatten(collect_list('topic_name')).alias('list')).withColumn('topic', most_common_topic('list'))\
             .withColumn('count', count_entrances('list')).select(col('country'), col('topic'), col('count'))


def update_table(database, collection1, end, collection2, hours, query):

    df = read_mongo(database, collection1)

    beg = end - timedelta(hours=hours)

    df = query(df, beg, end)

    write_mongo(df, database, collection2, 'overwrite')


def main():

    prev_hour = datetime.now().hour
    
    while True:

        now = datetime.now()

        df = (spark.
            read.
            format('kafka').
            option('kafka.bootstrap.servers', '{ip1}:9092,{ip2}:9092,{ip3}:9092').
            option('subscribe', 'qwerty').
            option('startingOffsets', 'earliest').
            load())

        df = df.selectExpr('CAST(value as STRING)')
    
        df = df.select(from_json(col('value'), data_schema).alias('df'))

        df = df.select('df').withColumn('time', to_timestamp(from_unixtime(col('df.mtime')/1000)))

        database = 'project_db2'

        collection_general = 'all_data'

        end = now - timedelta(minutes=now.minute, seconds=now.second, microseconds=now.microsecond)
        beg = end - timedelta(hours=1 if abs(now.hour-prev_hour)<2 else 2)
        prev_hour = now.hour

        df = df.filter(col('time').between(beg, end)).\
                withColumn('month', month(col('time'))).\
                withColumn('day', dayofmonth(col('time'))).\
                withColumn('hour', hour(col('time')))

        df = df.select(col('df.group.group_country'), 
                       col('df.group.group_name'), 
                       col('df.group.group_state'),
                       col('df.group.group_topics.topic_name'), 
                       col('month'), col('day'), col('hour'), col('time'))

        write_mongo(df, database, collection_general, 'append')

        for h, collection, query in zip([6, 3, 6], ['country_data', 'country_groups_data', 'state_topics_data'], [query1, query2, query3]):

            update_table(database, collection_general, end, collection, h, query)

        sleep(3600)


if __name__=='__main__': main()