import findspark
findspark.init()

import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.4 pyspark-shell' #org.apache.spark:spark-streaming-kafka-0-10_2.11:2.1.0,

from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("SSKafka") \
    .getOrCreate()

# default for startingOffsets is "latest", but "earliest" allows rewind for missed alerts
dsraw = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "loans") \
    .option("startingOffsets", "earliest") \
    .load()

ds = dsraw.selectExpr("CAST(value AS STRING)")

writeQuery = ds \
        .writeStream \
        .format("console")\
        .option("truncate", "false")\
        .start()

writeQuery.awaitTermination()