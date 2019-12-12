import os
import argparse
import json

import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col
from pyspark.sql.functions import *

from kafka_consumer.utils import load_schema

parser = argparse.ArgumentParser()
parser.add_argument("-c", "--config", type=str, required=True)
args = parser.parse_args()

config = json.load(open(args.config))


def write_batch_to_hive(row, _):
    row.write.format('hive').mode("append").saveAsTable(config["hive_table"])
    row.show(truncate=False)


os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.4 pyspark-shell'
SCHEMA_PATH = "schema.json"

schema = load_schema(config["schema"])

"""Create Sparksession instance"""
spark = SparkSession \
    .builder \
    .appName("KafkaToHive") \
    .enableHiveSupport() \
    .config("spark.hadoop.dfs.client.use.datanode.hostname", "true") \
    .getOrCreate()

"""Create Streaming Dataset from kafka topic"""
dsraw = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", config["bootstrap_servers"]) \
    .option("subscribe", config["topic"]) \
    .load()

"""Reading the dataset and converting jsons to Dataframes"""
ds = dsraw.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

"""Writing Dataframe batches to hive """
writeQuery = ds.writeStream.foreachBatch(write_batch_to_hive).start()
writeQuery.awaitTermination()