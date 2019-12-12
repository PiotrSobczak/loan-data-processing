import os

import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit,col
from pyspark.sql.functions import *

from kafka_consumer.utils import load_schema

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.4 pyspark-shell' #org.apache.spark:spark-streaming-kafka-0-10_2.11:2.1.0,

spark = SparkSession \
    .builder \
    .appName("SSKafka") \
    .enableHiveSupport() \
    .config("spark.hadoop.dfs.client.use.datanode.hostname", "true") \
    .getOrCreate()

SCHEMA_PATH = "schema.json"

schema = load_schema(SCHEMA_PATH)


dsraw = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "loans-json-dev") \
    .load()

ds = dsraw.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")


def process_row(row, row_id):
    # row2 = row.withColumn("age", lit(0))
    # row2.write.format('hive').mode("append").saveAsTable('managed_table_new_nt')
    # row2.write.mode("overwrite").format("text").save("/tmp")
    # row \
    #         .write \
    #         .format("console")\
    #         .option("truncate", "false")\
    #         .start()
    row.show(truncate=False)
    # row2.show()


# writeQuery = ds.writeStream.foreachBatch(process_row).start()
#
writeQuery = ds \
        .writeStream \
        .format("console")\
        .option("truncate", "false")\
        .start()

writeQuery.awaitTermination()