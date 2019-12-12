import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.4 pyspark-shell' #org.apache.spark:spark-streaming-kafka-0-10_2.11:2.1.0,

spark = SparkSession \
    .builder \
    .appName("SSKafka") \
    .enableHiveSupport() \
    .config("spark.hadoop.dfs.client.use.datanode.hostname", "true") \
    .getOrCreate()

# default for startingOffsets is "latest", but "earliest" allows rewind for missed alerts
dsraw = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "loans") \
    .option("startingOffsets", "earliest") \
    .load()

ds = dsraw.selectExpr("CAST(value AS STRING)").withColumnRenamed("value", "name")


def process_row(row, row_id):
    row2 = row.withColumn("age", lit(0))
    row2.write.format('hive').mode("append").saveAsTable('managed_table_new_nt')


writeQuery = ds.writeStream.foreachBatch(process_row).start()
#
# writeQuery = ds \
#         .writeStream \
#         .format("console")\
#         .option("truncate", "false")\
#         .start()
#
writeQuery.awaitTermination()