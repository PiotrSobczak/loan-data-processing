import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit,col
from pyspark.sql.types import *
from pyspark.sql.functions import *

import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.4 pyspark-shell' #org.apache.spark:spark-streaming-kafka-0-10_2.11:2.1.0,

spark = SparkSession \
    .builder \
    .appName("SSKafka") \
    .enableHiveSupport() \
    .config("spark.hadoop.dfs.client.use.datanode.hostname", "true") \
    .getOrCreate()


# default for startingOffsets is "latest", but "earliest" allows rewind for missed alerts

smallBatchSchema = spark.read.json("schema.json").schema

dsraw = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "loans-json-dev") \
    .load() # .option("startingOffsets", "earliest") \
# ds = dsraw.selectExpr("CAST(value AS STRING) as json")
ds = dsraw.select(from_json(col("value").cast("string"), smallBatchSchema).alias("data")).select("data.*")
# ds1 = dsraw.selectExpr("CAST(value AS STRING) as json")
# ds2 = ds.select( from_json(ds.json, schema=smallBatchSchema).as("data")).select("data.*")
# ds = dsraw.select(from_json(col('value').cast("string"), smallBatchSchema))
# ds = ds2.select( spark.read.json("json"))


def spark_to_hive_type_mapping(spark_type):
    if spark_type == 'StringType':
        return "string"
    elif spark_type == "LongType":
        return "int"
    elif spark_type == "DoubleType":
        return "double"
    else:
        raise Exception("Unknown datatype {}.".format(spark_type))


def generate_hive_table_definition(name, schema):
    create_statement_str = "CREATE TABLE {} {}"

    tuples = ["({} {})".format(field.name, spark_to_hive_type_mapping(str(field.dataType))) for field in schema.fields]
    return create_statement_str.format(name, ",".join(tuples))

print(generate_hive_table_definition("loans", ds.schema))


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