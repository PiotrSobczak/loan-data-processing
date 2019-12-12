import findspark
findspark.init()
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, HiveContext

# SparkContext.setSystemProperty("hive.metastore.uris", "thrift://ch-3.dc-2.db.dcwp.pl:9083")

sparkSession = (SparkSession
                .builder
                .appName('example-pyspark-read-and-write-from-hive')
                .enableHiveSupport()
                .config("spark.hadoop.dfs.client.use.datanode.hostname", "true")
                # .config("hive.metastore.warehouse.dir", "/warehouse/tablespace/managed/hive/")

                .getOrCreate())
data = [('FirstA', 6), ('SecondA', 7), ('ThirdA', 8), ('FourthA', 9), ('FifthA', 10)]
df = sparkSession.createDataFrame(data, schema=["name", "age"])
df.show()
# df.write.format("orc").mode("append").saveAsTable("createdatabasesunscrapersXD123")

#
# # Write into Hive
df.write.format('hive').mode("append").saveAsTable('managed_table_new_nt')
# df.write.saveAsTable("managed_table_new", format="hive", mode="append")

# data = sparkSession.createDataFrame(Seq(("ZZ", "m:x", 34.0))).toDF("pv", "metric", "value")

# data.write.mode("append").saveAsTable("results_test_hive")
# println(sqlContext.sql("select * from results_test_hive").count())

# Read from Hive
# sparkSession.sql('create database sunscrapersXD')
df_load = sparkSession.sql('select * from results_test_hive')
df_load.show()