import findspark
findspark.init()

#    Spark
from pyspark import SparkContext, SparkConf
#    Spark Streaming
from pyspark.streaming import StreamingContext
#    Kafka
from pyspark.streaming.kafka import KafkaUtils
#    json parsing
import json


sc = SparkContext(appName="PythonSparkStreamingKafka_RM_01")
sc.setLogLevel("WARN")

ssc = StreamingContext(sc, 1)

kafkaStream = KafkaUtils.createStream(ssc, 'localhost:2181', 'kafka-to-hdfs', {'loans':1})
kafkaStream.pprint()
# parsed = kafkaStream.map(lambda v: json.loads(v[1]))
# parsed.count().map(lambda x:'Tweets in this batch: %s' % x).pprint()

ssc.start()
ssc.awaitTermination()
