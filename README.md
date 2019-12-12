# loan-data-analysis-pipeline

This project is a pipeline for stream processing and data analysis of [loan data](https://www.kaggle.com/wendykan/lending-club-loan-data).


### Architecture

### Components
- kafka producer which simulates streaming loan data
- spark structured streaming kafka consumer which processess streaming data and saves it to hive
- kafka cluster
- hadoop cluster with hive

### Dependencies
- PySpark ver. >= 2.4
- Kafka cluster ver. >= 2.0
- Hadoop cluster with Hive ver ver. >= 1.0 (Here [hortonworks-sandbox](https://github.com/PiotrSobczak/hortonworks-sandbox-plus) was used)
- Jupyter notebook


### Run kafka producer
```
python3 -m kafka_producer.producer -c kafka_producer/config.json
```

### Run kafka consumer
```
python3 -m kafka_consumer.consumer -c kafka_consumer/config.json
```

### Run tests
```
pytest <REPOSITORY_PATH>
```
