import json
import datetime
import time

import pandas as pd
from kafka import KafkaProducer

from kafka_producer.iterator import Iterator

TOPIC_NAME = "loans"
SLEEP_INTERVAL = 1
CSV_FILE = "/home/psobczak/PycharmProjects/LoanAnalysis/loan_mini.csv"
BOOTSTRAP_SERVERS = ['localhost:9092']


def process_df(df):
    assert isinstance(df, pd.DataFrame)
    df_filtered = df.dropna(how='all', axis='columns')
    df_filtered['issue_d'] = df_filtered['issue_d'].apply(lambda x: datetime.datetime.strptime(x, '%b-%Y'))
    df_sorted = df_filtered.sort_values(by=['issue_d'])
    return df_sorted


if __name__ == "__main__":
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda m: json.dumps(m).encode('ascii')
    )

    df = pd.read_csv(CSV_FILE, low_memory=False)
    df_processed = process_df(df)
    iterator = Iterator.from_df(df)

    for json_msg in iterator():
        producer.send(TOPIC_NAME, json_msg)
        time.sleep(SLEEP_INTERVAL)


