import json
import datetime
import time

import pandas as pd
from kafka import KafkaProducer

from kafka_producer.iterator import Iterator

TOPIC_NAME = "loans"
SLEEP_INTERVAL = 1
CSV_FILE = "/home/psobczak/PycharmProjects/LoanAnalysis/loan_small.csv"
BOOTSTRAP_SERVERS = ['localhost:9092']


def process_df(df):
    assert isinstance(df, pd.DataFrame)
    df_filtered = df.dropna(how='all', axis='columns')
    df_filtered['issue_y'] = df_filtered['issue_d'].apply(lambda x: datetime.datetime.strptime(x, '%b-%Y').year)
    df_filtered['issue_m'] = df_filtered['issue_d'].apply(lambda x: datetime.datetime.strptime(x, '%b-%Y').month)
    df_sorted = df_filtered.sort_values(by=['issue_y', 'issue_m'])
    return df_sorted


if __name__ == "__main__":
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda m: m.encode('utf-8')
    )

    df = pd.read_csv(CSV_FILE, low_memory=False)
    df_processed = process_df(df)
    iterator = Iterator.from_df(df_processed)

    for json_msg in iterator():
        producer.send(TOPIC_NAME, json_msg)
        print(json_msg)
        time.sleep(SLEEP_INTERVAL)


