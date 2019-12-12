import datetime
import time
import random
import json
import argparse

import pandas as pd
from kafka import KafkaProducer

from kafka_producer.iterator import Iterator


def process_df(df):
    assert isinstance(df, pd.DataFrame)
    """Dropping columns which contain only nan values"""
    df_filtered = df.dropna(how='all', axis='columns')

    """Renaming the unnamed column to id"""
    df_renamed = df_filtered.rename(columns={"Unnamed: 0": "id"})

    """Extracting year and month from issue_d and saving as seperate columns"""
    df_renamed['issue_y'] = df_renamed['issue_d'].apply(lambda x: datetime.datetime.strptime(x, '%b-%Y').year)
    df_renamed['issue_m'] = df_renamed['issue_d'].apply(lambda x: datetime.datetime.strptime(x, '%b-%Y').month)

    """Sorting the data by Year and Month"""
    df_sorted = df_renamed.sort_values(by=['issue_y', 'issue_m'])
    return df_sorted


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--config", type=str, required=True)
    args = parser.parse_args()

    config = json.load(open(args.config))

    """Creating KafkaProducer instance"""
    producer = KafkaProducer(
        bootstrap_servers=config["bootstrap_servers"],
        value_serializer=lambda m: m.encode('utf-8')
    )

    """Loading the data"""
    df = pd.read_csv(config["csv_file"], low_memory=False)

    """Processing the dataframe"""
    df_processed = process_df(df)

    """Creating an iterator"""
    iterator = Iterator.from_df(df_processed)

    """Iterating over data and sending to kafka cluster with random sleep to simulate real-time streaming"""
    for json_msg in iterator():
        """Send a single row to kafka as json"""
        producer.send(config["topic"], json_msg)

        """Print the sent msg"""
        print(json_msg)

        """Sleep for duration between 0.5s and 1.5s"""
        time.sleep(config["min_sleep_interval"] + random.random())
