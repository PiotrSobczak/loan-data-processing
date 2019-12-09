import pandas as pd
import numpy as np

from kafka_producer.iterator import Iterator


def generate_df(num_rows):
    years = np.random.randint(2007, 2019, size=num_rows)
    months = np.random.randint(1, 12, size=num_rows)
    values = np.random.randint(-100, 100, size=num_rows)

    data = [["{}-{}-1".format(year, month), value]for year, month, value in zip(years, months, values)]
    df = pd.DataFrame(data, columns=['issue_d', 'value'])
    return df


def test_batch_iterator():
    DF_SIZE = 100

    df = generate_df(DF_SIZE)
    iterator = Iterator.from_df(df)

    assert len(iterator) == DF_SIZE
    assert isinstance(iterator, Iterator)
    assert isinstance(iterator._dataset, list)
    assert len(iterator._dataset) == DF_SIZE

