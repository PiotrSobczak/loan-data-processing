import datetime

import pandas as pd

from kafka_producer.producer import process_df


def test_process_df_drop_nans():
    data = [['Apr-2016', float('NaN')], ['Dec-2018', float('NaN')], ['Mar-2014', float('NaN')]]
    df = pd.DataFrame(data, columns=['issue_d', 'nan_column'])

    df_processed = process_df(df)
    assert "nan_column" not in df_processed.columns.tolist()


def test_process_df_sort():
    data = [['Apr-2010', 1], ['Nov-2017', 2], ['May-2012', 3]]
    df = pd.DataFrame(data, columns=['issue_d', 'value'])

    df_processed = process_df(df)

    assert df_processed["value"].tolist() == [1, 3, 2]


def test_process_df_convert():
    data = [['Apr-2010', 1], ['Nov-2017', 2], ['May-2012', 3]]
    df = pd.DataFrame(data, columns=['issue_d', 'value'])

    df_processed = process_df(df)
    assert "issue_y" in df_processed.columns
    assert "issue_m" in df_processed.columns

