class Iterator:
    def __init__(self, dataset):
        self._dataset = dataset
        self._size = len(dataset)

    def __len__(self):
        return int(self._size)

    def __call__(self):
        for elem in self._dataset:
            yield elem

    @staticmethod
    def from_df(df):
        df["json"] = df.apply(lambda x: x.to_json(), axis=1)
        json_list = df["json"].tolist()
        return Iterator(json_list)
