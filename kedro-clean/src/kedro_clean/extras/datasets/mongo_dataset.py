from kedro.io import AbstractDataset
import pandas as pd
from pymongo import MongoClient

class MongoDataset(AbstractDataset):
    def __init__(self, uri: str, database: str, collection: str):
        self.uri = uri
        self.database = database
        self.collection = collection

    def _load(self):
        client = MongoClient(self.uri)
        db = client[self.database]
        collection = db[self.collection]
        data = list(collection.find())
        return pd.DataFrame(data)

    def _save(self, data: pd.DataFrame) -> None:
        pass

    def _exists(self) -> bool:
        return True

    def _describe(self) -> dict:
        return {"uri": self.uri, "database": self.database, "collection": self.collection}