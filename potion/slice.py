import pandas as pd
from datetime import date
from .database import Database

class Slice(Database):

    def __init__(self, path, date=None, lookback=250):
        super().__init__(path)
        self.date = self.latest_date if date == None else min([self.latest_date, date])
        self.lookback = lookback
        self.files = sorted(list(path.rglob('*.csv')), key=lambda x: x.stem, reverse=True)[0:lookback]
        self.data = Slice.build_data(self)

    def build_data(self):
        df = pd.DataFrame()
        for file in self.files:
            read = pd.read_csv(file, index_col=0).filter(regex="Date|Symbol|Log Returns")
            df = pd.concat([df, read])
        return df.reset_index(drop=True)
        
