import requests
import pandas as pd
from io import StringIO
from datetime import date, datetime, timedelta


class Database:
    def __init__(self, path):
        self.url_volt = lambda date: f"https://www.nseindia.com/archives/nsccl/volt/CMVOLT_{date.strftime('%d%m%Y')}.CSV"
        self.data_store = path
        self.latest_date = Database.get_latest_date(self)


    def get_latest_date(self):
        files = list(self.data_store.rglob('*.csv'))
        latest_date = date(2011,3,28)
        if (len(files) > 0):
            latest_file = sorted(files, key=lambda x: x.stem, reverse=True)[0]
            latest_date = datetime.strptime(latest_file.stem, '%Y%m%d').date()
        return latest_date


    def fetch(self, date):
        url = self.url_volt(date)
        r = requests.get(url)
        r.raise_for_status()
        df = pd.read_csv(StringIO(r.content.decode('utf-8')), index_col=None, header=0)
        return df
        

    def save(self, date):
        df = Database.fetch(self, date)
        path = self.data_store.joinpath(
            date.strftime('%Y'), 
            date.strftime('%b').upper(),
            f"{date.strftime('%Y%m%d')}.csv"
            )
        path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(path.absolute())
        print (f"Saved {path.absolute()}")


    def update(self):
        current_date = self.latest_date + timedelta(days=1)
        while current_date <= date.today():
            try:
                Database.save(self, current_date)
                self.latest_date = current_date
            except Exception as e:
                print (str(e))
                pass
            current_date += timedelta(days=1)

# https://nseindia.com/marketinfo/companyTracker/compInfo.jsp?symbol=MINDTREE&series=EQ
            
            

        
