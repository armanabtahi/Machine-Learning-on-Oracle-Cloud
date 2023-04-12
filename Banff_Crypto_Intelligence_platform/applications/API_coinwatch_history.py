import oci
from oci.config import from_file 
import datetime
import time
import requests
import json
import sys
import pandas as pd


def time_stamp(x):
    if x=='now':
        return round(time.time())
    else:
        dt = datetime.datetime.strptime(x, '%Y-%m-%d %H:%M:%S')
        return round(dt.timestamp())


class CoinWatch:
    def __init__(self,CoinWatch_env):
        self.url='https://api.livecoinwatch.com/coins/single/history'
        self.currency=CoinWatch_env["currency"]
        self.code=CoinWatch_env["code"]
        self.key=CoinWatch_env["api_key_coinwatch"]
    def make_request(self,interval_timestamp):
        
        self.TimeStampOfIngestion         = iso8601_to_timestamp()
        self.TimeStampOfEarliestCreatedAt = iso8601_to_timestamp(interval_timestamp[0])
        self.TimeStampOfLatestCreatedAt   = iso8601_to_timestamp(interval_timestamp[1])
        
        data = json.dumps({"currency":self.currency,"code":self.code,"start":self.TimeStampOfEarliestCreatedAt*1000,"end":self.TimeStampOfLatestCreatedAt*1000,"meta":False})
        headers = {'content-type': 'application/json','x-api-key': self.key}
        r = requests.post(self.url, data=data, headers=headers)

        return r.json()
    
    def make_file_name(self):
        filename=f"rc{self.TimeStampOfIngestion}_{self.TimeStampOfEarliestCreatedAt}_{self.TimeStampOfLatestCreatedAt}.parquet"
        return filename


class OS_Data_Store:
    def __init__(self,env):
        resource_principals_signer = oci.auth.signers.get_resource_principals_signer()
        self.object_storage_client = oci.object_storage.ObjectStorageClient({},signer=resource_principals_signer)
        self.namespace_name = env["namespace_name"]
        self.compartment_id = env["compartment_id"]
        self.bucket_name    = env["bucket_name"]
        
    def create_object(self, object_body, path):
        response=self.object_storage_client.put_object(namespace_name = self.namespace_name,bucket_name= self.bucket_name,object_name= path,put_object_body = object_body) 
        return response    
    
    def get_object(self,path):
        response= self.object_storage_client.get_object(namespace_name=self.namespace_name,bucket_name= self.bucket_name,object_name=path)
        return response
    
def iso8601_to_timestamp(date_string=None):
    if date_string is None:
        date_time_obj = datetime.datetime.now()
    else:
        date_time_obj = datetime.datetime.fromisoformat(date_string)
    timestamp = int(date_time_obj.timestamp())
    return timestamp

def iso_8601_dates(from_date, to_date, interval_hour=8):
    start_date = datetime.datetime.fromisoformat(from_date)
    end_date = datetime.datetime.fromisoformat(to_date)
    
    current_date = start_date
    dates_list = []
    
    while current_date <= end_date:
        dates_list.append(current_date.isoformat())
        current_date += datetime.timedelta(hours=interval_hour)
    
    interval_list = []
    for i in range(0, len(dates_list)-1, 1):
        interval_list.append((dates_list[i], dates_list[i+1]))
        
    return interval_list


def main():
    
    env_str = sys.argv[1]
    CoinWatch_env_str = sys.argv[2]
    
    env = json.loads(env_str)
    CoinWatch_env = json.loads(CoinWatch_env_str)
    
    intervals=iso_8601_dates(CoinWatch_env["from_date"],CoinWatch_env["to_date"],CoinWatch_env["interval_hour"])
    
    CW=CoinWatch(CoinWatch_env)
    OSDS=OS_Data_Store(env)
    for inter in intervals:
        CW_data=CW.make_request(inter)
        CW_filename=CW.make_file_name()
        #write into parquet
        df=pd.DataFrame(CW_data['history'], index=None)
        df.to_parquet(CW_filename)
        with open(CW_filename, 'rb') as f:
            OSDS.create_object(f,'{}/{}'.format(CoinWatch_env["CW_ingest_path"],CW_filename))
        
if __name__=="__main__": 
    main()