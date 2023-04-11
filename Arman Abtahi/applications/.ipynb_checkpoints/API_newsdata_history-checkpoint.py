import oci
from oci.config import from_file 
import datetime
import time
import requests
import json
import sys
import pandas as pd

def iso8601_to_timestamp(date_string=None):
    if date_string is None:
        date_time_obj = datetime.datetime.now()
    else:
        date_time_obj = datetime.datetime.fromisoformat(date_string)
    timestamp = int(date_time_obj.timestamp())
    return timestamp
    
def iso_8601_dates(from_date, to_date, interval_hour=24):
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

class NewsData:
    def __init__(self,NewsData_env):
        self.key=NewsData_env["api_key_datanews"]
        self.country=NewsData_env["country"]
        self.language=NewsData_env["language"]
        self.q=NewsData_env["q"]


    def make_request(self,interval):
        self.url=f"https://newsdata.io/api/1/archive?apikey={self.key}&country={self.country}&language={self.language}&q={self.q}&from_date={interval[0]}&to_date={interval[1]}"
        response = requests.get(self.url)
        
        data=response.json()
    
        self.TimeStampOfIngestion         = iso8601_to_timestamp()
        self.TimeStampOfLatestCreatedAt   = iso8601_to_timestamp(interval[1])
        self.TimeStampOfEarliestCreatedAt = iso8601_to_timestamp(interval[0])
        return data
    
    def make_file_name(self):
        filename="rn{}_{}_{}.parquet".format(self.TimeStampOfIngestion,self.TimeStampOfEarliestCreatedAt,self.TimeStampOfLatestCreatedAt)
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
    
    
def main():
    
    env_str = sys.argv[1]
    NewsData_env_str = sys.argv[2]
    
    env = json.loads(env_str)
    NewsData_env = json.loads(NewsData_env_str)
    
    intervals=iso_8601_dates(NewsData_env["from_date"],NewsData_env["to_date"],NewsData_env["interval_hour"])


    ND=NewsData(NewsData_env)
    OSDS=OS_Data_Store(env)
    for inter in intervals:
        ND_data=ND.make_request(inter)
        ND_filename=ND.make_file_name()
        
        #write into parquet
        try:
            df=pd.DataFrame(ND_data['results'], index=None)
            df.to_parquet(ND_filename)
            with open(ND_filename, 'rb') as f:
                OSDS.create_object(f,'{}/{}'.format(NewsData_env["ND_ingest_path"],ND_filename))


        
    
    
if __name__=="__main__": 
    main()