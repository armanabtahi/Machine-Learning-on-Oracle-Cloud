import oci
from oci.config import from_file 
import json
import sys
import pandas as pd
import io
import os
import pyarrow.parquet as pq
import pyarrow as pa
import oracledb
import math
import tempfile
import time

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
    
    def list_object(self,prefix):
        objects = []
        start=None
        while True:
            list_objects_response = self.object_storage_client.list_objects(namespace_name=self.namespace_name, 
                                                                        bucket_name=self.bucket_name,prefix=prefix,start=start)
            objects += list_objects_response.data.objects
            if not list_objects_response.data.next_start_with:
                break
            start = list_objects_response.data.next_start_with
        return objects
    
def filter_obj_list(object_list,from_date, to_date):
    l=[]
    for i, obj in enumerate(object_list):
        if (from_date<int(obj.name[-29:-19])) and (int(obj.name[-18:-8])<=to_date):
            l.append(object_list[i])
    return l
def main():
    
    env_str = sys.argv[1]
    adw_env_str = sys.argv[2]
    env = json.loads(env_str)
    adw_env = json.loads(adw_env_str)
    
    temp_dir = tempfile.gettempdir()
    
    OSDS=OS_Data_Store(env)

    get_object_response=OSDS.get_object(adw_env['tnsnames_dir'])
    tnsnames = io.BytesIO(get_object_response.data.content)
    with open(os.path.join(temp_dir, 'tnsnames.ora'), 'wb') as t:
        t.write(tnsnames.getbuffer())
    
    get_object_response=OSDS.get_object(adw_env['ewallet_dir'])
    ewallet = io.BytesIO(get_object_response.data.content)
    with open(os.path.join(temp_dir, 'ewallet.pem'), 'wb') as t:
        t.write(ewallet.getbuffer())


    pool=oracledb.create_pool(user=adw_env['user'],
                     password=adw_env['userpwd'],
                     dsn=adw_env['dsn'],
                     config_dir=temp_dir,
                     wallet_location=temp_dir,
                     wallet_password=adw_env['userpwd'],
                     min=3,
                     max=5,
                     increment=1)
    

    
    ################################
    # crypto
        # prepare the data for insertion
        
    ## reading Data from ADW to find the last ingestion
    sql_query_crypto = "SELECT MAX(timestamp) FROM crypto"
    with pool.acquire() as connection_adw:
        with connection_adw.cursor() as cursor:
            cursor.execute(sql_query_crypto)
            last_timestamp = cursor.fetchall()[0][0]/1000
    now = int(time.time())       
            
    list_objects_response=OSDS.list_object(adw_env['CW_ingest_path'])
    
    #limiting the list to unseen data in the table
    list_objects_response=filter_obj_list(list_objects_response,last_timestamp,now)
    
    data = []
    for obj in list_objects_response:
        if obj.name[-7:] == "parquet":
            get_object_response = OSDS.get_object(obj.name)
            parquet_table = pq.read_table(io.BytesIO(get_object_response.data.content))
            df = parquet_table.to_pandas()
            df = df.where(pd.notnull(df), None)
            data += df[['date', 'rate', 'volume', 'cap', 'liquidity']].values.tolist()

    # insert the data using MERGE statement to ignore duplicates
    if data:
        with pool.acquire() as connection_adw:
            with connection_adw.cursor() as cursor:
                try:
                    cursor.executemany("""
                        MERGE INTO Crypto c
                        USING (SELECT :1 AS timestamp, :2 AS rate, :3 AS volume, :4 AS cap, :5 AS liquidity FROM dual) d
                        ON (c.timestamp = d.timestamp)
                        WHEN NOT MATCHED THEN
                            INSERT (timestamp, rate, volume, cap, liquidity) VALUES (d.timestamp, d.rate, d.volume, d.cap, d.liquidity)""",
                        [(int(row[0]) if row[0] is not None else None,
                          float(row[1]) if row[1] is not None else None,
                          int(row[2]) if row[2] is not None else None,
                          int(row[3]) if row[3] is not None else None,
                         float(row[4]) if not math.isnan(row[4]) else None) for row in data])

                    # commit the transaction
                    connection_adw.commit()
                except Exception as e:
                    print("Error during insert:", e)
                    connection_adw.rollback()

    ################
    #news
    # prepare the data for insertion
    ## reading Data from ADW to find the last ingestion
    sql_query_news = "SELECT MAX(pubdate) FROM news"
    with pool.acquire() as connection_adw:
        with connection_adw.cursor() as cursor:
            cursor.execute(sql_query_news)
            last_timestamp = int(cursor.fetchall()[0][0].timestamp())
        
    list_objects_response=OSDS.list_object(adw_env['ND_ingest_path'])
    #limiting the list to unseen data in the table
    list_objects_response=filter_obj_list(list_objects_response,last_timestamp,now)
    
    data = []
    for obj in list_objects_response:
        if obj.name[-7:] == "parquet":
            get_object_response = OSDS.get_object(obj.name)
            parquet_table = pq.read_table(io.BytesIO(get_object_response.data.content))
            df = parquet_table.to_pandas()
            # replace None with empty string
            df = df.fillna('')
            if not df.empty:
                data += df[['pubDate', 'title', 'link', 'keywords', 'creator','video_url','description',
                            'content','image_url','source_id','category','country','language']].values.tolist()

    # insert the data using MERGE statement to ignore duplicates
    if data:
        with pool.acquire() as connection_adw:
            with connection_adw.cursor() as cursor:
                try:

                    cursor.executemany("""
                            MERGE INTO News c
                            USING (SELECT TO_DATE(:1, 'YYYY-MM-DD HH24:MI:SS') AS pubDate,
                                          :2 AS title, 
                                          :3 AS link, 
                                          :4 AS keywords, 
                                          :5 AS creator, 
                                          :6 AS video_url, 
                                          :7 AS description, 
                                          :8 AS content, 
                                          :9 AS image_url, 
                                          :10 AS source_id, 
                                          :11 AS category, 
                                          :12 AS country,
                                          :13 AS language
                            FROM dual) d
                            ON (c.pubDate = d.pubDate AND c.title = d.title )
                            WHEN NOT MATCHED THEN
                                INSERT (pubDate, title, link, keywords,creator,video_url,description,content,
                                image_url,source_id, category,country, language) 
                                VALUES (d.pubDate, d.title, d.link, d.keywords,d.creator,d.video_url,
                                d.description,d.content,d.image_url,d.source_id, d.category,d.country, d.language)""",
                            [(str(row[0]), str(row[1]),str(row[2]),str(row[3]),str(row[4]),str(row[5]),
                             row[6][:4000] if row[6] is not None else None,
                             row[7][:4000] if row[7] is not None else None,
                             str(row[8]),str(row[9]),str(row[10]),str(row[11]),str(row[12])) for row in data])
                    # commit the transaction
                    connection_adw.commit()
                except Exception as e:
                    print("Error during insert:", e)
                    connection_adw.rollback()

        
if __name__=="__main__": 
    main()