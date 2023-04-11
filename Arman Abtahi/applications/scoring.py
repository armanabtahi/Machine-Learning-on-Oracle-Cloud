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
import datetime
import time

import requests



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
    

def model_result_to_adw(pool,df_last_row,pred,endpoint):
    table_name = 'Predictions'    
    columns = ['TIMESTAMP',
               'DATETIME',
               'RATE',
               'DECISION',  
               'DEPLOYED_URL']

    sql_create_query = f"CREATE TABLE {table_name} (TIMESTAMP NUMBER, DATETIME VARCHAR(64), RATE NUMBER, DECISION VARCHAR(64), DEPLOYED_URL VARCHAR(256) )"

    sql_insert_query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES (:1, :2, :3, :4, :5)"

    dt_utc = datetime.datetime.utcfromtimestamp(df_last_row['TIMESTAMP'].values[0]/1000).strftime('%Y-%m-%d %H:%M:%S.%f UTC')


    data=[int(df_last_row['TIMESTAMP'].values[0]),
          dt_utc,
          float(df_last_row['RATE'].values[0]),
          pred['prediction'][0],
          endpoint]

    with pool.acquire() as connection_adw:
        with connection_adw.cursor() as cursor:
            try:
                # check if the table exists
                cursor.execute(f"SELECT * FROM {table_name}")
            except:
                # create the table if it doesn't exist
                cursor.execute(sql_create_query)

            try:   
                # insert the data into the table
                cursor.execute(sql_insert_query, data)

                # commit the changes
                connection_adw.commit()
            except Exception as e:
                print("Error during insert:", e)
                connection_adw.rollback()

def get_endpoint(pool):
    sql_query_last_row='''SELECT *
                FROM (
                  SELECT *
                  FROM model_results
                  ORDER BY DATETIME DESC
                )
                WHERE ROWNUM  < 3'''
    with pool.acquire() as connection_adw:
        with connection_adw.cursor() as cursor:
            cursor.execute(sql_query_last_row)
            two_last_rows= cursor.fetchall()
            df_last_two_rows = pd.DataFrame(two_last_rows, columns=[desc[0] for desc in cursor.description])
    
    endpoints=df_last_two_rows['DEPLOY_URL'].values.tolist()
    return endpoints


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
    # get last data
    sql_query_last_row='''SELECT *
                    FROM (
                      SELECT *
                      FROM FEATURES
                      ORDER BY TIMESTAMP DESC
                    )
                    WHERE ROWNUM = 1'''
    with pool.acquire() as connection_adw:
        with connection_adw.cursor() as cursor:
            cursor.execute(sql_query_last_row)
            last_row= cursor.fetchall()
            df_last_row = pd.DataFrame(last_row, columns=[desc[0] for desc in cursor.description])
    body = df_last_row.to_json(orient='records')
    # 
    # ask for prediction
    auth = oci.auth.signers.get_resource_principals_signer()
    endpoints=get_endpoint(pool)
    
    # I look at the last URL, if did not work, use previous URL
    try:
        endpoint=endpoints[0]+'/predict'
        pred=requests.post(endpoint, json=body, auth=auth).json()
        # save to adw
        model_result_to_adw(pool,df_last_row,pred,endpoint)
    
    except Exception as e:
        print("Latest URL does not work, moving to previous URL", e)
        endpoint=endpoints[1]+'/predict'
        pred=requests.post(endpoint, json=body, auth=auth).json()
        # save to adw
        model_result_to_adw(pool,df_last_row,pred,endpoint)
    
    
if __name__=="__main__": 
    main()