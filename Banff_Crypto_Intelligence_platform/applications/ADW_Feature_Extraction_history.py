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

import re
import string
import nltk
from nltk.corpus import stopwords
from transformers import BertTokenizer, TFBertModel
import tensorflow as tf
nltk.download('stopwords')



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
    
def create_content(t,df_news,news_hour_befor=24):
    pt=news_hour_befor*60*60*1000
    # Convert embeddings to a numpy array
    mean_embeddings = df_news[(df_news['TIMESTAMP']<t) & (df_news['TIMESTAMP']>t-pt)].mean()[1:]
    return mean_embeddings     

def target(t,df_features,avg_rate_hour_after=24,hold_percent=2):
    ft=avg_rate_hour_after*60*60*1000
    rate=df_features[df_features['TIMESTAMP']==t]['RATE'].values[0]
    mean=df_features[(df_features['TIMESTAMP']<t+ft)&(df_features['TIMESTAMP']>t)]['RATE'].mean()
    change=100*(mean-rate)/(rate + 1e-6)
    return "BUY" if change > hold_percent else "SELL" if change < - hold_percent else "HOLD"

def get_embeddings(text):
    # Load the BERT tokenizer and model
    tokenizer = BertTokenizer.from_pretrained('bert-base-cased')
    model = TFBertModel.from_pretrained('bert-base-cased')
    #clean the text data
    text = text.lower() # convert text to lowercase
    text = re.sub(r'\d+', '', text) # remove digits
    text = text.translate(str.maketrans('', '', string.punctuation)) # remove punctuation
    text = ' '.join([word for word in text.split() if word not in stopwords.words('english')]) # remove stop words
    
    # Tokenize the text data using the BERT tokenizer
    tokens = tokenizer.encode_plus(
        text,
        max_length=512,
        truncation=True,
        padding='max_length',
        add_special_tokens=True,
        return_attention_mask=True,
        return_token_type_ids=False,
        return_tensors='tf'
    )
    
    # Define a function to extract the BERT embeddings from the model
    embeddings = model(tokens)[1][0]
    
    return embeddings.numpy()

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
    sql_query_crypto = "SELECT * FROM CRYPTO"
    sql_query_news = "SELECT * FROM NEWS"

    with pool.acquire() as connection_adw:
        with connection_adw.cursor() as cursor:
            cursor.execute(sql_query_crypto)
            result_crypto = cursor.fetchall()
            df_crypto = pd.DataFrame(result_crypto, columns=[desc[0] for desc in cursor.description])
            
    with pool.acquire() as connection_adw:
        with connection_adw.cursor() as cursor:        
            cursor.execute(sql_query_news)
            result_news = cursor.fetchall()
            df_news = pd.DataFrame(result_news, columns=[desc[0] for desc in cursor.description])


    ## df_news
    df_news['TIMESTAMP']=df_news['PUBDATE'].apply(lambda x: 1000*int(time.mktime(datetime.datetime.strptime(str(x), '%Y-%m-%d %H:%M:%S').timetuple())))
    df_news['CONTENTS']=df_news['DESCRIPTION'].astype(str).fillna('')+df_news['CONTENT'].astype(str).fillna('')
    df_news['EMBEDDINGS']=df_news['CONTENTS'].apply(get_embeddings)
    # convert NumPy arrays to lists of their elements
    df_news['EMBEDDINGS'] =df_news['EMBEDDINGS'].apply(lambda x: list(x))
    # create new DataFrame with 768 columns
    df_temp = pd.DataFrame(df_news['EMBEDDINGS'].apply(pd.Series))
    # rename columns
    df_temp.columns = [f'EMBD{i+1}' for i in range(768)]
    df_news = pd.merge(df_news,df_temp, left_index=True, right_index=True)
    
    remove_feats=['DESCRIPTION', 'CONTENT','CONTENTS' ,'PUBDATE', 'TITLE','LINK', 'KEYWORDS', 'CREATOR', 'VIDEO_URL', 'IMAGE_URL', 'SOURCE_ID', 'CATEGORY', 'COUNTRY', 'LANGUAGE','EMBEDDINGS']
    df_news=df_news.drop(remove_feats,axis=1)
    
    ## df_features
    df_features=df_crypto.copy()
    df_mean_embeddings=df_features['TIMESTAMP'].apply(lambda x: create_content(x,df_news,news_hour_befor=24))
    df_features = pd.merge(df_features,df_mean_embeddings, left_index=True, right_index=True)
    
    ## target
    df_features['TARGET']=df_features['TIMESTAMP'].apply(lambda x: target(x,df_features,avg_rate_hour_after=24,hold_percent=0.5))
    df_features=df_features.dropna()
    
    column_defs = ',\n'.join([f'{col} VARCHAR2(64)' if col == 'TARGET' else f'{col} NUMBER' for col in df_features.columns])
    sql_create_query_features = f'CREATE TABLE FEATURES (\n{column_defs}\n)'
    
    # insert the data into the new table
    sql_insert_query = f"INSERT INTO FEATURES ({', '.join(df_features.columns)}) VALUES ({', '.join([':' + str(i+1) for i in range(len(df_features.columns))])})"

    with pool.acquire() as connection_adw:
        with connection_adw.cursor() as cursor:
            try:
                # create the table
                cursor.execute(sql_create_query_features)

                # insert the data into the table
                cursor.executemany(sql_insert_query, df_features.values.tolist())

                # commit the changes
                connection_adw.commit()
            except Exception as e:
                print("Error during insert:", e)
                connection_adw.rollback()


if __name__=="__main__": 
    main()