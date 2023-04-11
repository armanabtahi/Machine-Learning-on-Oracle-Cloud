import oci
import json
import sys
import pandas as pd
import io
import os
import oracledb
import math
import tempfile
import datetime
import numpy as np

from ads import set_auth
from ads.common.model_metadata import UseCaseType
from ads.model.framework.sklearn_model import SklearnModel

from sklearn.impute import SimpleImputer
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import make_pipeline
from sklearn.compose import  make_column_transformer
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import TimeSeriesSplit

from sklearn.metrics import accuracy_score
from sklearn.metrics import classification_report

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
    
def TimeSeries_CV(model,df,k=3):

    # Split the data into k folds using TimeSeriesSplit
    tscv = TimeSeriesSplit(n_splits=k)

    cv_accuracy = []
    cv_report = []

    for train_index, val_index in tscv.split(df):

        # Split the data into training and validation sets for this fold
        train_data = df.iloc[train_index]
        val_data = df.iloc[val_index]

        # Train model
        X_train = train_data.drop('TARGET', axis=1)
        y_train = train_data['TARGET']
        model.fit(X_train, y_train)

        # Make predictions 
        X_val = val_data.drop('TARGET', axis=1)
        y_val = val_data['TARGET']
        y_pred = model.predict(X_val)

        accuracy = accuracy_score(y_val, y_pred)
        report=classification_report(y_val, y_pred)

        cv_accuracy.append(accuracy)
        cv_report.append(report)
    return cv_accuracy,cv_report            


def deploy_model(estimator,df,env,DS_env):
    X, y = df.drop( ['TARGET'], axis=1), df["TARGET"]
    estimator.fit(X, y)
    set_auth(auth='resource_principal')
    artifact_dir = tempfile.mkdtemp()
    
    current_time = datetime.datetime.now()
    current_time_str = current_time.strftime('%Y-%m-%d %H:%M:%S')
    display_name=f"Random Forest {current_time_str}"
    
    model = SklearnModel(estimator=estimator, artifact_dir=artifact_dir)
    model.prepare(
                inference_conda_env="generalml_p37_cpu_v1",
                training_conda_env="generalml_p37_cpu_v1",
                use_case_type=UseCaseType.MULTINOMIAL_CLASSIFICATION,
                X_sample=X,
                y_sample=y,
                as_onnx=False,
                force_overwrite=True,
                deployment_instance_shape = DS_env['deployment_instance_shape'],
                deployment_ocpus=2,
                deployment_memory_in_gbs = 16,
                deployment_bandwidth_mbps = 10,
                compartment_id=env['compartment_id'],
                project_id=DS_env['project_id']
        
    )
    model.verify(X[:10])["prediction"]
    model_id =model.save(display_name=display_name)
    try:
        model.deploy(display_name=display_name)
    except Exception as e:
                print("Error during deloyment:", e)
    
    resource_principals_signer = oci.auth.signers.get_resource_principals_signer()
    DS=oci.data_science.DataScienceClient({},signer=resource_principals_signer)
    list_model_deployments_response=DS.list_model_deployments(
        compartment_id=env['compartment_id'],
        project_id=DS_env['project_id'],
        display_name=display_name
        )
    response=list_model_deployments_response.data
    model_deployment_url=response[0].model_deployment_url
    model_deployment_id=response[0].id
    model_id=response[0].model_deployment_configuration_details.model_configuration_details.model_id
    deploy_response={
        'project_id':DS_env['project_id'],
        'model_deployment_url':model_deployment_url,
        'model_name':display_name,
        'model_deployment_id':model_deployment_id,
        'model_id':model_id,
    }
    return deploy_response

def model_result_to_adw(pool,cv_accuracy,cv_report,deploy_response):
    table_name = 'model_results'    
    columns = ['datetime',
               'model_name',
               'project_id', 
               'model_id', 
               'deploy_id', 
               'deploy_URL', 
               'average_accuracy', 
               'fold_accuracy', 
               'fold_report']
    
    sql_create_query = f"CREATE TABLE {table_name} ({', '.join([c + ' VARCHAR(2000)' for c in columns])})"
    sql_insert_query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES (:1, :2, :3, :4, :5, :6,:7,:8,:9)"
    
    datetime_now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    avg_accuracy = np.mean(cv_accuracy)
        
    data=[datetime_now,
          deploy_response['model_name'],
          deploy_response['project_id'],
          deploy_response['model_id'],
          deploy_response['model_deployment_id'],
          deploy_response['model_deployment_url'],
          str(avg_accuracy), 
          str(cv_accuracy), 
          str(cv_report)]
    
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

                
def main():
    
    env_str = sys.argv[1]
    adw_env_str = sys.argv[2]
    DS_env_str = sys.argv[3]
    env = json.loads(env_str)
    adw_env = json.loads(adw_env_str)
    DS_env = json.loads(DS_env_str)
    
    ## Connecting to ADW
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
    
    ## reading Data from ADW and creating DataFrame
    sql_query_features = "SELECT * FROM Features"
    with pool.acquire() as connection_adw:
        with connection_adw.cursor() as cursor:
            cursor.execute(sql_query_features)
            result_features = cursor.fetchall()
            df = pd.DataFrame(result_features, columns=[desc[0] for desc in cursor.description])

    df=df.sort_values(by='TIMESTAMP')
    
    ## Processing data    
    numeric_features=['RATE','VOLUME','CAP','LIQUIDITY']
    drop_features=['TIMESTAMP']
    passthrough_features=list(df.columns[5:-1])
    numeric_transformer=make_pipeline(SimpleImputer(strategy="median"),StandardScaler()) 

    preprocessor=make_column_transformer(
        (numeric_transformer,numeric_features),
        ('passthrough', passthrough_features),
        ('drop',drop_features)
        )
    
    ## make a model
    pipe_RF = make_pipeline(preprocessor,RandomForestClassifier(n_estimators=100))  
#    pipe_RF = RandomForestClassifier(n_estimators=100)  
    
    ## cross validating the model and results in adw
    cv_accuracy,cv_report=TimeSeries_CV(pipe_RF,df,k=4)
    
    ## deploy the model
    deploy_response=deploy_model(pipe_RF,df,env,DS_env)

    ## save details to a table in ADW
    model_result_to_adw(pool,cv_accuracy,cv_report,deploy_response)
    


if __name__=="__main__": 
    main()