import oci
import logging
import datetime
import pytz
from oci.exceptions import ServiceError
#from oci.config import from_file 
        
        
def config(env,key_files,config_uri):
    config = {
        "user": env["user"],
        "key_file": key_files["key_file"],
        "fingerprint": key_files["fingerprint"],
        "tenancy": env["compartment_id"],
        "region": env["region"]
    }
    header="[DEFAULT]\n"
    with open(config_uri, "w") as file:
        file.write(header)
        for key, value in config.items():
            file.write(f"{key}={value}\n")
    
    #config = from_file(file_location=config_uri)
    return config


def path2uri(env,path):
    return "oci://{}@{}/{}".format(env["bucket_name"],env["namespace_name"],path)

###############
###############
###############    
class schedule_time:
        
    def next_sharp_hour_datetime(self,hours=0, days=0, weeks=0):
        # Get the current UTC time as a datetime object
        now = datetime.datetime.now(datetime.timezone.utc)
        # Add the specified number of hours, days, and months to the current time
        future = now + datetime.timedelta(hours=hours, days=days, weeks=weeks)
        # Round the time to the next sharp hour
        next_hour = (future.replace(second=0, microsecond=0, minute=0, hour=future.hour)
                     + datetime.timedelta(hours=1))
        formatted_datetime = next_hour.strftime('%Y-%m-%dT%H:%M:%S+00:00')
        return formatted_datetime
    
    def next_sharp_hour_timestamp_ms(self,hours=0, days=0, weeks=0):
        # Get the current UTC time as a datetime object
        now = datetime.datetime.now(datetime.timezone.utc)
        # Add the specified number of hours, days, and months to the current time
        future = now + datetime.timedelta(hours=hours, days=days, weeks=weeks)
        # Round the time to the next sharp hour
        next_hour = (future.replace(second=0, microsecond=0, minute=0, hour=future.hour)
                     + datetime.timedelta(hours=1))

        # Get the timestamp in seconds and convert it to milliseconds
        formatted_timestamp_ms = int(next_hour.timestamp() * 1000)
        return formatted_timestamp_ms
    
###############
###############
###############
class Identity:
    def __init__(self,config):
        self.config=config
        logging.warning('>>>Identity object initialized<<<')

###############
###############
###############
class Policy:
    def __init__(self,identity,env):
        self.identity_client=oci.identity.IdentityClient(identity.config)
        logging.warning('>>>Identity Client<<<\n')
        self.compartment_id=env["compartment_id"]
        
    def create(self,name,statements,description):
        
        list_policies_response=self.identity_client.list_policies(compartment_id=self.compartment_id,
                                                                 name=name)
        
        if list_policies_response.data:
            logging.warning('>>>Policy already existed<<<')
            self.policy_id=list_policies_response.data[0].id
            update_policy_details=oci.identity.models.UpdatePolicyDetails(statements=statements)
            update_policy_response=self.identity_client.update_policy(policy_id=self.policy_id,
                                                                     update_policy_details=update_policy_details)
            logging.warning('>>>Policy statements updated: {}<<<'.format(statements))
            logging.warning('>>>Policy ID : {}<<<\n'.format(self.policy_id))
            return update_policy_response
        else:
            create_policy_details=oci.identity.models.CreatePolicyDetails(
            compartment_id=self.compartment_id,
            description=description,
            name=name,
            statements=statements
            )
            create_policy_response=self.identity_client.create_policy(
            create_policy_details=create_policy_details
            )
            logging.warning('>>>Policy statements : {}<<<'.format(statements))
            self.policy_id=create_policy_response.data.id
            logging.warning('>>>Policy ID : {}<<<\n'.format(self.policy_id))
            return create_policy_response

###############
###############
###############

class Events:
    def __init__(self,identity,env):
        self.event_client=oci.events.EventsClient(identity.config)
        logging.warning('>>>Event Client<<<\n')
        self.compartment_id=env["compartment_id"]
        
        
        
    def create_rule(self,display_name,condition,topic_id,description=None):
        actions=oci.events.models.ActionDetailsList(
            actions=[oci.events.models.CreateNotificationServiceActionDetails(
                        action_type="ONS",
                        is_enabled=True,
                        topic_id=topic_id
                        )
                    ]
            )
        create_rule_details=oci.events.models.CreateRuleDetails(
            actions=actions,
            compartment_id=self.compartment_id,
            condition=condition,
            description=description,
            display_name=display_name,
            is_enabled=True
            )
        create_rule_response=self.event_client.create_rule(create_rule_details=create_rule_details)
        logging.warning('>>>event created : {}<<<\n'.format(display_name))
        return create_rule_response
        
###############
###############
###############        
class Data_Catalog_Pipeline:
    def __init__(self,identity,env):
        self.config=identity.config
        self.compartment_id=env["compartment_id"]
        self.bucket_name=env["bucket_name"]
        self.region=env["region"]
        self.namespace_name=env["namespace_name"]
        
        logging.warning('>>>Data Catalog pipeline initialized<<<')
        self.data_catalog_client = oci.data_catalog.DataCatalogClient(self.config)
        logging.warning('>>>Data Catalog Client<<<')
        self.identity_client=oci.identity.IdentityClient(self.config)
        logging.warning('>>>Identity Client<<<\n')
        
    def create_catalog(self,display_name):
        create_catalog_details=oci.data_catalog.models.CreateCatalogDetails(
                compartment_id=self.compartment_id,
                display_name=display_name
        )
        create_catalog_response = self.data_catalog_client.create_catalog(
            create_catalog_details=create_catalog_details
        )
        list_catalog_response=self.data_catalog_client.list_catalogs(
            compartment_id=self.compartment_id,
            display_name=display_name
        )
        
        self.catalog_id=list_catalog_response.data[0].id
        logging.warning('>>>Data Catalog ID = {}<<<\n'.format(self.catalog_id))
        return create_catalog_response
    
    def create_dynamic_group(self,dynamic_group_name="data-catalog-dynamic-group",description="data catalog dynamic group"):
        list_dynamic_groups_response=self.identity_client.list_dynamic_groups(compartment_id=self.compartment_id,
                                                                 name=dynamic_group_name)
        if list_dynamic_groups_response.data:
            logging.warning('>>>Dynamic Group already existed<<<')
            self.dynamic_group_id=list_dynamic_groups_response.data[0].id
            update_dynamic_group_details=oci.identity.models.UpdateDynamicGroupDetails(
                                                matching_rule="Any {resource.id = '"+self.catalog_id+"'}")
            update_dynamic_group_response=self.identity_client.update_dynamic_group(dynamic_group_id=self.dynamic_group_id,
                                                                     update_dynamic_group_details=update_dynamic_group_details)
            
            logging.warning('>>>Dynamic Group updated ID : {}<<<\n'.format(self.dynamic_group_id))
            return update_dynamic_group_response
        
        else:
            create_dynamic_group_details=oci.identity.models.CreateDynamicGroupDetails(
                compartment_id=self.compartment_id,
                description=description,
                matching_rule="Any {resource.id = '"+self.catalog_id+"'}",
                name=dynamic_group_name
            )
            create_dynamic_group_response=self.identity_client.create_dynamic_group(
                create_dynamic_group_details=create_dynamic_group_details
            )

            self.dynamic_group_id=create_dynamic_group_response.data.id
            logging.warning('>>>Dynamic Group ID = {}<<<\n'.format(self.dynamic_group_id))
            return create_dynamic_group_response

    def create_policy(self,name='data-catalog-dynamic-group-policy',
                      statements=["Allow dynamic-group data-catalog-dynamic-group to read object-family in tenancy"],
                      description="Grant access to object storage resources in any compartment in the tenancy"):
        
        list_policies_response=self.identity_client.list_policies(compartment_id=self.compartment_id,
                                                                 name=name)
        
        if list_policies_response.data:
            logging.warning('>>>Policy already existed<<<')
            self.policy_id=list_policies_response.data[0].id
            update_policy_details=oci.identity.models.UpdatePolicyDetails(statements=statements)
            update_policy_response=self.identity_client.update_policy(policy_id=self.policy_id,
                                                                     update_policy_details=update_policy_details)
            logging.warning('>>>Policy statement updated: {}<<<'.format(statements))
            logging.warning('>>>Policy ID : {}<<<\n'.format(self.policy_id))
            return update_policy_response
        else:

            create_policy_details=oci.identity.models.CreatePolicyDetails(
                compartment_id=self.compartment_id,
                description=description,
                name=name,
                statements=statements
            )
            create_policy_response=self.identity_client.create_policy(
                create_policy_details=create_policy_details
            )
            logging.warning('>>>Policy created, statements : {}<<<'.format(statements))
            self.policy_id=create_policy_response.data.id
            logging.warning('>>>Policy ID : {}<<<\n'.format(self.policy_id))
            return create_policy_response
    
    def create_data_asset(self,display_name="Object Storage Data Asset",description="Data asset for object storage"):
        
        types=self.data_catalog_client.list_types(
            catalog_id=self.catalog_id,
            type_category="dataAsset",
            name= "Oracle Object Storage"
        )
        self.data_catalog_type_key=types.data.items[0].key
        
        logging.warning('>>>Data Catalog type key : {}<<<'.format(self.data_catalog_type_key))
        
        properties={
                "default": 
                    {
                        "url":"https://swiftobjectstorage.{}.oraclecloud.com".format(self.region), 
                        "namespace":self.namespace_name}
        }
        
        
        create_data_asset_details=oci.data_catalog.models.CreateDataAssetDetails(
            display_name=display_name,
            type_key=self.data_catalog_type_key,
            description=description,
            properties=properties
        )
        create_data_asset_response = self.data_catalog_client.create_data_asset(
            catalog_id=self.catalog_id,
            create_data_asset_details=create_data_asset_details
        )
        self.data_asset_key=create_data_asset_response.data.key
        logging.warning('>>>Data Asset key : {}<<<\n'.format(self.data_asset_key))
        return create_data_asset_response
    
    def create_connection(self,display_name="Data Asset Connection",description="Connection for data asset"):
        
        types=self.data_catalog_client.list_types(
            catalog_id=self.catalog_id,
            type_category="connection",
            name= "Resource Principal"
        )
        self.connection_type_key=types.data.items[0].key     
        logging.warning('>>>Connection type key : {}<<<'.format(self.connection_type_key))
        
        properties={
                "default": 
                    {
                        "ociRegion": self.region,
                        "ociCompartment": self.compartment_id,
                    }
        }
        create_connection_details=oci.data_catalog.models.CreateConnectionDetails(
            display_name=display_name,
            type_key=self.connection_type_key,
            properties=properties,
            is_default=True,
            description=description,
        )
        create_connection_response = self.data_catalog_client.create_connection(
            catalog_id=self.catalog_id,
            data_asset_key=self.data_asset_key,
            create_connection_details=create_connection_details
        )
        self.connection_key=create_connection_response.data.key
        logging.warning('>>>Connection key : {}<<<\n'.format(self.connection_key))
        return create_connection_response
    
    def create_job_definition(self,display_name="Harvest Object Storage Data Asset"):
        properties= {
            "configuration": {
              "includeUnrecognizedFiles": "false"
            },
            "scope0": {
              "FolderName": self.bucket_name,
              "Type": "jobDefinitionScope"
            }
          }
        create_job_definition_details=oci.data_catalog.models.CreateJobDefinitionDetails(
            connection_key=self.connection_key,
            data_asset_key=self.data_asset_key,
            display_name=display_name,
            job_type="HARVEST",
            is_incremental=True,
            properties=properties
        )
        create_job_definition_response=self.data_catalog_client.create_job_definition(
            catalog_id=self.catalog_id,
            create_job_definition_details=create_job_definition_details,   
        )
        self.job_definition_key=create_job_definition_response.data.key
        
        logging.warning('>>>Job definition key : {}<<<\n'.format(self.job_definition_key))
        return create_job_definition_response
    
    def create_job(self,display_name,time_schedule_begin=None,time_schedule_end=None,schedule_cron_expression=None):
        create_job_details=oci.data_catalog.models.CreateJobDetails(
            display_name=display_name,
            job_definition_key=self.job_definition_key,
            schedule_cron_expression=schedule_cron_expression,
            time_schedule_begin=time_schedule_begin,
            time_schedule_end=time_schedule_end,
        )
        create_job_response=self.data_catalog_client.create_job(
            catalog_id=self.catalog_id,
            create_job_details=create_job_details
        )
        
        self.job_key=create_job_response.data.key
        logging.warning('>>>Job scedule {} from {} to {}<<<\n'.format(schedule_cron_expression,time_schedule_begin,time_schedule_end))
        logging.warning('>>>Job key : {}<<<'.format(self.job_key))
        
        return create_job_response
    
    def create_job_execution(self):
        create_job_execution_details=oci.data_catalog.models.CreateJobExecutionDetails()
        create_job_executio_response=self.data_catalog_client.create_job_execution(
            catalog_id=self.catalog_id,
            job_key=self.job_key,
            create_job_execution_details=create_job_execution_details
        )
        self.job_execution_key=create_job_executio_response.data.key
        logging.warning('>>>Job execution key : {}<<<\n'.format(self.job_execution_key))
        return create_job_executio_response
    
    def delete_data_catalog(self,catalog_id=None,policy_id=None,dynamic_group_id =None):
        if not catalog_id:
            catalog_id=self.catalog_id
        if not policy_id:
            policy_id=self.policy_id
        if not dynamic_group_id:
            dynamic_group_id =self.dynamic_group_id
            
        self.data_catalog_client.delete_catalog(catalog_id=catalog_id)
        logging.warning('>>>Data Catalog Deleted: {}<<<'.format(catalog_id))
        self.identity_client.delete_policy(policy_id=policy_id)  
        logging.warning('>>>Policy Deleted: {}<<<'.format(policy_id))
        self.identity_client.delete_dynamic_group(dynamic_group_id =dynamic_group_id)
        logging.warning('>>>Dynamic Group Deleted: {}<<<\n'.format(dynamic_group_id))
        
    def delete_job_definition(self,catalog_id=None,job_definition_key=None):
        if not catalog_id:
            catalog_id=self.catalog_id
        if not job_definition_key:
            job_definition_key=self.job_definition_key
            
        self.data_catalog_client.delete_job_definition(
            catalog_id=catalog_id,
            job_definition_key=job_definition_key
        )
        logging.warning('>>>Job Definition Deleted: {}<<<\n'.format(job_definition_key))
        
    def delete_job(self,catalog_id=None,job_key=None):
        if not catalog_id:
            catalog_id=self.catalog_id
        if not job_key:
            job_key=self.job_key
            
        self.data_catalog_client.delete_job(
            catalog_id=catalog_id,
            job_key=job_key
        )
        logging.warning('>>>Job Deleted: {}<<<\n'.format(job_key))
    

###############
###############
###############    
class OSDataSource:
    def __init__(self,identity,env):
        logging.warning('>>>OsDataSource initialized<<<')
        self.object_storage_client = oci.object_storage.ObjectStorageClient(identity.config)
        logging.warning('>>>object storage client<<<\n')
        self.namespace_name=env["namespace_name"]
        self.compartment_id=env["compartment_id"]
        self.bucket_name=env["bucket_name"]
        
    def create_bucket(self):
        try:
            create_bucket_details=oci.object_storage.models.CreateBucketDetails(
                name=self.bucket_name,
                compartment_id=self.compartment_id)
            create_bucket_response = self.object_storage_client.create_bucket(
                namespace_name=self.namespace_name,
                create_bucket_details=create_bucket_details)
            logging.warning('>>>bucket created<<<\n')
            logging.warning(create_bucket_response.data)
        except ServiceError as e:
            if e.code == "BucketAlreadyExists" or "Bucket with this name already exists" in e.message:
                logging.warning('>>>bucket {} already existed<<<\n'.format(self.bucket_name))
            else:
                logging.warning('>>>Error creating bucket : {}<<<\n'.format(e))
   
        
    def create_object(self,object_body,object_name):
        put_object_response=self.object_storage_client.put_object(
            namespace_name=self.namespace_name,
            bucket_name=self.bucket_name,
            object_name=object_name,
            put_object_body=object_body)
        logging.warning('>>>object created : {}<<<\n'.format(object_name))
        return put_object_response
        
    def list_object(self, prefix):

        
        objects = []
        start=None

        while True:
            list_objects_response = self.object_storage_client.list_objects(namespace_name=self.namespace_name, 
                                                                        bucket_name=self.bucket_name, 
                                                                        prefix=prefix,
                                                                        start=start
                                                                        )

            objects += list_objects_response.data.objects

            if not list_objects_response.data.next_start_with:
                break

            start = list_objects_response.data.next_start_with
            
        
        logging.warning('>>>list of objects in {}<<<'.format(prefix))
        for obj in objects:
            logging.warning(obj.name)
        return objects
    
    def get_object(self,object_name):
        get_object_response = self.object_storage_client.get_object(namespace_name=self.namespace_name, 
                                                                    bucket_name=self.bucket_name, 
                                                                    object_name=object_name)    
        logging.warning('>>>got objects {}<<<\n'.format(object_name))
        return get_object_response
    
    

    
###############
###############
###############    
class Data_Flow_Pipeline:
    def __init__(self,identity,env):
        logging.warning('>>>Data Flow Pipeline initialized<<<')
        self.data_flow_client = oci.data_flow.DataFlowClient(identity.config)
        logging.warning('>>>Data flow client<<<\n')
        self.compartment_id=env["compartment_id"]
    
    def create_application(self, display_name, file_uri, logs_bucket_uri, archive_uri,
                        driver_shape="VM.Standard.E4.Flex", executor_shape="VM.Standard.E4.Flex", 
                           driver_shape_config={"ocpus":2,"memoryInGBs":16}, executor_shape_config={"ocpus":2,"memoryInGBs":16},
                           num_executors=1,language="PYTHON", spark_version="3.2.1",
                       arguments=None,parameters=None):
        create_application_details=oci.data_flow.models.CreateApplicationDetails(
            compartment_id=self.compartment_id,
            display_name=display_name,
            driver_shape=driver_shape,
            executor_shape=executor_shape,
            driver_shape_config=driver_shape_config,
            executor_shape_config=executor_shape_config,
            language=language,
            num_executors=num_executors,
            spark_version=spark_version,
            file_uri=file_uri,
            logs_bucket_uri=logs_bucket_uri,
            archive_uri=archive_uri,
            arguments=arguments,
            parameters=parameters
            )
        
        create_application_response=self.data_flow_client.create_application(create_application_details=create_application_details)
        self.application_id=create_application_response.data.id
        if create_application_response.status !=200:
            logging.warning(">>>Failed to create application<<<\n")
        else:
            logging.warning(">>>application {} is created: id = {}<<<\n".format(display_name,self.application_id))
            return create_application_response 
    
    def create_run(self,display_name,application_id=None,arguments=None,parameters=None):
        
        if not application_id:
            application_id=self.application_id
            
        create_run_details = oci.data_flow.models.CreateRunDetails(
            compartment_id=self.compartment_id,
            application_id=application_id,
            display_name=display_name,
            arguments=arguments,
            parameters=parameters
            )
        create_run_response=self.data_flow_client.create_run(create_run_details=create_run_details)
        self.run_id=create_run_response.data.id
        if create_run_response.status !=200:
            logging.warning(">>>Failed to create run<<<\n")
        else:
            logging.warning(">>>run is created: id = {}<<<\n".format(self.run_id))
            return create_run_response    
        
        
        
###############
###############
###############        
class Data_Integration_Pipeline:
    def __init__(self,identity,env):
        logging.warning('>>>Data integration Pipeline initialized<<<')
        self.data_integration_client = oci.data_integration.DataIntegrationClient(identity.config)
        logging.warning('>>>Data integration client<<<\n')
        self.compartment_id=env["compartment_id"]
   
    def list_workspace(self,name=None,logging=True):
        list_workspaces_response=self.data_integration_client.list_workspaces(
            compartment_id=self.compartment_id,
            name=name,
            )
        if logging:
            logging.warning(">>>workspaces list = {}<<<\n".format(list_workspaces_response.data))
        return list_workspaces_response    
    
    def list_projects(self,workspace_id=None,name=None):
        if workspace_id:
            self.workspace_id=workspace_id        
        list_projects_response=self.data_integration_client.list_projects(
            workspace_id=selfworkspace_id,
            name=name,
            )
        logging.warning(">>>projects list = {}<<<\n".format(list_projects_response.data))
        return list_projects_response
    
    
    def list_tasks(self,workspace_id=None,name=None):
        if workspace_id:
            self.workspace_id=workspace_id
        list_tasks_response=self.data_integration_client.list_tasks(
            workspace_id=self.workspace_id,
            name=name,
            )
        logging.warning(">>>tasks list = {}<<<\n".format(list_tasks_response.data))
        return list_tasks_response
    

    def get_workspace(self,workspace_id=None ):
        if workspace_id:
            self.workspace_id=workspace_id
        get_workspace_response=self.data_integration_client.get_workspace(
            workspace_id=self.workspace_id,
            )
        logging.warning(">>>workspace {} = {}<<<\n".format(workspace_id,get_workspace_response.data))
        return get_workspace_response

    def get_project(self,workspace_id=None,project_key=None):
        if workspace_id:
            self.workspace_id=workspace_id
        if project_key:
            self.project_key=project_key
        get_project_response=self.data_integration_client.get_project(
            workspace_id=self.workspace_id,
            project_key=self.project_key
            )
        logging.warning(">>>project {} = {}<<<\n".format(project_key,get_project_response.data))
        return get_project_response
    
    def get_task(self,workspace_id=None,task_key=None):
        if workspace_id:
            self.workspace_id=workspace_id
        if task_key:
            self.task_key=task_key
        get_task_response=self.data_integration_client.get_task(
            workspace_id=self.workspace_id,
            task_key=self.task_key
            )
        logging.warning(">>>task {} = {}<<<\n".format(task_key,get_task_response.data))
        return get_task_response

    
    
    def create_workspace(self,display_name):
        create_workspace_details=oci.data_integration.models.CreateWorkspaceDetails(
            compartment_id=self.compartment_id,
            display_name=display_name
            )
        create_workspace_response=self.data_integration_client.create_workspace(
            create_workspace_details=create_workspace_details,
            )
        
        list_workspace_response=self.list_workspace(name=display_name,logging=False)

        self.workspace_id=list_workspace_response.data[0].id
        logging.warning(">>>workspace is created: id = {}<<<".format(self.workspace_id))
        logging.warning(">>>workspace takes time to create, give it some time before creaing a project<<<\n")
        return create_workspace_response
    

    def create_project(self,name,description=None,workspace_id=None):
        if workspace_id:
            self.workspace_id=workspace_id
        create_project_details=oci.data_integration.models.CreateProjectDetails(
            identifier=name.upper(),
            name=name,
            description=description,
            )
        create_project_response=self.data_integration_client.create_project(
            workspace_id=self.workspace_id,
            create_project_details=create_project_details,
            )
        self.project_key=create_project_response.data.key
        logging.warning(">>>project is created: key = {}<<<\n".format(self.project_key))
        return create_project_response
    
    
    def create_task(self,name,dataflow_application_id,description=None,workspace_id=None,project_key=None):
        if workspace_id:
            self.workspace_id=workspace_id
            
        if project_key:
            self.project_key=project_key
            
#         dataflow_application=oci.data_integration.models.DataflowApplication(
#             application_id=dataflow_application_id,
#             compartment_id=self.compartment_id)
#         registry_metadata=oci.data_integration.models.RegistryMetadata(
#             aggregator_key=self.project_key)   
#         create_task_from_oci_dataflowtask_details=oci.data_integration.models.CreateTaskFromOCIDataflowTask(
#             identifier=name.upper(),
#             name=name,
#             model_type="OCI_DATAFLOW_TASK",
#             registry_metadata=registry_metadata,
#             dataflow_application=dataflow_application)
#          create_task_response=self.data_integration_client.create_task(
#              workspace_id=self.workspace_id,
#              create_task_details=create_task_from_oci_dataflowtask_details)

        payload={"modelType":"OCI_DATAFLOW_TASK",
                 "identifier":name.upper(),
                 "name":name,
                 "description":description,
                 "registryMetadata":{"aggregatorKey":self.project_key},
                 "opConfigValues":{"configParamValues":{}},
                "parameters":[],
                 "dfApplication":{
                     "applicationId":dataflow_application_id,
                     "compartmentId":self.compartment_id
                     }
                }
        create_task_response=self.data_integration_client.create_task(
             workspace_id=self.workspace_id,
             create_task_details=payload, 
             )

        self.task_key=create_task_response.data.key
        logging.warning(">>>task is created: key = {}<<<\n".format(self.task_key))
        return create_task_response
        
    def create_application(self,name,workspace_id=None):
        if workspace_id:
            self.workspace_id=workspace_id
        create_application_details=oci.data_integration.models.CreateApplicationDetails(
            identifier=name.upper(),
            name=name,
            model_type="INTEGRATION_APPLICATION",
            )
        create_application_response=self.data_integration_client.create_application(
            workspace_id=self.workspace_id,
            create_application_details=create_application_details
            )
        self.application_key=create_application_response.data.key
        logging.warning(">>>application is created: key = {}<<<\n".format(self.application_key))
 
        return create_application_response

    def create_patch(self,name,workspace_id=None,application_key=None,task_key=None):
        if workspace_id:
            self.workspace_id=workspace_id
            
        if application_key:
            self.application_key=application_key
        
        if task_key:
            self.task_key=task_key
            
        create_patch_details=oci.data_integration.models.CreatePatchDetails(
            identifier=name.upper(),
            name=name,
            object_keys=[self.task_key],
            patch_type="PUBLISH"
            )
        
        create_patch_response=self.data_integration_client.create_patch(
            workspace_id=self.workspace_id,
            application_key=self.application_key,
            create_patch_details=create_patch_details,
            )
        self.patch_key=create_patch_response.data.key
        logging.warning(">>>task is published into application, patch key = {}<<<".format(self.patch_key))
        logging.warning(">>>patching takes time, wait a bit<<<\n")
        return create_patch_response
    
    def create_schedule(self,name,hour=1,minute=0,second=0,workspace_id=None,application_key=None):
        if workspace_id:
            self.workspace_id=workspace_id
            
        if application_key:
            self.application_key=application_key
        frequency_details=oci.data_integration.models.HourlyFrequencyDetails(
            model_type="HOURLY",
            frequency="HOURLY",
            time=oci.data_integration.models.Time(
                hour=hour,
                minute=minute,
                second=second
                )
            )
        
        create_schedule_details=oci.data_integration.models.CreateScheduleDetails(
            identifier=name.upper(),
            name=name,
            frequency_details=frequency_details,
            timezone="UTC",
            is_daylight_adjustment_enabled=False
            )
        
        create_schedule_response=self.data_integration_client.create_schedule(
            workspace_id=self.workspace_id,
            application_key=self.application_key,
            create_schedule_details=create_schedule_details
            )
        self.schedule_key=create_schedule_response.data.key
        logging.warning(">>>schedule is created: key = {}<<<\n".format(self.schedule_key))
        return create_schedule_response
    
    def create_task_schedule(self,name,is_enabled=True,start_time_millis=None,end_time_millis=None,
                             expected_duration=4,expected_duration_unit="MINUTES",
                             workspace_id=None,application_key=None,task_key=None):
        if workspace_id:
            self.workspace_id=workspace_id
            
        if application_key:
            self.application_key=application_key
        
        if task_key:
            self.task_key=task_key
            
        schedule_ref=oci.data_integration.models.Schedule(
            key=self.schedule_key
            )
        registry_metadata=oci.data_integration.models.RegistryMetadata(
            aggregator_key=self.task_key
            )
        create_task_schedule_details=oci.data_integration.models.CreateTaskScheduleDetails(
            identifier=name.upper(),
            name=name,
            expected_duration=expected_duration,
            expected_duration_unit=expected_duration_unit,
            schedule_ref=schedule_ref,
            end_time_millis=end_time_millis,
            start_time_millis=start_time_millis,
            registry_metadata=registry_metadata,
            is_enabled=is_enabled
            )
        
        create_task_schedule_response=self.data_integration_client.create_task_schedule(
            workspace_id=self.workspace_id, 
            application_key=self.application_key, 
            create_task_schedule_details=create_task_schedule_details
            )
        self.task_schedule_key=create_task_schedule_response.data.key
        logging.warning(">>>schedule task is created: key = {}<<<\n".format(self.task_schedule_key))
        return create_task_schedule_response


 