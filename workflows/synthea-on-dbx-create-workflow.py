# Databricks notebook source
# DBTITLE 1,Install Databricks SDK
# MAGIC %pip install databricks-sdk --upgrade

# COMMAND ----------

# DBTITLE 1,Python Library Restart
dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Extract Databricks SDK version
# MAGIC %pip show databricks-sdk | grep -oP '(?<=Version: )\S+'

# COMMAND ----------

# DBTITLE 1,Set Databricks Widgets
dbutils.widgets.text("catalog_name", "")
dbutils.widgets.text("schema_name", "synthea")
dbutils.widgets.text(
    "instance_pool_id", "", "Optional Instance Pool ID for the Cluster Spec"
)
dbutils.widgets.text(
    "node_type_id",
    "i3.xlarge",
    "Node Type Id, Required if Instance Pool Id is not specified",
)
dbutils.widgets.dropdown(
    "create_landing_zone", "false", ["true", "false"], "Optional Create a landing zone"
)
dbutils.widgets.dropdown(
    "inject_bad_data",
    "true",
    ["true", "false"],
    "Optional inection of bad data to select files",
)
dbutils.widgets.dropdown(
  "serverless"
  ,"false"
  ,["true", "false"]
  ,"Serverless Job Mode"
)
dbutils.widgets.text(name = "min_records", defaultValue="1", label = "Minimum Generated Record Count")
dbutils.widgets.text(name = "max_records", defaultValue="1000", label = "Maximum Generated Record Count")

# COMMAND ----------

# DBTITLE 1,Get Widget Inputs
catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")
instance_pool_id = dbutils.widgets.get("instance_pool_id")
node_type_id = dbutils.widgets.get("node_type_id")
create_landing_zone = dbutils.widgets.get("create_landing_zone").lower()
inject_bad_data = dbutils.widgets.get("inject_bad_data").lower()
serverless = dbutils.widgets.get("serverless").lower()
min_records = int(dbutils.widgets.get("min_records"))
max_records = int(dbutils.widgets.get("max_records"))

# COMMAND ----------

# DBTITLE 1,File Destination Information
print(
    f"""
Based on user input's the job will write files into this catalog.schema's Volume:
catalog_name = {catalog_name}
schema_name = {schema_name}
create_landing_zone = {create_landing_zone}
inject_bad_data = {inject_bad_data}
serveless = {serverless}

The Databricks workflow created by this notebook will write files into the following schema's Volume:
/Volumes/{catalog_name}/{schema_name}/synthetic_files_raw/

Please note that is the catalog, schema, or Volume do not exist, the workflow notebooks will attempt to create them.  If the user does not have the appropriate permissions to create or use the inputted catalog, or create or use the inputted schema, the workflow will fail during execution.  Please adjust the inputted values and re-run this notebook.  
"""
)

# COMMAND ----------

# DBTITLE 1,Databricks SDK workspace initialization
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

# COMMAND ----------

# DBTITLE 1,Get Current User and Spark Version
current_user = w.current_user.me()
latest_lts_version = w.clusters.select_spark_version(
    latest=True, long_term_support=True
)

# COMMAND ----------

if current_user.name.family_name is None:
    current_user.name.family_name = (
        current_user.user_name.split("@", 1)[0].strip(" ").replace(".", "_").lower()
    )
print(current_user.name.family_name)

# COMMAND ----------

# DBTITLE 1,Synthetic Data Deneration Job Inputs
job_name = (
    current_user.name.family_name.lower()
    + "-"
    + current_user.id
    + "-synthea-data-generation"
)
job_cluster_key = (
    current_user.name.family_name.lower() + "-" + current_user.id + "-synthea-data-gen"
)
job_description = f"Job to generate synthetic data for {current_user.user_name} in /Volumes/{catalog_name}/{schema_name}/synthetic_files_raw/ using the synthea library jar."

# COMMAND ----------

# DBTITLE 1,Print Job Inputs
print(
    f"""
Current user: {current_user.user_name}
Latest LTS version: {latest_lts_version}

Databricks Workflow Name: {job_name}
Job cluster key: {job_cluster_key}
Job description: {job_description}

Cluster Specification Details: 
instance_pool_id = {instance_pool_id}
node_type_id = {node_type_id}

Note that node_type_id will only be used if an instance_pool_id is not set.
"""
)

# COMMAND ----------

# DBTITLE 1,Import Databricks Cluster Configuration Modules
from databricks.sdk.service.jobs import JobCluster
from databricks.sdk.service.compute import ClusterSpec, DataSecurityMode, RuntimeEngine, AwsAttributes, AwsAvailability

# COMMAND ----------

# DBTITLE 1,Get the base path
# get the base path of the current notebook so we can determine what all notebook paths for the workflow definitions are relative to
full_path = (
    dbutils.entry_point.getDbutils()
    .notebook()
    .getContext()
    .notebookPath()
    .getOrElse(None)
)
path_parts = full_path.split("/")
workflows_index = path_parts.index("workflows")
base_path = "/".join(path_parts[:workflows_index])
print(f"The base path is: {base_path}")

# COMMAND ----------

# DBTITLE 1,Job Cluster Specification Creation
if serverless == "true":
  cluster_spec = None
  job_cluster_key = None
else:
  if instance_pool_id == "": 
    cluster_spec = JobCluster(
      job_cluster_key = job_cluster_key
      ,new_cluster = ClusterSpec(
        spark_version = latest_lts_version
        ,spark_conf = {
          "spark.master": "local[*, 4]",
          "spark.databricks.cluster.profile": "singleNode"
        }
        ,custom_tags = {
          "ResourceClass": "SingleNode"
        }
        ,spark_env_vars = {
          "JNAME": "zulu17-ca-amd64"
        }
        ,data_security_mode = DataSecurityMode("SINGLE_USER")
        ,runtime_engine = RuntimeEngine("STANDARD")
        ,num_workers = 0
        ,node_type_id = node_type_id
        ,aws_attributes = AwsAttributes(
          availability = AwsAvailability("ON_DEMAND")
        )
      )
    )
  else:
    cluster_spec = JobCluster(
      job_cluster_key = job_cluster_key
      ,new_cluster = ClusterSpec(
        spark_version = latest_lts_version
        ,spark_conf = {
          "spark.master": "local[*, 4]",
          "spark.databricks.cluster.profile": "singleNode"
        }
        ,custom_tags = {
          "ResourceClass": "SingleNode"
        }
        ,spark_env_vars = {
          "JNAME": "zulu17-ca-amd64"
        }
        ,instance_pool_id = instance_pool_id
        ,driver_instance_pool_id = instance_pool_id
        ,data_security_mode = DataSecurityMode("SINGLE_USER")
        ,runtime_engine = RuntimeEngine("STANDARD")
        ,num_workers = 0
      )
    )

# COMMAND ----------

# DBTITLE 1,Import Databricks SDK Job and Task Configuration Modules
from databricks.sdk.service.jobs import (
    Source,
    Task,
    NotebookTask,
    TaskEmailNotifications,
    TaskNotificationSettings,
    WebhookNotifications,
    RunIf,
    QueueSettings,
    JobParameter,
    JobRunAs
)

# COMMAND ----------

# DBTITLE 1,Syntha Set-up Check Task
# task 0: syntha_set_up_check
synthea_set_up_check = Task(
  task_key = "synthea_set_up_check"
  ,description = "Check to see if the synthea jar and configuration files have been set up"
  ,run_if = RunIf("ALL_SUCCESS")
  ,job_cluster_key = job_cluster_key
  ,notebook_task = NotebookTask(
    notebook_path = f"{base_path}/notebooks/00-setup-notebooks/0.0-set-up-check"
    ,source = Source("WORKSPACE")
    ,base_parameters = dict("")
  )
  ,timeout_seconds = 0
  ,email_notifications = TaskEmailNotifications()
  ,notification_settings = TaskNotificationSettings(
    no_alert_for_skipped_runs = False
    ,no_alert_for_canceled_runs = False
    ,alert_on_last_attempt = False
  )
  ,webhook_notifications = WebhookNotifications()
)

# COMMAND ----------

# DBTITLE 1,Import Databricks Task Dependency Modules
from databricks.sdk.service.jobs import TaskDependency, ConditionTask, ConditionTaskOp

# COMMAND ----------

# DBTITLE 1,Result Conditional Task
# task 1:  result_conditional
result_conditional = Task(
  task_key = "result_conditional"
  ,description = "Check the result of the synthea_set_up_check task. If 'true' then proceed with data generation, otherwise prepare the catalog, schema, volume, synthea jar file and configuration files."
  ,depends_on = [TaskDependency(
    task_key = "synthea_set_up_check"
  )]
  ,run_if = RunIf("ALL_SUCCESS")
  ,condition_task = ConditionTask(
    op = ConditionTaskOp("EQUAL_TO")
    ,left = "{{tasks.[synthea_set_up_check].values.[result]}}"
    ,right = "True"
  )
  ,timeout_seconds = 0
  ,email_notifications = TaskEmailNotifications()
  ,notification_settings = TaskNotificationSettings(
    no_alert_for_skipped_runs = False
    ,no_alert_for_canceled_runs = False
    ,alert_on_last_attempt = False
  )
  ,webhook_notifications = WebhookNotifications()
)

# COMMAND ----------

# DBTITLE 1,Unity Catalog Setup Task
# task 2: uc_setup
uc_setup = Task(
  task_key = "uc_setup"
  ,description = "Create the catalog, schema, and volume for the data to be saved in if it does not exist."
  ,depends_on = [TaskDependency(
    task_key = "result_conditional"
    ,outcome = "false"
  )]
  ,run_if = RunIf("ALL_SUCCESS")
  ,job_cluster_key = job_cluster_key
  ,notebook_task = NotebookTask(
    notebook_path = f"{base_path}/notebooks/00-setup-notebooks/0.1-uc-setup"
    ,source = Source("WORKSPACE")
    ,base_parameters = dict("")
  )
  ,timeout_seconds = 0
  ,email_notifications = TaskEmailNotifications()
  ,notification_settings = TaskNotificationSettings(
    no_alert_for_skipped_runs = False
    ,no_alert_for_canceled_runs = False
    ,alert_on_last_attempt = False
  )
  ,webhook_notifications = WebhookNotifications()
)

# COMMAND ----------

# DBTITLE 1,Install Synthea Task
# task 3: install_synthea
install_synthea = Task(
  task_key = "install_synthea"
  ,description = "Download the synthea jar file and save to the specified volume."
  ,depends_on = [TaskDependency(
    task_key = "uc_setup"
  )]
  ,run_if = RunIf("ALL_SUCCESS")
  ,job_cluster_key = job_cluster_key
  ,notebook_task = NotebookTask(
    notebook_path = f"{base_path}/notebooks/00-setup-notebooks/0.2-install-synthea"
    ,source = Source("WORKSPACE")
    ,base_parameters = dict("")
  )
  ,timeout_seconds = 0
  ,email_notifications = TaskEmailNotifications()
  ,notification_settings = TaskNotificationSettings(
    no_alert_for_skipped_runs = False
    ,no_alert_for_canceled_runs = False
    ,alert_on_last_attempt = False
  )
  ,webhook_notifications = WebhookNotifications()
)

# COMMAND ----------

# DBTITLE 1,Configure Synthea Task
# task 4: configure_synthea
configure_synthea = Task(
  task_key = "configure_synthea"
  ,description = "Write synthea configuration file to the volume to control data output."
  ,depends_on = [TaskDependency(
    task_key = "install_synthea"
  )]
  ,run_if = RunIf("ALL_SUCCESS")
  ,job_cluster_key = job_cluster_key
  ,notebook_task = NotebookTask(
    notebook_path = f"{base_path}/notebooks/00-setup-notebooks/0.3-synthea-configuration"
    ,source = Source("WORKSPACE")
    ,base_parameters = dict("")
  )
  ,timeout_seconds = 0
  ,email_notifications = TaskEmailNotifications()
  ,notification_settings = TaskNotificationSettings(
    no_alert_for_skipped_runs = False
    ,no_alert_for_canceled_runs = False
    ,alert_on_last_attempt = False
  )
  ,webhook_notifications = WebhookNotifications()
)

# COMMAND ----------

# DBTITLE 1,Generate Synthetic Data Task
# task 5: generate_synthetic_data
generate_synthetic_data = Task(
  task_key = "generate_synthetic_data"
  ,description = "Run synthea to generate synthetic healthcare data."
  ,depends_on = [
    TaskDependency(
      task_key = "result_conditional"
      ,outcome = "true"
    )
    ,TaskDependency(
      task_key = "configure_synthea"
    )
  ]
  ,run_if = RunIf("AT_LEAST_ONE_SUCCESS")
  ,job_cluster_key = job_cluster_key
  ,notebook_task = NotebookTask(
    notebook_path = f"{base_path}/notebooks/01-data-generation/1.0-synthea-data-generator"
    ,source = Source("WORKSPACE")
    ,base_parameters = dict("")
  )
  ,timeout_seconds = 0
  ,email_notifications = TaskEmailNotifications()
  ,notification_settings = TaskNotificationSettings(
    no_alert_for_skipped_runs = False
    ,no_alert_for_canceled_runs = False
    ,alert_on_last_attempt = False
  )
  ,webhook_notifications = WebhookNotifications()
)

# COMMAND ----------

# task 6:  inject_data_quality
inject_data_quality_conditional = Task(
  task_key = "inject_bad_data_conditional"
  ,description = "Check if the user opted to create the landing zone using the job parameter create_landing_zone upon initializing the workflow"
  ,depends_on = [TaskDependency(
    task_key = "generate_synthetic_data"
  )]
  ,run_if = RunIf("ALL_SUCCESS")
  ,condition_task = ConditionTask(
    op = ConditionTaskOp("EQUAL_TO")
    ,left = "{{job.parameters.inject_bad_data}}"
    ,right = "true"
  )
  ,timeout_seconds = 0
  ,email_notifications = TaskEmailNotifications()
  ,notification_settings = TaskNotificationSettings(
    no_alert_for_skipped_runs = False
    ,no_alert_for_canceled_runs = False
    ,alert_on_last_attempt = False
  )
  ,webhook_notifications = WebhookNotifications()
)

# COMMAND ----------

# DBTITLE 1,inject_bad_data task
# task 7: inject_bad_data
inject_data_quality = Task(
  task_key = "inject_bad_data"
  ,description = "Check if the user opted to create the landing zone using the job parameter create_landing_zone upon initializing the workflow"
  ,depends_on = [
    TaskDependency(
      task_key = "inject_bad_data_conditional"
      ,outcome = "true"
    )
  ]
  ,run_if = RunIf("ALL_SUCCESS")
  ,job_cluster_key = job_cluster_key
  ,notebook_task = NotebookTask(
    notebook_path = f"{base_path}/notebooks/01-data-generation/2.0-inject-bad-data"
    ,source = Source("WORKSPACE")
    ,base_parameters = dict("")
  )
  ,timeout_seconds = 0
  ,email_notifications = TaskEmailNotifications()
  ,notification_settings = TaskNotificationSettings(
    no_alert_for_skipped_runs = False
    ,no_alert_for_canceled_runs = False
    ,alert_on_last_attempt = False
  )
  ,webhook_notifications = WebhookNotifications()
)

# COMMAND ----------

# DBTITLE 1,Evaluate if creating landing zone
# task 8:  create_landing_zone_conditional
create_landing_zone_conditional = Task(
  task_key = "create_landing_zone_conditional"
  ,description = "Check if the user opted to create the landing zone using the job parameter create_landing_zone upon initializing the workflow"
  ,depends_on = [TaskDependency(
    task_key = "generate_synthetic_data"
  ),
                TaskDependency(
    task_key = "inject_bad_data"
    )]
  ,run_if = RunIf("AT_LEAST_ONE_SUCCESS")
  ,condition_task = ConditionTask(
    op = ConditionTaskOp("EQUAL_TO")
    ,left = "{{job.parameters.create_landing_zone}}"
    ,right = "true"
  )
  ,timeout_seconds = 0
  ,email_notifications = TaskEmailNotifications()
  ,notification_settings = TaskNotificationSettings(
    no_alert_for_skipped_runs = False
    ,no_alert_for_canceled_runs = False
    ,alert_on_last_attempt = False
  )
  ,webhook_notifications = WebhookNotifications()
)

# COMMAND ----------

# DBTITLE 1,Copy Files to Landing Zone
# task 9: copy_files_to_landing_zone
copy_files_to_landing_zone = Task(
  task_key = "copy_files_to_landing_zone"
  ,description = "Copy new files from synthea_raw_files volume to landing volume"
  ,depends_on = [
    TaskDependency(
      task_key = "create_landing_zone_conditional"
      ,outcome = "true"
    )
  ]
  ,run_if = RunIf("ALL_SUCCESS")
  ,job_cluster_key = job_cluster_key
  ,notebook_task = NotebookTask(
    notebook_path = f"{base_path}/notebooks/01-data-generation/3.0-move-synthea-files-to-landing"
    ,source = Source("WORKSPACE")
    ,base_parameters = dict("")
  )
  ,timeout_seconds = 0
  ,email_notifications = TaskEmailNotifications()
  ,notification_settings = TaskNotificationSettings(
    no_alert_for_skipped_runs = False
    ,no_alert_for_canceled_runs = False
    ,alert_on_last_attempt = False
  )
  ,webhook_notifications = WebhookNotifications()
)

# COMMAND ----------

# DBTITLE 1,Existing Jobs List and Deletion
jobs_list = w.jobs.list(
  expand_tasks = False
  ,name = job_name
)
jobs_list = [job.as_dict() for job in jobs_list]
print(jobs_list, "\n")
if len(jobs_list) == 0: 
  print("No jobs found. Proceed with creating a new job...")
else:
  print("One or more jobs with the same name already exists. Deleting the jobs...\n\n")
  for i in range(0,len(jobs_list)):
    print(f"Deleting job {jobs_list[i].get('job_id')}\n")
    w.jobs.delete(jobs_list[i].get("job_id"))
  print("All jobs with the same name have been deleted. Proceed with creating a new job...")

# COMMAND ----------

# DBTITLE 1,Create Synthea Data Generation Workflow
print("Attempting to create the job. Please wait...\n")

if serverless == "false":
  cluster_spec = [cluster_spec]

j = w.jobs.create(
  name = job_name
  ,description = job_description
  ,tasks = [
    synthea_set_up_check
    ,result_conditional
    ,uc_setup
    ,install_synthea
    ,configure_synthea
    ,generate_synthetic_data
    ,inject_data_quality_conditional
    ,inject_data_quality
    ,create_landing_zone_conditional
    ,copy_files_to_landing_zone
  ]
  ,job_clusters = cluster_spec
  ,queue = QueueSettings(enabled = True)
  ,parameters = [
    JobParameter(
      name = "catalog_name"
      ,default = catalog_name
      ,value = catalog_name
    )
    ,JobParameter(
      name = "schema_name"
      ,default = schema_name
      ,value = schema_name
    )
    ,JobParameter(
      name = "create_landing_zone"
      ,default = create_landing_zone
      ,value = create_landing_zone
    )
    ,JobParameter(
      name = "inject_bad_data"
      ,default = inject_bad_data
      ,value = inject_bad_data
    )
    ,JobParameter(
      name = "min_records"
      ,default = min_records
      ,value = min_records
    )   
    ,JobParameter(
      name = "max_records"
      ,default = max_records
      ,value = max_records
    )  
  ]
  ,run_as = JobRunAs(
    user_name = current_user.user_name
  )
)

print(f"Job created successfully. Job ID: {j.job_id}")

# COMMAND ----------

# DBTITLE 1,Notebook Exit with Job Status
import json
dbutils.notebook.exit(json.dumps({
  "status": "OK",
  "job": j.as_dict()
}))
