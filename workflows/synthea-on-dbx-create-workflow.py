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
dbutils.widgets.text("instance_pool_id", "", "Optional Instance Pool ID for the Cluster Spec")
dbutils.widgets.text("node_type_id", "i3.xlarge", "Node Type Id, Required if Instance Pool Id is not specified")

# COMMAND ----------

# DBTITLE 1,Get Widget Inputs
catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")
instance_pool_id = dbutils.widgets.get("instance_pool_id")
node_type_id = dbutils.widgets.get("node_type_id")

# COMMAND ----------

# DBTITLE 1,File Destination Information
print(
f"""
Based on user input's the job will write files into this catalog.schema's Volume:
catalog_name = {catalog_name}
schema_name = {schema_name}

The Databricks workflow created by this notebook will write files into the following schema's Volume:
/Volumes/{catalog_name}/{schema_name}/synthetic_files_raw/

Please note that is the catalog, schema, or Volume do not exist, the workflow notebooks will attempt to create them.  If the user does not have the appropriate permissions to create or use the inputted catalog, or create or use the inputted schema, the workflow will fail during execution.  Please adjust the inputted values and re-run this notebook.  
"""
)

# COMMAND ----------

# DBTITLE 1,Databricks SDK workspace initialization
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import Source, Task, NotebookTask, TaskEmailNotifications, TaskNotificationSettings, WebhookNotifications, RunIf, QueueSettings, JobParameter, JobRunAs, JobCluster
from databricks.sdk.service.compute import ClusterSpec, DataSecurityMode, RuntimeEngine

w = WorkspaceClient()

# COMMAND ----------

# DBTITLE 1,Get Current User and Spark Version
current_user = w.current_user.me()
latest_lts_version = w.clusters.select_spark_version(latest=True, long_term_support=True)

# COMMAND ----------

# DBTITLE 1,Synthetic Data Deneration Job Inputs
job_name = current_user.name.family_name.lower() + "-" + current_user.id + "-synthea-data-generation"
job_cluster_key = current_user.name.family_name.lower() + "-" + current_user.id + "-synthea-data-gen"
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

# DBTITLE 1,Job Cluster Specification Creation
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

# task 0: syntha_set_up_check
synthea_set_up_check = Task(
  task_key = "synthea_set_up_check"
  ,description = "Check to see if the synthea jar and configuration files have been set up"
  ,run_if = RunIf("ALL_SUCCESS")
  ,job_cluster_key = job_cluster_key
  ,notebook_task = NotebookTask(
    notebook_path = f"/Workspace/Users/{current_user.user_name}/synthea-on-dbx/notebooks/00-setup-notebooks/0.0-set-up-check"
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

from databricks.sdk.service.jobs import TaskDependency, ConditionTask, ConditionTaskOp

# COMMAND ----------

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
    notebook_path = f"/Workspace/Users/{current_user.user_name}/synthea-on-dbx/notebooks/00-setup-notebooks/0.1-uc-setup"
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
    notebook_path = f"/Workspace/Users/{current_user.user_name}/synthea-on-dbx/notebooks/00-setup-notebooks/0.2-install-synthea"
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

{
  "task_key": "install_synthea",
  "depends_on": [
    {
      "task_key": "uc_setup"
    }
  ],
  "run_if": "ALL_SUCCESS",
  "notebook_task": {
    "notebook_path": "/Workspace/Users/matthew.giglia@databricks.com/db-nosql/00-setup-notebooks/0.1-install-synthea",
    "source": "WORKSPACE"
  },
  "job_cluster_key": "mg-synthea-data-gen",
  "timeout_seconds": 0,
  "email_notifications": {},
  "notification_settings": {
    "no_alert_for_skipped_runs": false,
    "no_alert_for_canceled_runs": false,
    "alert_on_last_attempt": false
  },
  "webhook_notifications": {}
}

# COMMAND ----------

# DBTITLE 1,Create Synthea Data Generation Workflow
print("Attempting to create the job. Please wait...\n")

j = w.jobs.create(
  name = job_name
  ,description = job_description
  ,tasks = [
    synthea_set_up_check
    ,result_conditional
    ,uc_setup
  ]
  ,job_clusters = [cluster_spec]
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
  ]
  ,run_as = JobRunAs(
    user_name = current_user.user_name
  )
)

print(f"Job created successfully. Job ID: {j.job_id}")

# COMMAND ----------

,
,
{
  "task_key": "install_synthea",
  "depends_on": [
    {
      "task_key": "uc_setup"
    }
  ],
  "run_if": "ALL_SUCCESS",
  "notebook_task": {
    "notebook_path": "/Workspace/Users/matthew.giglia@databricks.com/db-nosql/00-setup-notebooks/0.1-install-synthea",
    "source": "WORKSPACE"
  },
  "job_cluster_key": "mg-synthea-data-gen",
  "timeout_seconds": 0,
  "email_notifications": {},
  "notification_settings": {
    "no_alert_for_skipped_runs": false,
    "no_alert_for_canceled_runs": false,
    "alert_on_last_attempt": false
  },
  "webhook_notifications": {}
},
{
  "task_key": "configure_synthea",
  "depends_on": [
    {
      "task_key": "install_synthea"
    }
  ],
  "run_if": "ALL_SUCCESS",
  "notebook_task": {
    "notebook_path": "/Workspace/Users/matthew.giglia@databricks.com/db-nosql/00-setup-notebooks/0.2-synthea-configuration",
    "source": "WORKSPACE"
  },
  "job_cluster_key": "mg-synthea-data-gen",
  "timeout_seconds": 0,
  "email_notifications": {},
  "notification_settings": {
    "no_alert_for_skipped_runs": false,
    "no_alert_for_canceled_runs": false,
    "alert_on_last_attempt": false
  },
  "webhook_notifications": {}
},
{
  "task_key": "generate_synthetic_data",
  "depends_on": [
    {
      "task_key": "result_conditional",
      "outcome": "true"
    },
    {
      "task_key": "configure_synthea"
    }
  ],
  "run_if": "AT_LEAST_ONE_SUCCESS",
  "notebook_task": {
    "notebook_path": "/Workspace/Users/matthew.giglia@databricks.com/db-nosql/01-data-generation/1.0-synthea-data-generator",
    "source": "WORKSPACE"
  },
  "job_cluster_key": "mg-synthea-data-gen",
  "timeout_seconds": 0,
  "email_notifications": {},
  "notification_settings": {
    "no_alert_for_skipped_runs": false,
    "no_alert_for_canceled_runs": false,
    "alert_on_last_attempt": false
  },
  "webhook_notifications": {}
}
