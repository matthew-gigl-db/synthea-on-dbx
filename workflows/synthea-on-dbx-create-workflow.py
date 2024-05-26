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
from databricks.sdk.service.jobs import Source, Task, NotebookTask, TaskEmailNotifications, TaskNotificationSettings, WebhookNotifications, RunIf, QueueSettings, JobParameter, JobRunAs
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

# DBTITLE 1,Create Synthea Data Generation Workflow
print("Attempting to create the job. Please wait...\n")

j = w.jobs.create(
  name = job_name
  ,description = job_description
  ,tasks = [
    Task(
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
