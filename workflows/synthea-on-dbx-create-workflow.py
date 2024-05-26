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

dbutils.widgets.text("catalog_name", "")
dbutils.widgets.text("schema_name", "synthea")
dbutils.widgets.text("instance_pool_id", "", "Optional Instance Pool ID for the Cluster Spec")
dbutils.widgets.text("node_type_id", "i3.xlarge", "Node Type Id, Required if Instance Pool Id is not specified")

# COMMAND ----------

# DBTITLE 1,Input Catalog and Schema Name.
# catalog_name = input("Catalog name (for example, main) for the job to write files into the schema's Volume: ")
# schema_name = input("Schema name (for example, my_schema) that the Volume will be created in: ")
catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")
instance_pool_id = dbutils.widgets.get("instance_pool_id")
node_type_id = dbutils.widgets.get("node_type_id")

# COMMAND ----------

# DBTITLE 1,Catalog Schema Volume
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
from databricks.sdk.service.jobs import Source, Task, NotebookTask, TaskEmailNotifications, TaskNotificationSettings, WebhookNotifications, RunIf
from databricks.sdk.service.compute import ClusterSpec, DataSecurityMode, RuntimeEngine

w = WorkspaceClient()

# COMMAND ----------

current_user = w.current_user.me()
latest_lts_version = w.clusters.select_spark_version(latest=True, long_term_support=True)

# COMMAND ----------

# current_user.as_dict()

# COMMAND ----------

job_name = current_user.name.family_name.lower() + "-" + current_user.id + "-synthea-data-generation"
job_cluster_key = current_user.name.family_name.lower() + "-" + current_user.id + "-synthea-data-gen"
job_description = f"Job to generate synthetic data for {current_user.user_name} in /Volumes/{catalog_name}/{schema_name}/synthetic_files_raw/ using the synthea library jar."

# COMMAND ----------

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

# DBTITLE 1,Check if a job with the same name exists
jobs_list = w.jobs.list(
  expand_tasks=False
  ,name = job_name
)
jobs_list = [job.as_dict() for job in jobs_list]
print(jobs_list)
if len(jobs_list) == 0: 
  print("No jobs found. Creating a new job...")
else:
  print("One or more jobs with the same name already exists. Deleting the jobs...")
  for i in range(0,len(jobs_list)):
    print(f"Deleting job {jobs_list[i].get('job_id')}")
    w.jobs.delete(jobs_list[i].get("job_id"))
  print("All jobs with the same name have been deleted. Creating a new job...")

# COMMAND ----------


# job_name            = input("Some short name for the job (for example, my-job): ")
# description         = input("Some short description for the job (for example, My job): ")
# existing_cluster_id = input("ID of the existing cluster in the workspace to run the job on (for example, 1234-567890-ab123cd4): ")
# notebook_path       = input("Workspace path of the notebook to run (for example, /Users/someone@example.com/my-notebook): ")
# task_key            = input("Some key to apply to the job's tasks (for example, my-key): ")

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
)

print(f"View the job at {w.config.host}/#job/{j.job_id}\n")


# COMMAND ----------

"job_clusters": [
    {
      "job_cluster_key": "mg-synthea-data-gen",
      "new_cluster": {
        "cluster_name": "",
        "spark_version": "14.3.x-scala2.12",
        "spark_conf": {
          "spark.master": "local[*, 4]",
          "spark.databricks.cluster.profile": "singleNode"
        },
        "custom_tags": {
          "ResourceClass": "SingleNode"
        },
        "spark_env_vars": {
          "JNAME": "zulu17-ca-amd64"
        },
        "#########comment": "instance_pool_id and driver_instance_pool_id are specific to the workspace",
        "instance_pool_id": "0727-104344-hauls13-pool-uftxk0r6", 
        "driver_instance_pool_id": "0727-104344-hauls13-pool-uftxk0r6",
        "data_security_mode": "SINGLE_USER",
        "runtime_engine": "STANDARD",
        "num_workers": 0
      }
    }
  ],
  "queue": {
    "enabled": true
  },
  "parameters": [
    {
      "name": "catalog_name",
      "default": "mgiglia"
    },
    {
      "name": "schema_name",
      "default": "synthea"
    }
  ],
  "run_as": {
    "user_name": "matthew.giglia@databricks.com"
  }

# COMMAND ----------

    {
      "task_key": "synthea_set_up_check",
      "run_if": "ALL_SUCCESS",
      "notebook_task": {
        "notebook_path": "/Workspace/Users/matthew.giglia@databricks.com/db-nosql/00-setup-notebooks/set-up-check",
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
