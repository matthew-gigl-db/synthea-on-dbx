# synthea-on-dbx
### Generate Synthetic Health Care Records using Synthea Directly On Databricks

Synthetic Health's [Synthea Patient Generator](https://github.com/synthetichealth/synthea) is a great resource for generating realistic looking synthetic health care data in a variety of formats including FHIR, C-CDA, CPCDS and a light weight, easy to use CSV format.  

The data includes encounters, conditions, allergies, care plans and more!  

Synthea's patient record generation may be executed by issueing shell commands with a downloadable JAR file.  The only requirements are Java's JDK version 11 or higher.  Historically Databricks users would execute Synthea on a local machine to generate some files and then upload them to cloud storage for use.  

The purpose of this repository is to allow a Databricks user to generate synthetic patient records with Synthea directly using a Databricks Workflow.  The workflow may be executed indepently once its created, or it may be looped with a random wait time to help simulate the variablity in patient records flowing into a health system.  This is particularly useful for devloping and testing streaming ETL methods such as for Spark Structured Streaming or Delta Live Tables.  

***

#### Usage:  

1. Clone this repo into a git controlled folder in your Databricks **Unity Catalog** enabled workspace of choice. The user must either be allowed to create a **catalog**, have full use permissions on an existing catalog to create a **schema**, or have permissions in a catalog.schema to use and create a **volume**.
1. Open the ***synthea-on-databricks*** notebook.
1. Attach the notebook to your Databricks Interactive Cluster of choice.  The notebook has been tested with both Databricks Generic Serverless compute and with a Single Node Assigned User cluster running DBR 14.3 LTS.  
1. Run the first few commands of the notebook to install or upgrade the Databricks Python SDK and set the required values for the Databricks Widgets:   
    
    a. **catalog_name**: the catalog that will be created and/or used for the volume that will be created. Note that there is no default for this value and must be set by the user.  

    b. **schema_name**: the schema that will be created and/or used for the volume that will be created.  The default for the schema name is "synthea".  Note that if more than one user wishes to run this notebook that either **catalog_name** or **schema_name** must be unique for a unique volume to be created.  It's OK for both users to write to the same volume as long as they both have permissions to use it.  

    b. **create_landing_zone**: optional configuration setting that indicates if an additional volume called landing will also be created. Default value is false, the landing volume will not be created. The intention of the landing zone is to copy files that are generated at the default **synthetic_raw_files** volume to the **landing** volume. 
    <br><br>Files in the _landing volume_ are organized into directories that contain files for a given table.<br>
       **Example:**
            <br> <img src="https://i.postimg.cc/Y2mNVQYR/landing.png" alt="drawing" width="400"/>
    <br><br>Files in the default _synthea_raw_files volume_ are organized into directories by timestamp, where each timestamp directory contains all of the files, or tables, for that given timestamp.<br>
        <br>**Example:**
            <br> <img src="https://i.postimg.cc/vZyv5D1z/synthea-raw-files.png" alt="drawing" width="400"/>
    <br>The landing zone volume is intended to simulate how files are landed and organized by ingestion process' in other scenarios compared to synthetic_raw_files. When create_landing_zone == True, the landing volume will be created alongside the default volume, synthea_raw_files.

    c. **node_type_id**: If an **instance_pool_id** is not set, the **node_type_id** specifies which type of compute resouce will be used to create the assigned single node job cluster used to create and execute the workflow.  This defaults to *i3.xlarge*, which is an AWS **node_type_id**.  The Databricks Python SDK Workspace Client may be used to list the available node types in your workspace with `nodes = w.clusters.list_node_types()`.  Please review the Databricks Python SDK [documentation](https://docs.databricks.com/en/dev-tools/sdk-python.html) for more information. 

    d. **instance_pool_id**: If your workspace has a pool of clusters available to be used for job compute, you may optionally specify the **instance_pool_id**.  These ids are specific to a Databricks workspace and can be found using the Databricks Python SDK Workspace Client with `pools = w.instance_pools.list()`.

    e. **number_of_job_runs**:  Once the Databricks Workflow has been posted to the Workspace, you can optionally loop its execution a number of times using this input variable.  The default is one time.  Interactively executing the final commands in the notebook will also execute the Workflow with `w.jobs.run_now_and_wait(...)`.  This variable is used for convenience when demonstrating later streaming workflows that will utilize the synthetic data generated. 
     
1. Run the rest of the ***synthea-on-databricks*** notebook to peform the following actions:    
  
    a. Post a conditional Databricks Workflow to the Workspace.  The workflow will check to see if Synthea's JAR and configuration file is available in the desired **volume** in Unity Catalog.  If its there, the workflow will immediately begin running the data generator that will generate a random number patient records between 1 and 1000.  If the JAR and configuration file are not available, the workflow will create or replace the catalog, schema, and volume required, download the latest JAR from the Synthea github page, and write the configuration file that will be used for its execution.  
    
    b. Excute the workflow from the notebook a given number of times as set above.   
  
***

#### Limitations: 

The first release of this workflow generates between 1 and 1000 patient records in a collection of 18 CSV files that are saved in the output/csv subfolder of the volume.  The patient records are all from the default geographic location: Boston, MA.  Synthea is capable of taking several options including different file output types such comprehensive HL7 FHIR.   Setting additional parameters will generate patients in different geographic locations.  

The notebook `notebooks/00-setup-notebooks/0.3-synthea-configuration` is capable of taking several Databricks Widget inputs and is responsible for writing the configuration file based on those inputs. Despite having those options the workflow has not been set up to accpet these additional values yet.  Future releases of this repo will add the additional options required in the **synthea-on-databricks** notebook to execute the workflow and provide more control over the configuration file creation.   




















  