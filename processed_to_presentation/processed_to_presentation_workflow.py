# Databricks notebook source
# MAGIC %run "/Workspace/Formula1/includes/configuration"

# COMMAND ----------

# MAGIC %md ##### Set Up Notebook Workflow to ingest all files

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS presentation
# MAGIC LOCATION "abfss://presentation@formula1dlhrm293.dfs.core.windows.net/";          

# COMMAND ----------

params = [
    {
        'file_name': "/Workspace/Formula1/processed_to_presentation/01. Race Results",
        'timeout_limit': 0,  # no timeout limit
        'destination_parameters': {
            'destination_container': 'presentation',
            'destination_path': 'race_results',
            'destination_format': 'delta',
            'merge_col': 'race_id',
            'file_date': '2021-04-18',
        }
    },
    {
        'file_name': "/Workspace/Formula1/processed_to_presentation/02. Driver Standings",
        'timeout_limit': 0,
        "destination_parameters": {
            'destination_container': 'presentation',
            'destination_path': 'driver_standings',
            'destination_format': 'delta',
            'merge_col': 'race_year',
            'file_date': '2021-04-18'
        }
    },
    {
        'file_name': "/Workspace/Formula1/processed_to_presentation/03. Constructor Standings",
        'timeout_limit': 0,
        "destination_parameters": {
            'destination_container': 'presentation',
            'destination_path': 'constructor_standings',
            'destination_format': 'delta',
            'merge_col': 'race_year',
            'file_date': '2021-04-18'
        }
    }
]

# COMMAND ----------

for param in params:
    try:
        run_status = dbutils.notebook.run(
            path=param['file_name'],
            timeout_seconds=param['timeout_limit'],
            arguments=param['destination_parameters']
        )
        #custom dependency enforcing
        if run_status != TASK_RUN_SUCCESS_MSG:
            raise Exception(f"Notebook Workflow Failed at {param['file_name']}")
            break
        print(f"Notebook Workflow Succeeded at {param['file_name']}")
    except Exception as e:
        print(f"Notebook Workflow Failed at {param['file_name']}: {e}")
        break