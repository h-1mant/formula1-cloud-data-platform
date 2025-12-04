# Databricks notebook source
# MAGIC %run "/Workspace/Formula1/includes/configuration"

# COMMAND ----------

# MAGIC %md ##### Set Up Notebook Workflow to ingest all files

# COMMAND ----------

params = [
    {
        'file_name': "/Workspace/Formula1/raw_to_processed/ingestion/01. ingest_circuits_file",
        'timeout_limit': 0,  # no timeout limit
        'destination_parameters': {
            'destination_container': 'processed',
            'destination_path': 'circuits',
            'destination_format': 'delta',
            'merge_col': 'circuit_id',
            'file_date': '2021-03-28'         
        }
    },
    {
        'file_name': "/Workspace/Formula1/raw_to_processed/ingestion/02. ingest_races_file",
        'timeout_limit': 0,
        "destination_parameters": {
            'destination_container': 'processed',
            'destination_path': 'races',
            'destination_format': 'delta',
            'merge_col': 'race_id',
            'file_date': '2021-03-28'
        }
    },
    {
        'file_name': "/Workspace/Formula1/raw_to_processed/ingestion/03. ingest_constructors_file",
        'timeout_limit': 0,
        "destination_parameters": {
            'destination_container': 'processed',
            'destination_path': 'constructors',            
            'destination_format': 'delta',
            'merge_col': 'constructor_id',
            'file_date': '2021-03-28'
        }
    },
    {
        'file_name': "/Workspace/Formula1/raw_to_processed/ingestion/04. ingest_drivers_file",
        'timeout_limit': 0,
        "destination_parameters": {
            'destination_container': 'processed',
            'destination_path': 'drivers',
            'destination_format': 'delta',
            'merge_col': 'driver_id',
            'file_date': '2021-03-28'
        }
    },
    {
        'file_name': "/Workspace/Formula1/raw_to_processed/ingestion/05. ingest_results_file",
        'timeout_limit': 0,
        "destination_parameters": {
            'destination_container': 'processed',
            'destination_path': 'results',
            'destination_format': 'delta',
            'file_date': '2021-03-28',
            'merge_col': 'result_id'
        }
    },
    {
        'file_name': "/Workspace/Formula1/raw_to_processed/ingestion/06. ingest_pit_stops_file",
        'timeout_limit': 0,
        "destination_parameters": {
            'destination_container': 'processed',
            'destination_path': 'pit_stops',
            'destination_format': 'delta',
            'file_date': '2021-03-28',
            'merge_col': 'race_id'
        }
    },
    {
        'file_name': "/Workspace/Formula1/raw_to_processed/ingestion/07. ingest_lap_times_file",
        'timeout_limit': 0,
        "destination_parameters": {
            'destination_container': 'processed',
            'destination_path': 'lap_times',
            'destination_format': 'delta',
            'file_date': '2021-03-28',
            'merge_col': 'race_id'
        }
    },
    {
        'file_name': "/Workspace/Formula1/raw_to_processed/ingestion/08. ingest_qualifying_file",
        'timeout_limit': 0,
        "destination_parameters": {
            'destination_container': 'processed',
            'destination_path': 'qualifying',
            'destination_format': 'delta',
            'file_date': '2021-03-28',
            'merge_col': 'qualify_id'
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