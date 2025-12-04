# Databricks notebook source
# MAGIC %md ##### imports

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, FloatType
import pyspark.sql.functions as F, types as T 
from pyspark.sql import Window as W
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %md ##### helper functions

# COMMAND ----------

def get_adls_path(container, path=None, storage_account="formula1dlhrm293"):
    abfs_url = f"abfss://{container}@{storage_account}.dfs.core.windows.net/"
    return abfs_url if not path else abfs_url+path

# COMMAND ----------

def set_adls_path(container, path=None, storage_account="formula1dlhrm293"):
    abfs_url = f"abfss://{container}@{storage_account}.dfs.core.windows.net/"
    return abfs_url if not path else abfs_url+path

# COMMAND ----------

def batch_load(df, container, path, dest_format):
    table_name = f'{container}.{path}'
    container_path = get_adls_path(container)
    table_path = get_adls_path(container,path)
    
    #if first load, create external database
    if not spark.catalog.tableExists(table_name):    
        spark.sql(f"""
              CREATE DATABASE IF NOT EXISTS {container}
              LOCATION '{container_path}'
        """)
    #overwrite table
    df.write.mode('overwrite').option('path',table_path).format(dest_format).saveAsTable(table_name)

# COMMAND ----------

def partition_col_last(df, table_name, partition_col):
    # Get target schema
    table_schema = spark.table(table_name).schema

    # Cast and select columns in exact order
    for field in table_schema:
        if field.name in df.columns:
            df = df.withColumn(field.name, F.col(field.name).cast(field.dataType))        
        else:
            raise Exception(f'Column: {field.name} not found in table schema!')
    
    #Re-arrange partition column to be last column for insertInto
    cols = [field.name for field in table_schema if field.name != partition_col] + [partition_col]

    return df.select(*cols)

# COMMAND ----------

def df_column_to_list(input_df, column_name):
  df_row_list = input_df.select(column_name) \
                        .distinct() \
                        .collect()
  
  column_value_list = [row[column_name] for row in df_row_list]
  return column_value_list

# COMMAND ----------

# MAGIC %md ##### Incremental Load: Partition Overwrite Method

# COMMAND ----------

def incremental_load(df, container, path, partition_col, dest_format):    
    table_name = f'{container}.{path}'
    container_path = get_adls_path(container)
    table_path = get_adls_path(container,path)

    #for first load, create schema and external table
    if not spark.catalog.tableExists(table_name):
        spark.sql(f"""
              CREATE DATABASE IF NOT EXISTS {container}
              LOCATION '{container_path}'
        """)
        df.write.mode('overwrite').option('path',table_path).format(dest_format).partitionBy(partition_col).saveAsTable(table_name)        
    else: 
        ##### SPARK METHOD ######
        #set dynamic partition overwrite: overwrite existing partitions, insert new partitions
        spark.conf.set('spark.sql.sources.partitionOverwriteMode','dynamic')
        
        #insertInto expects partition_col to be last column
        df = partition_col_last(df, table_name, partition_col)

        df.write.mode('overwrite').format(dest_format).insertInto(table_name)

# COMMAND ----------

# MAGIC %md ##### Incremental Load: Delta Table Merge Pattern

# COMMAND ----------

def merge_pattern(df, container, path, merge_col, dest_format):
    table_name = f'{container}.{path}'
    container_path = get_adls_path(container)
    table_path = get_adls_path(container,path)

    #for first load, create schema and external table
    if not spark.catalog.tableExists(table_name): 
        spark.sql(f"""
              CREATE DATABASE IF NOT EXISTS {container}
              LOCATION '{container_path}'
        """)
        df.write.mode('overwrite').option('path',table_path).format(dest_format).partitionBy(merge_col).saveAsTable(table_name)        
    else:
        deltaTableTarget = DeltaTable.forPath(spark, get_adls_path(container,path))        
        #source df to be merged into target delta table
        (
            deltaTableTarget.alias("target")
            .merge(source=df.alias('source'),
                   condition=f"target.{merge_col} = source.{merge_col}"
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )


# COMMAND ----------

# MAGIC %md ##### configuration parameters

# COMMAND ----------

DESTINATION_PATH_NAME = "destination_path"
DESTINATION_CONTAINER_NAME = "destination_container"
DESTINATION_FORMAT_NAME = "destination_format"

TASK_RUN_SUCCESS_MSG = "Success" 

FILE_DATE_NAME = "file_date"