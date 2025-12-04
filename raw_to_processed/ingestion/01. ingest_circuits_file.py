# Databricks notebook source
# MAGIC %run "/Workspace/Formula1/includes/configuration"

# COMMAND ----------

# MAGIC %md ##### read notebook workflow params

# COMMAND ----------

# params = dbutils.notebook.entry_point.getCurrentBindings()
# container = params.get(DESTINATION_CONTAINER_NAME) #external database (container)
# path = params.get(DESTINATION_PATH_NAME) #folder name
# dest_format = params.get(DESTINATION_FORMAT_NAME)
# merge_col = params.get('merge_col')
# file_date = params.get(FILE_DATE_NAME)

# COMMAND ----------

#register widgets to be populated by ADF 
dbutils.widgets.text("container", "")
dbutils.widgets.text("path", "")
dbutils.widgets.text("dest_format", "")
dbutils.widgets.text("merge_col", "")
dbutils.widgets.text("file_date", "")

container = dbutils.widgets.get("container")
path = dbutils.widgets.get("path")
dest_format = dbutils.widgets.get("dest_format")
merge_col = dbutils.widgets.get("merge_col")
file_date = dbutils.widgets.get("file_date")

print(container, path, dest_format, merge_col, file_date)

# COMMAND ----------

# MAGIC %md ##### Set Ingestion Schema

# COMMAND ----------

#batch extract
circuits_schema = StructType([
    StructField('circuitId',IntegerType(),False),
    StructField('circuitRef',StringType(),True),
    StructField('name',StringType(),True),
    StructField('location',StringType(),True),
    StructField('country',StringType(),True),
    StructField('lat',DoubleType(),True),
    StructField('lon',DoubleType(),True),
    StructField('alt',IntegerType(),True),
    StructField('url',StringType(),True)
]) 

# COMMAND ----------

# MAGIC %md ##### ingest circuits file

# COMMAND ----------

circuits_df = (
    spark.read
    .option('header','True')
    .schema(circuits_schema) #Ingestion Schema
    .csv(get_adls_path('raw',f'{file_date}/circuits.csv'))
    .withColumnRenamed('circuitId','circuit_id')
    .withColumnRenamed('circuitRef','circuit_ref')
    .withColumnRenamed('lat','latitude')
    .withColumnRenamed('lon','longitude')
    .withColumnRenamed('alt','altitude')
    .withColumn("ingestion_timestamp",F.current_timestamp()) #audit column
    .withColumn('file_date',F.lit(file_date))
    .drop('url')
)

# COMMAND ----------

# MAGIC %md ##### Save as Parquet

# COMMAND ----------

merge_pattern(circuits_df, container, path, merge_col, dest_format)

dbutils.notebook.exit(TASK_RUN_SUCCESS_MSG)