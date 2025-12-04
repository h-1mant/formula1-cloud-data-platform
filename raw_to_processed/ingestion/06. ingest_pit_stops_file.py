# Databricks notebook source
# MAGIC %run "/Workspace/Formula1/includes/configuration"

# COMMAND ----------

# MAGIC %md ##### read notebook workflow params

# COMMAND ----------

# params = dbutils.notebook.entry_point.getCurrentBindings()
# container = params.get(DESTINATION_CONTAINER_NAME) #external database
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

pit_stops_schema = StructType([
    StructField('driverId', IntegerType(), False),
    StructField('duration', FloatType(), True),
    StructField('lap', IntegerType(), True),
    StructField('milliseconds', IntegerType(), True),
    StructField('raceId', IntegerType(), True),
    StructField('stop', IntegerType(), True),
    StructField('time', StringType(), True)
])

# COMMAND ----------

# MAGIC %md ##### ingest pit stops file

# COMMAND ----------

pit_stops_df = (
    spark.read
    .option('header','True')
    .option('multiLine','True')
    .schema(pit_stops_schema) #Ingestion Schema
    .json(get_adls_path('raw',f'{file_date}/pit_stops.json'))
    .withColumnRenamed('driverId','driver_id')
    .withColumnRenamed('raceId','race_id')
    .withColumn("ingestion_timestamp",F.current_timestamp()) #audit column
    .withColumn('file_date',F.lit(file_date))
)

# COMMAND ----------

# MAGIC %md ##### Save as Parquet

# COMMAND ----------

merge_pattern(pit_stops_df, container, path, merge_col, dest_format)

dbutils.notebook.exit(TASK_RUN_SUCCESS_MSG)