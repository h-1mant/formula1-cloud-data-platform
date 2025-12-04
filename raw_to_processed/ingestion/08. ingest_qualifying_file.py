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

qualifying_schema = StructType([
    StructField('constructorId', IntegerType(), False),
    StructField('driverId', IntegerType(), True),
    StructField('number', IntegerType(), True),
    StructField('position', IntegerType(), True),
    StructField('q1', StringType(), True),
    StructField('q2', StringType(), True),
    StructField('q3', StringType(), True),
    StructField('qualifyId', IntegerType(), True),
    StructField('raceId', IntegerType(), True),
])

# COMMAND ----------

# MAGIC %md ##### ingest qualifying file

# COMMAND ----------

qualifying_df = (
    spark.read
    .option('header','True')
    .option('multiLine','True')
    .schema(qualifying_schema) #Ingestion Schema
    .json(get_adls_path('raw',f'{file_date}/qualifying/qualifying_split_*.json'))
    .withColumnRenamed('constructorId','constructor_id')
    .withColumnRenamed('qualifyId','qualify_id')
    .withColumnRenamed('raceId','race_id')
    .withColumn("ingestion_timestamp",F.current_timestamp()) #audit column
    .withColumn('file_date',F.lit(file_date))
)

# COMMAND ----------

# MAGIC %md ##### Save as Parquet

# COMMAND ----------

merge_pattern(qualifying_df, container, path, merge_col, dest_format)

dbutils.notebook.exit(TASK_RUN_SUCCESS_MSG)