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

results_schema = StructType([
    StructField('constructorId', IntegerType(), True),
    StructField('driverId', IntegerType(), True),
    StructField('fastestLap', IntegerType(), True),
    StructField('fastestLapSpeed', FloatType(), True),
    StructField('fastestLapTime', StringType(), True),
    StructField('grid', IntegerType(), True),
    StructField('laps', IntegerType(), True),
    StructField('milliseconds', IntegerType(), True),
    StructField('number', IntegerType(), True),
    StructField('points', DoubleType(), True),
    StructField('position', StringType(), True),
    StructField('positionOrder', IntegerType(), True),
    StructField('positionText', StringType(), True),
    StructField('raceId', IntegerType(), True),
    StructField('rank', IntegerType(), True),
    StructField('resultId', IntegerType(), True),
    StructField('statusId', IntegerType(), True),
    StructField('time', StringType(), True)
])

# COMMAND ----------

# MAGIC %md ##### ingest results file

# COMMAND ----------

#incremental extract
results_df = (
    spark.read
    .option('header','True')
    .schema(results_schema) #Ingestion Schema
    .json(get_adls_path('raw',f'{file_date}/results.json'))
    .withColumnRenamed('resultId','result_id')
    .withColumnRenamed('raceId','race_id')
    .withColumnRenamed('driverId','driver_id')
    .withColumnRenamed('constructorId','constructor_id')
    .withColumnRenamed('positionText','position_text')
    .withColumnRenamed('positionOrder','position_order')
    .withColumnRenamed('fastestLap','fastest_lap_time')
    .withColumnRenamed('fastestLapSpeed','fastest_lap_speed')
    .withColumn("ingestion_timestamp",F.current_timestamp()) #audit column
    .withColumn('file_date',F.lit(file_date))
    .drop('statusId')
)

# COMMAND ----------

# MAGIC %md ##### Save as Parquet

# COMMAND ----------

merge_pattern(results_df, container, path, merge_col, dest_format)

dbutils.notebook.exit(TASK_RUN_SUCCESS_MSG)