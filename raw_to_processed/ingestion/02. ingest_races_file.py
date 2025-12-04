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

# MAGIC %md ##### Set Ingestion Schema

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

#batch extract
races_schema = StructType([
    StructField('raceId',IntegerType(),False),
    StructField('year',IntegerType(),True),
    StructField('round',IntegerType(),True),
    StructField('circuitId',IntegerType(),True),
    StructField('name',StringType(),True),
    StructField('date',StringType(),True),
    StructField('time',StringType(),True),
    StructField('url',StringType(),True)
]) 

# COMMAND ----------

# MAGIC %md ##### ingest races file

# COMMAND ----------

races_df = (
    spark.read
    .option('header','True')
    .schema(races_schema) #Ingestion Schema
    .csv(get_adls_path('raw',f'{file_date}/races.csv'))
    .withColumnRenamed('raceId','race_id')
    .withColumnRenamed('year','race_year')
    .withColumnRenamed('circuitId','circuit_id')
    .withColumn('race_timestamp',F.to_timestamp(F.concat(F.col('date'),F.lit(' '),F.col('time')),'yyyy-MM-dd HH:mm:ss')) #create race timestamp col 
    .withColumn("ingestion_timestamp",F.current_timestamp()) #audit column
    .withColumn('file_date',F.lit(file_date))
    .drop('date','time','url')
)

# COMMAND ----------

# MAGIC %md ##### Save as Parquet

# COMMAND ----------

merge_pattern(races_df, container, path, merge_col, dest_format)

dbutils.notebook.exit(TASK_RUN_SUCCESS_MSG)