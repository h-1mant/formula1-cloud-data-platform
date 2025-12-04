# Databricks notebook source
# MAGIC %run "/Workspace/Formula1/includes/configuration"

# COMMAND ----------

# #read notebook workflow params
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

### Batch Extract

drivers_df = (
    spark.read.format("delta").load(get_adls_path('processed', 'drivers'))
    .withColumnRenamed("number", "driver_number")
    .withColumnRenamed("name", "driver_name")
    .withColumnRenamed("nationality", "driver_nationality")
)

constructors_df = (
    spark.read.format("delta").load(get_adls_path('processed', 'constructors'))
    .withColumnRenamed("name", "team")
)

circuits_df = (
    spark.read.format("delta").load(get_adls_path('processed', 'circuits'))
    .withColumnRenamed("location", "circuit_location")
)

races_df = (
    spark.read.format("delta").load(get_adls_path('processed', 'races'))
    .withColumnRenamed("name", "race_name")
    .withColumnRenamed("race_timestamp", "race_date")
)

# COMMAND ----------

### Incremental Extract
results_df = (
    spark.read.format("delta").load(get_adls_path('processed','results'))
    .filter(f"file_date = '{file_date}'")
    .withColumnRenamed("time", "race_time")
    .withColumnRenamed("race_id", "result_race_id")
    .withColumnRenamed("file_date", "result_file_date")
)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

### Transformations

race_circuits_df = (
    races_df.join(
        circuits_df,
        races_df.circuit_id == circuits_df.circuit_id,
        "inner"
    )
    .select(
        races_df.race_id,
        races_df.race_year,
        races_df.race_name,
        races_df.race_date,
        circuits_df.circuit_location
    )
)

race_results_df = (
    results_df
    .join(race_circuits_df, results_df.result_race_id == race_circuits_df.race_id)
    .join(drivers_df, results_df.driver_id == drivers_df.driver_id)
    .join(constructors_df, results_df.constructor_id == constructors_df.constructor_id)
)

final_df = (
    race_results_df
    .select(
        "race_id",
        "race_year",
        "race_name",
        "race_date",
        "circuit_location",
        "driver_name",
        "driver_number",
        "driver_nationality",
        "team",
        "grid",
        "fastest_lap_time",
        "race_time",
        "points",
        "position",
        "result_file_date"
    )
    .withColumn("created_date", current_timestamp())
    .withColumnRenamed("result_file_date", "file_date")
)

# COMMAND ----------

# MAGIC %md ##### Save as External Table (Parquet) in Presentation Layer

# COMMAND ----------

merge_pattern(final_df, container, path, merge_col, dest_format)

dbutils.notebook.exit(TASK_RUN_SUCCESS_MSG)