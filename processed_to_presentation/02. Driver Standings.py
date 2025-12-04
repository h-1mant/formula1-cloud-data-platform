# Databricks notebook source
# MAGIC %run "/Workspace/Formula1/includes/configuration"

# COMMAND ----------

# MAGIC %md ##### Driver Standings

# COMMAND ----------

from pyspark.sql import Window as W
from pyspark.sql.functions import sum, when, count, col, desc, rank, asc

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

# read race results from previous notebook workflow dependency
race_results_df = (
    spark.read.format("delta").load(f"{get_adls_path('presentation', 'race_results')}")
    .filter(f"file_date = '{file_date}'")
)

# extract distinct years from latest batch
race_year_list = df_column_to_list(race_results_df, "race_year")

#identify which race_years were affected based on latest batch data, update driver_standings accordingly
race_results_df = (
    spark.read.format("delta").load(f"{get_adls_path('presentation', 'race_results')}")
    .filter(col("race_year").isin(race_year_list))
)

#update driver standings for all years in incremental extract
driver_standings_df = (
    race_results_df.groupBy(
        "race_year", "driver_name", "driver_nationality", "team"
    )
    .agg(
        sum("points").alias("total_points"),
        count(when(col("position") == 1, True)).alias("wins"),
    )
)

driver_rank_spec = W.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))
final_df = driver_standings_df.withColumn("rank", rank().over(driver_rank_spec))

# COMMAND ----------

# MAGIC %md ##### incremental load

# COMMAND ----------

merge_pattern(final_df, container, path, merge_col, dest_format)

dbutils.notebook.exit(TASK_RUN_SUCCESS_MSG)