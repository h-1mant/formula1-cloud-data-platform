# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS raw
# MAGIC LOCATION 'abfss://raw@formula1dlhrm293.dfs.core.windows.net/';

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS raw.circuits;
# MAGIC CREATE TABLE IF NOT EXISTS raw.circuits (
# MAGIC   circuitId INT,
# MAGIC   circuitRef STRING,
# MAGIC   name STRING,
# MAGIC   location STRING,
# MAGIC   country STRING,
# MAGIC   lat DOUBLE,
# MAGIC   long DOUBLE,
# MAGIC   alt INT,
# MAGIC   url STRING
# MAGIC )
# MAGIC USING CSV
# MAGIC OPTIONS (path "abfss://raw@formula1dlhrm293.dfs.core.windows.net/2021-03-28/circuits.csv", header TRUE)

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS raw.constructors;
# MAGIC CREATE TABLE IF NOT EXISTS raw.constructors (
# MAGIC   constructorId INT,
# MAGIC   constructorRef STRING,
# MAGIC   name STRING,
# MAGIC   nationality STRING,
# MAGIC   url STRING
# MAGIC )
# MAGIC USING JSON
# MAGIC OPTIONS (path "abfss://raw@formula1dlhrm293.dfs.core.windows.net/2021-03-28/constructors.json")

# COMMAND ----------

# MAGIC %sql 
# MAGIC DROP TABLE IF EXISTS raw.drivers;
# MAGIC CREATE TABLE IF NOT EXISTS raw.drivers (
# MAGIC   code STRING,
# MAGIC   dob STRING,
# MAGIC   driverId STRING,
# MAGIC   driverRef STRING,
# MAGIC   name STRUCT <forename: STRING, surname: STRING>,
# MAGIC   surname STRING,
# MAGIC   nationality STRING,
# MAGIC   number INT,
# MAGIC   url STRING
# MAGIC ) 
# MAGIC USING JSON
# MAGIC OPTIONS (path "abfss://raw@formula1dlhrm293.dfs.core.windows.net/2021-03-28/drivers.json")

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS raw.races;
# MAGIC CREATE TABLE IF NOT EXISTS raw.races (
# MAGIC   raceId INT,
# MAGIC   year INT,
# MAGIC   round INT,
# MAGIC   circuitId INT,
# MAGIC   name STRING,
# MAGIC   date STRING,
# MAGIC   time STRING,
# MAGIC   url STRING
# MAGIC )
# MAGIC USING CSV
# MAGIC OPTIONS (path "abfss://raw@formula1dlhrm293.dfs.core.windows.net/2021-03-28/races.csv", header TRUE)

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS raw.results;
# MAGIC CREATE TABLE IF NOT EXISTS raw.results (
# MAGIC   resultId INT,
# MAGIC   raceId INT,
# MAGIC   driverId INT,
# MAGIC   constructorId INT,
# MAGIC   number INT,
# MAGIC   grid INT,
# MAGIC   position INT,
# MAGIC   positionText STRING,
# MAGIC   positionOrder INT,
# MAGIC   points DOUBLE,
# MAGIC   laps INT,
# MAGIC   time STRING,
# MAGIC   milliseconds INT,
# MAGIC   fastestLap INT,
# MAGIC   rank INT,
# MAGIC   fastestLapTime STRING,
# MAGIC   fastestLapSpeed DOUBLE,
# MAGIC   statusId INT
# MAGIC )
# MAGIC USING JSON
# MAGIC OPTIONS (path "abfss://raw@formula1dlhrm293.dfs.core.windows.net/2021-03-28/results.json") 

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS raw.pit_stops;
# MAGIC CREATE TABLE IF NOT EXISTS raw.pit_stops (
# MAGIC   driverId INT,
# MAGIC   duration FLOAT,
# MAGIC   lap INT,
# MAGIC   milliseconds INT,
# MAGIC   raceId INT,
# MAGIC   stop INT,
# MAGIC   time STRING
# MAGIC )
# MAGIC USING JSON
# MAGIC OPTIONS (path "abfss://raw@formula1dlhrm293.dfs.core.windows.net/2021-03-28/pit_stops.json", multiline TRUE)  

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS raw.lap_times;
# MAGIC CREATE TABLE IF NOT EXISTS raw.lap_times (
# MAGIC   raceId INT,
# MAGIC   driverId INT,
# MAGIC   lap INT,
# MAGIC   position INT,
# MAGIC   time STRING,
# MAGIC   milliseconds INT
# MAGIC )
# MAGIC USING CSV
# MAGIC OPTIONS (path "abfss://raw@formula1dlhrm293.dfs.core.windows.net/2021-03-28/lap_times/")

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS raw.qualifying;
# MAGIC CREATE TABLE IF NOT EXISTS raw.qualifying (
# MAGIC   constructorId INT NOT NULL,
# MAGIC   driverId INT,
# MAGIC   number INT,
# MAGIC   position INT,
# MAGIC   q1 STRING,
# MAGIC   q2 STRING,
# MAGIC   q3 STRING,
# MAGIC   qualifyId INT,
# MAGIC   raceId INT
# MAGIC )
# MAGIC USING JSON
# MAGIC OPTIONS (path "abfss://raw@formula1dlhrm293.dfs.core.windows.net/2021-03-28/qualifying/", multiLine TRUE)