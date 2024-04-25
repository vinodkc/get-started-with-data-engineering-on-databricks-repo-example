# Databricks notebook source
# MAGIC %fs
# MAGIC ls /databricks-datasets/nyctaxi/tables/nyctaxi_yellow/

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS hive_metastore.vkc_schema;

# COMMAND ----------

# MAGIC %sql
# MAGIC use hive_metastore.vkc_schema;
# MAGIC CREATE TABLE IF NOT EXISTS  nyctaxi_yellowcab_table
# MAGIC USING DELTA
# MAGIC OPTIONS (
# MAGIC   path "/databricks-datasets/nyctaxi/tables/nyctaxi_yellow/" 
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from nyctaxi_yellowcab_table;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT vendor_id,
# MAGIC   SUM(trip_distance) as SumTripDistance,
# MAGIC   AVG(trip_distance) as AvgTripDistance,
# MAGIC   COUNT(trip_distance) as CountTripDistance
# MAGIC FROM nyctaxi_yellowcab_table
# MAGIC WHERE passenger_count IN (1, 2, 4)
# MAGIC GROUP BY vendor_id
# MAGIC ORDER BY vendor_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE nyctaxi_yellowcab_table;
# MAGIC DROP DATABASE  hive_metastore.vkc_schema;
