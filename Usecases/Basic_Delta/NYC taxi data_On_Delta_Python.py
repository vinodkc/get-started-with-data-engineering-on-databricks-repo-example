# Databricks notebook source
# MAGIC %fs
# MAGIC ls /databricks-datasets/nyctaxi/tables/nyctaxi_yellow/

# COMMAND ----------

spark.sql("CREATE DATABASE IF NOT EXISTS hive_metastore.vkc_schema")

# COMMAND ----------

spark.sql("USE hive_metastore.vkc_schema")
spark.sql("""
CREATE TABLE IF NOT EXISTS nyctaxi_yellowcab_table
USING DELTA
OPTIONS (
  path "/databricks-datasets/nyctaxi/tables/nyctaxi_yellow/"
)
""")

# COMMAND ----------

df = spark.read.format("delta").load("/databricks-datasets/nyctaxi/tables/nyctaxi_yellow/")
display(df)

# COMMAND ----------

# import the required functions
from pyspark.sql.functions import sum, avg, count
from pyspark.sql.types import FloatType

# read the nyctaxi_yellowcab_table into a dataframe
nyctaxi_yellowcab_df = spark.read.table("nyctaxi_yellowcab_table")

# cast the "trip_distance" column to FloatType
nyctaxi_yellowcab_df = nyctaxi_yellowcab_df.withColumn("trip_distance", nyctaxi_yellowcab_df["trip_distance"].cast(FloatType()))

# perform the required aggregation and filtering operations
result_df = nyctaxi_yellowcab_df.filter(nyctaxi_yellowcab_df.passenger_count.isin([1, 2, 4])) \
    .groupBy("vendor_id") \
    .agg(sum("trip_distance").alias("SumTripDistance"),
         avg("trip_distance").alias("AvgTripDistance"),
         count("trip_distance").alias("CountTripDistance")) \
    .orderBy("vendor_id")

# display the result dataframe
display(result_df)

# COMMAND ----------

# Drop table
spark.sql("DROP TABLE nyctaxi_yellowcab_table")

# Drop database
spark.sql("DROP DATABASE hive_metastore.vkc_schema")
