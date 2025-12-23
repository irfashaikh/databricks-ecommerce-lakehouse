# Databricks notebook source
# MAGIC %md
# MAGIC ### **Data Reading With Autoloader**

# COMMAND ----------

df = (
  spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.inferSchema", "true")
    .option("cloudFiles.schemaLocation", "/mnt/schema/bronze")
    .load("/mnt/raw-data/")                                        -- Lo
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Data writing with Autoloader**

# COMMAND ----------

df.writeStream \
  .format("delta") \
  .option("checkpointLocation", "/mnt/checkpoints/bronze") \
  .outputMode("append") \
  .trigger(once = True)\
  .start("/mnt/delta/bronze_table")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### **Making the above notebook more dynamic**

# COMMAND ----------

# This will create a parameter for the filename as it is changing in our Autoloader Logic

dbutils.widgets.text("filename" , "orders")    # here order is a default value
p_filename = dbutils.widgets.get("filename")

# COMMAND ----------

