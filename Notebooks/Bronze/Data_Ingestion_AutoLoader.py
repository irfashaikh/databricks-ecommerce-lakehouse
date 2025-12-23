# Databricks notebook source
# MAGIC %md
# MAGIC #### **Data Reading With Autoloader**

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ##### **1. Customer**

# COMMAND ----------

customer_df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.schemaLocation",
            "/Volumes/ecommerce_catalog/checkpoints/customer_schema")
    .option("header", "true")
    .load("/Volumes/ecommerce_catalog/bronze/customer")
    .withColumn("create_date", current_timestamp())
    .withColumn("source_file", input_file_name())
)


# COMMAND ----------

# MAGIC %md
# MAGIC ##### **2. Order**

# COMMAND ----------

order_df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.schemaLocation",
            "/Volumes/ecommerce_catalog/checkpoints/order_schema")
    .option("header", "true")
    .load("/Volumes/ecommerce_catalog/bronze/order")
    .withColumn("create_date", current_timestamp())
    .withColumn("source_file", input_file_name())
)


# COMMAND ----------

# MAGIC %md
# MAGIC ##### **3. Product**

# COMMAND ----------

product_df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.schemaLocation",
            "/Volumes/ecommerce_catalog/checkpoints/product_schema")
    .option("header", "true")
    .load("/Volumes/ecommerce_catalog/bronze/product")
    .withColumn("create_date", current_timestamp())
    .withColumn("source_file", input_file_name())
)


# COMMAND ----------

print(type(product_df))

# COMMAND ----------

# MAGIC %md
# MAGIC #### **Data writing with Autoloader**

# COMMAND ----------

# MAGIC %md
# MAGIC ##### **1. Customer**

# COMMAND ----------

customer_df.writeStream \
  .format("delta") \
  .outputMode("append") \
  .option("checkpointLocation",
          "/Volumes/ecommerce_catalog/bronze/checkpoints/customer") \
  .trigger(availableNow=True) \
  .toTable("ecommerce_catalog.bronze.customer")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### **2. order**

# COMMAND ----------

order_df.writeStream \
  .format("delta") \
  .outputMode("append") \
  .option("checkpointLocation",
          "/Volumes/ecommerce_catalog/bronze/checkpoints/order") \
  .trigger(availableNow=True) \
  .toTable("ecommerce_catalog.bronze.order")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### **product**

# COMMAND ----------

product_df.writeStream \
  .format("delta") \
  .outputMode("append") \
  .option("checkpointLocation",
          "/Volumes/ecommerce_catalog/bronze/checkpoints/product") \
  .trigger(availableNow=True)\
  .toTable("ecommerce_catalog.bronze.product")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### **Making the above notebook more dynamic**

# COMMAND ----------

# This will create a parameter for the filename as it is changing in our Autoloader Logic

dbutils.widgets.text("filename" , "orders")    # here order is a default value
p_filename = dbutils.widgets.get("filename")