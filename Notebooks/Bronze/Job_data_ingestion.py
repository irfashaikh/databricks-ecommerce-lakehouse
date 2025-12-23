# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

dbutils.widgets.text("filename" , "")    # here order is a default value
p_filename = dbutils.widgets.get("filename")

# COMMAND ----------

df = spark.read.format("csv")\
        .option("header", "true")\
        .option("inferSchema", "true")\
        .load(f'/Volumes/ecommerce_catalog/source/ecommerce_source_volume/{p_filename}.csv')

# COMMAND ----------

df = df.withColumn("create_date" , current_timestamp())\
    .withColumn("update_date" , current_timestamp())

# COMMAND ----------

df.write\
    .format('delta')\
    .mode('append')\
    .option('mergeschema' , True)\
    .saveAsTable(f"ecommerce_catalog.bronze.{p_filename}_table")