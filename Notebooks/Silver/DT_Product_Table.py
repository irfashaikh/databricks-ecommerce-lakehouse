# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

# COMMAND ----------

df_products = spark.read.table("ecommerce_catalog.bronze.products")

# COMMAND ----------

df_products.display()

# COMMAND ----------

df_products = df_products.drop("create_date")\
    .withColumnRenamed("created_date" , "create_date")

# COMMAND ----------

df_products.display()

# COMMAND ----------

df_products = df_products.withColumn("create_date" , to_timestamp(col("create_date")))

# COMMAND ----------

df_products.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### **Writing data into silver layer**

# COMMAND ----------

df_products.write.format("delta")\
    .mode("append")\
    .option("mergeSchema", "true")\
    .saveAsTable("ecommerce_catalog.silver.product")