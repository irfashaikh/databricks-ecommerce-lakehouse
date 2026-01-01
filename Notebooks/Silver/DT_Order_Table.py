# Databricks notebook source
# MAGIC %md
# MAGIC #### **Orders - Fetching data from source(bronze layer)**

# COMMAND ----------

from pyspark.sql.functions import * 
from pyspark.sql.types import *
from pyspark.sql.window import Window

# COMMAND ----------

df_orders = spark.read.table("ecommerce_catalog.bronze.orders")
df_orders.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### **checking duplicates records are there or not**

# COMMAND ----------

df_orders.count()

# COMMAND ----------

window_spe = Window.partitionBy("order_id").orderBy("order_date")

df_orders = df_orders.withColumn("rn" , row_number().over(window_spe))\
      .filter(col('rn')== 1)

# COMMAND ----------

df_orders.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### **Checking if Null values are there or not**

# COMMAND ----------

df_orders.select([
  sum(col(c).isNull().cast("int")).alias(c)
  for c in df_orders.columns
]).display()

# COMMAND ----------

df_orders = df_orders.drop("rn")

# COMMAND ----------

df_orders.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### **Extracting year from order_date**

# COMMAND ----------

df_orders = df_orders.withColumn("year", year(col("order_date")))

# COMMAND ----------

df_orders.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### **Writing data into the silver layer**

# COMMAND ----------

df_orders.write.format("delta")\
    .mode("append")\
    .option("mergeSchema", "true")\
    .saveAsTable("ecommerce_catalog.silver.orders")