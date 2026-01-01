# Databricks notebook source
# MAGIC %md
# MAGIC ##### **Fetching data from source(bronze layer)**

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

# COMMAND ----------

df_customer = spark.read.table("ecommerce_catalog.bronze.customers")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### **Checking if duplicates record exist or not**

# COMMAND ----------

window_spe = Window.partitionBy('customer_id').orderBy('signup_date')

df_customer = df_customer.withColumn('rn' , row_number().over(window_spe))\
    .filter(col('rn') == 1)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### **Deleting the rn column as we don't need it.**

# COMMAND ----------

df_customer = df_customer.drop('rn')

# COMMAND ----------

# MAGIC %md
# MAGIC ##### **Checking if Null values are present or not**

# COMMAND ----------

from pyspark.sql.functions import col, sum

null_check_df = df_customer.select([
    sum(col(c).isNull().cast("int")).alias(c)
    for c in df_customer.columns
])

null_check_df.display()

# if the sum of all the columns is zero, then there are no null values.

# COMMAND ----------

# MAGIC %md
# MAGIC ##### **Extracting year column from the SignIn date**

# COMMAND ----------

df_customer = df_customer.withColumn("year" , year(col("signup_date")))

# COMMAND ----------

df_customer.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### **Writing the code in silver layer as a delta table**

# COMMAND ----------

df_customer.write.format("delta")\
    .mode("append")\
    .option("mergeSchema", "true")\
    .saveAsTable("ecommerce_catalog.silver.customers")

# COMMAND ----------

