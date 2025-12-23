# Databricks notebook source
# MAGIC %md
# MAGIC ### **Data Ingestion**

# COMMAND ----------

# MAGIC %md
# MAGIC ##### **Manually Data Ingestion**

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

df_customer = spark.read.format('csv')\
      .option("header", "true")\
      .option("inferSchema", "true")\
      .load('/Volumes/ecommerce_catalog/source/ecommerce_source_volume/customers.csv')

df_order = spark.read.format('csv')\
      .option("header", "true")\
      .option("inferSchema", "true")\
      .load('/Volumes/ecommerce_catalog/source/ecommerce_source_volume/orders.csv')


df_product = spark.read.format('csv')\
      .option("header", "true")\
      .option("inferSchema", "true")\
      .load('/Volumes/ecommerce_catalog/source/ecommerce_source_volume/products.csv')

# COMMAND ----------

# MAGIC %md
# MAGIC #### **Adding Metadata for all the datasets**

# COMMAND ----------

df_customer = df_customer.withColumn('created_date' , current_timestamp())\
            .withColumn('update_date' , current_timestamp()).display()

# COMMAND ----------

df_order = df_order.withColumn('created_date' , current_timestamp())\
            .withColumn('update_date' , current_timestamp())

df_product = df_product.withColumn('created_date' , current_timestamp())\
            .withColumn('update_date' , current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ##### **Writing data in Bronze layer**

# COMMAND ----------

df_customer.write\
    .format('delta')\
    .mode('append')\
    .option('mergeschema' , True)\
    .saveAsTable("ecommerce_catalog.bronze.customer")



# COMMAND ----------

df_order.write\
    .format('delta')\
    .mode('append')\
    .option('mergeschema' , True)\
    .saveAsTable("ecommerce_catalog.bronze.order")


df_customer.write\
    .format('delta')\
    .mode('append')\
    .option('mergeschema' , True)\
    .saveAsTable("ecommerce_catalog.bronze.product")    