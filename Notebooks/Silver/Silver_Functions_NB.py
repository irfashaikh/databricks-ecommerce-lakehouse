# Databricks notebook source
df = spark.read.table("ecommerce_catalog.bronze.bronze_ecommerce")

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### **Spliting customer name to first name and last name and delete the customer name column**

# COMMAND ----------

from pyspark.sql.functions import *

df = df.withColumn("first_name" , split(col("customer_name"), ' ')[0])
df = df.withColumn("last_name" , split(col("customer_name"), ' ') [1])
# 0 - first instance
# 1 - second instance

# COMMAND ----------

df.drop('customer_name')
display(df)

# COMMAND ----------

df.withColumn("Full_name" , concat(col("first_name") , lit(' '), col('last_name'))).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Creating Functions in Databricks Catalogs**

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace function ecommerce_catalog.bronze.discount_fun(after_disount double)
# MAGIC returns double
# MAGIC language sql
# MAGIC return after_disount * 0.90 
# MAGIC
# MAGIC -- The function is defined in the sql language in databricks catalog.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select order_id , category ,total_amount, ecommerce_catalog.bronze.discount_fun(total_amount) as discounted_amount 
# MAGIC from ecommerce_catalog.bronze.bronze_ecommerce
# MAGIC
# MAGIC -- same function is getting fetched from the catalog and used in the query to display the records.

# COMMAND ----------

df = df.withColumn("discounted_column" , expr("ecommerce_catalog.bronze.discount_fun(total_amount)"))
df = df.withColumnRenamed("discounted_column" , "discounted_price")
df.select("total_amount" , "discounted_price").display()

# In this way we create a functions in the catalog and use then in SQL or in Pyspark.
# To use in pyspark, we have to use EXPR funtion.

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Creating functions in python**

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace function ecommerce_catalog.bronze.upper(upper_country string)
# MAGIC returns string 
# MAGIC language sql
# MAGIC return UPPER(upper_country)
# MAGIC
# MAGIC -- The function is defined in the sql language in databricks catalog.

# COMMAND ----------

df = df.withColumn("country" , expr("ecommerce_catalog.bronze.upper(country)")).display()

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace function ecommerce_catalog.bronze.lower(lower_country string)
# MAGIC returns string 
# MAGIC language python
# MAGIC as 
# MAGIC $$
# MAGIC     return lower_country.lower()
# MAGIC $$

# COMMAND ----------

