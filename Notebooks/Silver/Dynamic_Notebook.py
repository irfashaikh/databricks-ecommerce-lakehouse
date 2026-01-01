# Databricks notebook source
source_file = 'ecommerce_catalog.bronze.bronze_ecommerce'

df = spark.read.table(f"{source_file}")

df.limit(10).display()

# COMMAND ----------

from pyspark.sql.functions import * 

# df.select(current_timestamp() , current_date()).limit(10).display()

# df.withColumn("order_date",to_date(col("order_date"))).limit(10).display()  # this function will convert to_timestamp function to a date function.

# df.withColumn("Year",year("order_date")).limit(10).display()

# df.withColumn("month_of_year" , month("order_date")).limit(10).display()

# df.withColumn("order_date" , date_format(col("order_date"), "yyyy-MM-dd")).limit(20).display()

df.limit(10).display()

# COMMAND ----------

df1 = df.withColumn("year" , year("order_date"))
df1.limit(29).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### **Window Functions**

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import *

# window_spe = Window.partitionBy("year").orderBy(col("year").desc())

# df1 = df1.withColumn("dense_rank" ,dense_rank().over(window_spe))

# df1 = df1.withColumn("rank" ,rank().over(window_spe)).limit(500).display()



mywindow = Window.partitionBy("order_status").orderBy("category")

df1 = df1.withColumn("dense_rank" , dense_rank().over(mywindow))
df1.withColumn("rank" , rank().over(mywindow)).display()
# df1.withColumn("sum" , count("total_amount").over(mywindow)).display()

# COMMAND ----------

# df1.filter(col('order_status').isNull()).display()
# df1.filter(col('order_id').isNull()).display()
# df1.filter(col('order_date').isNull()).display()

# df1.filter(col('customer_id').isNull()).display()
# df1.filter(col('customer_name').isNull()).display()

# df1.filter(col('product_id').isNull()).display()
# df1.filter(col('product_name').isNull()).display()

# Null_count = df1.select([
#     sum(col(c).isNull().cast("int")).alias(c)
#     for c in df1.columns
# ])
# Null_count.display()

# TO check the null records in the dataframe columns is not a good practice.
# to run a loop in the datagrame columns and calculate the null value in the column by adding sum function and we have to cast because aggregation won't work on boolean values.


# null_records = df1.select([col(c).isNull() for c in df1.columns])
# null_records.display()

# This code return a boolean records where the null value is there.


# COMMAND ----------

df1.na.drop().display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Creating a Class for code reuseability**

# COMMAND ----------

class win:

    mywindow = Window.partitionBy('order_status').orderBy('total_amount')

    def dense_rank_fun (self , df):
        dense_rank_df = df.withColumn("dense_rank" , dense_rank().over(mywindow))
        return dense_rank_df
    
    def rank_fun (self , df):
        rank_df = df.withColumn("rank" , rank().over(mywindow))
        return rank_df

    def row_number_fun (self , df):
        row_number_df = df.withColumn("row_number" , row_number().over(mywindow))
        return row_number_df

# COMMAND ----------

obj = win()
result = obj.dense_rank_fun(df)
result = obj.rank_fun(result)
result = obj.row_number_fun(result)
display(result)


# COMMAND ----------

result = result.withColumn("year" , year(col("order_date"))).display()

# COMMAND ----------

df.count()

# COMMAND ----------

df = df.dropna()
df.count()

# COMMAND ----------

df.write.format("delta")\
    .mode("append")\
    .option("mergeSchema", "true")\
    .saveAsTable("ecommerce_catalog.silver.silver_ecommerce")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- select * from ecommerce_catalog.silver.silver_ecommerce
# MAGIC select count(*) from ecommerce_catalog.silver.silver_ecommerce

# COMMAND ----------

