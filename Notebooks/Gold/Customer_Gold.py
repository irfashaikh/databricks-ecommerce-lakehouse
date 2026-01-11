# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ##### **Data Reading from source(silver layer)**

# COMMAND ----------

df = spark.read.table("ecommerce_catalog.silver.customers")
df.limit(5).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### **Creating a parameter** 

# COMMAND ----------

dbutils.widgets.text("load_flag" , "1")
load_flag = int(dbutils.widgets.get("load_flag"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### **Removing duplicates**

# COMMAND ----------

df = df.dropDuplicates(subset = ['customer_id'])
df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC #### **Dividing old and new records**

# COMMAND ----------

if load_flag == 0:
  df_old = spark.sql('''select dim_key , customer_id , create_date , update_date
                     from ecommerce_catalog.gold.customers''')
else:
  df_old = spark.sql('''select 0 dim_key , 0 customer_id , 0 create_date , 0 update_date
                     from ecommerce_catalog.silver.customers where 1=0''')

# COMMAND ----------

df_old.display()

# COMMAND ----------

df_old = df_old.withColumnRenamed("create_date" , "old_create_date")\
    .withColumnRenamed("update_date" , "old_update_date")\
    .withColumnRenamed("customer_id" , "old_customer_id")

# COMMAND ----------

df_old.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### **Applying join to df_old**

# COMMAND ----------

df_join = df.join(df_old, df.customer_id==df_old.old_customer_id , "left")

# COMMAND ----------

df_join.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### **Seperating old and new records on dim_key**

# COMMAND ----------

df_new = df_join.filter(df_join["dim_key"].isNull())

# COMMAND ----------

df_old = df_join.filter(df_join["dim_key"].isNotNull())

# COMMAND ----------

df_old.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### **preparing df_old**

# COMMAND ----------

# droping columns which are not needed
df_old = df_old.drop("old_create_date" , "old_customer_id" ,"old_update_date")

# update_date should be updated as we processed this data
df_old = df_old.withColumn("update_date" , current_timestamp())

# COMMAND ----------

df_old.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### **preparing df_new**

# COMMAND ----------

df_new.limit(5).display()

# COMMAND ----------

df_new = df_new.drop("old_create_date" , "old_customer_id" , "old_update_date")

# As it is new df the currnt_date and update_date should be current timestamp.
df_new = df_new.withColumn("create_date" , current_timestamp())\
        .withColumn("update_date" , current_timestamp())

# COMMAND ----------

df_new.limit(5).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### **Adding dim_key values**

# COMMAND ----------

df_new = df_new.withColumn("dim_key" , monotonically_increasing_id()+lit(1))

# COMMAND ----------

df_new.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### **Adding max_dim_key to a df_new**

# COMMAND ----------

if load_flag == 1:
  max_dim_key = 0

else:
  df_dim = spark.sql('''select max(dim_key) as max_dim_key 
                    from ecommerce_catalog.gold.customers''')
  
  max_dim_key = df_dim.collect()[0]['max_dim_key']

# COMMAND ----------

df_new = df_new.withColumn("dim_key" , lit(max_dim_key) + col("dim_key"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### **Taking union of df_old and df_new**

# COMMAND ----------

df_final = df_new.unionByName(df_old)

# COMMAND ----------

df_final.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### **SCD Type 1**

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

table_name = "ecommerce_catalog.gold.customers"

if spark.catalog.tableExists(table_name):

    delta_tbl = DeltaTable.forName(spark, table_name)

    (
        delta_tbl.alias("trg")
        .merge(
            df_final.alias("src"),
            "trg.dim_key = src.dim_key"
        )
        .whenMatchedUpdateAll()      # SCD Type 1 overwrite
        .whenNotMatchedInsertAll()
        .execute()
    )

else:
    (
        df_final.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(table_name)
    )
