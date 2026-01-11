# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

source_path = 'ecommerce_catalog.silver.customers'

df = spark.read.table(source_path)
df.limit(5).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### **Creating a parameter**

# COMMAND ----------

dbutils.widgets.text("flag", '1')
variable = int(dbutils.widgets.get("flag"))

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### **Dividing old and new records**

# COMMAND ----------

target_path = 'ecommerce_catalog.gold.customer_dyn'

# COMMAND ----------

if variable == 0:
    df_old = spark.sql(f'''SELECT dim_key, customer_id , create_date , update_date
                   from {target_path}''')
    
else:
    df_old = spark.sql(f'''select 0 dim_key , 0 customer_id , 0 create_date , 0 update_date 
                       from {source_path} where 1 = 0''')



# COMMAND ----------

df_old.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### **Renaming the columns name to avoid confusion in old and new df**

# COMMAND ----------

df_old = df_old.withColumnRenamed("customer_id" , "old_customer_id")\
      .withColumnRenamed("create_date" , "old_create_date")\
      .withColumnRenamed("update_date" , "old_update_date")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### **Applying joins on old_df and new_df**

# COMMAND ----------

df_join = df.join(df_old , df.customer_id == df_old.old_customer_id , "left")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### **seperating old and new records**

# COMMAND ----------

df_new = df_join.filter(df_join["dim_key"].isNull())

# COMMAND ----------

df_old = df_join.filter(df_join["dim_key"].isNotNull())

# COMMAND ----------

df_old.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### **Preparing old df**

# COMMAND ----------

# Droping columns which are not required

df_old = df_old.drop("old_customer_id" , "old_create_date" ,"old_update_date" )

# update date should be new and which is live.

df_old = df_old.withColumn("update_date" , current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ##### **Preparing df_new**

# COMMAND ----------

df_new.display()

# COMMAND ----------

df_new = df_new.drop("old_customer_id" , "old_create_date" , "old_update_date")

df_new = df_new.withColumn("create_date" , current_timestamp())\
        .withColumn("update_date" , current_timestamp())


# COMMAND ----------

# MAGIC %md
# MAGIC ##### **Adding dim key to the df**

# COMMAND ----------

df_new = df_new.withColumn("dim_key" , monotonically_increasing_id()+lit(1))
df_new.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### **Adding max dim key to the df_new**

# COMMAND ----------

if variable == 1:
  max_dim = 0

else:
  max_dim_df = spark.sql(f'''select max(dim_key) as max_dim 
                        from {target_path}''')

max_dim = max_dim_df.collect()[0]['max_dim']

# COMMAND ----------

# MAGIC %md
# MAGIC ##### **dim_key of new_df should be incrementing from df_old_last +1**

# COMMAND ----------

df_new = df_new.withColumn("dim_key" , lit(max_dim) + col("dim_key"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### **Taking Union of two dataframe**

# COMMAND ----------

df_end = df_old.unionByName(df_new)

# COMMAND ----------

df_end.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### **SCD Type 1**

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

if spark.catalog.tableExists(target_path):

    delta_tbl = DeltaTable.forName(spark, target_path)

    (
        delta_tbl.alias("trg")
        .merge(
            df_end.alias("src"),
            "trg.dim_key = src.dim_key"
        )
        .whenMatchedUpdateAll()      # SCD Type 1 overwrite
        .whenNotMatchedInsertAll()
        .execute()
    )

else:
    (
        df_end.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(target_path)
    )


# COMMAND ----------

