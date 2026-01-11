# Databricks notebook source
# MAGIC %md
# MAGIC ##### **Fact And Dimension Tables**

# COMMAND ----------

df = spark.sql("select * from ecommerce_catalog.silver.orders")
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### **Creating Dimension tables**

# COMMAND ----------

df_dim_cus = spark.sql("select dim_key , customer_id from ecommerce_catalog.gold.customers")


df_dim_pro = spark.sql("select product_id as pro_dim_key , product_id  from ecommerce_catalog.silver.product")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### **Creating Fact Table**

# COMMAND ----------

df_fact = df.join(df_dim_cus , df_dim_cus["customer_id"] == df["customer_id"] , "left" )\
            .join (df_dim_pro , df_dim_pro["pro_dim_key"] == df["product_id"] , "left")

# COMMAND ----------

df_fact.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### **Drop column from fact table which are not numeric**

# COMMAND ----------

# Droping columns which are not needed. we have dim_key for both product and customer, So storing customer_id and product_id does not make any sense.
df_fact = df_fact.drop("customer_id","product_id" , "order_status")

# COMMAND ----------

df_fact.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### **Applying upsert operation on fact table**

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

if spark.catalog.tableExists("ecommerce_catalog.gold.fact"):
    
    dlt_obj = DeltaTable.forName(spark , "ecommerce_catalog.gold.fact")
    dlt_obj.alias("t").merge(df_fact.alias("s") , "t.dim_key = s.dim_key and t.pro_dim_key = s.pro_dim_key and t.order_id = s.order_id ")\
        .whenMatchedUpdateAll()\
        .whenNotMatchedInsertAll()\
        .execute()

else:
    df_fact.write.format("delta")\
        .saveAsTable("ecommerce_catalog.gold.fact")

# COMMAND ----------

df = spark.sql("select * from ecommerce_catalog.gold.fact")
df.display()

# COMMAND ----------

