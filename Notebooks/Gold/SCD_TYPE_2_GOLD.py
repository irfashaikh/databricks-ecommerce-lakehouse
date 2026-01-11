# Databricks notebook source
import dlt

# COMMAND ----------

df = spark.read.table("ecommerce_catalog.silver.product")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### **Exceptations**

# COMMAND ----------

my_rules = {
    "rule1" : "product_id IS NOT NULL" ,
    "rule2" : "product_name IS NOT NULL" ,
    "rule3" : "category IS NOT NULL" ,
    "rule4" : "price IS NOT NULL" ,
}

# COMMAND ----------

# MAGIC %md
# MAGIC ##### **Reading from the source**

# COMMAND ----------

# DLT table
@dlt.table
#DLT Exceptations
@dlt.expect_all_or_drop(my_rules)
# Decorator
def SCD2_gold():
    df = spark.read.table("ecommerce_catalog.silver.product")
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ##### **Creating a streaming view**

# COMMAND ----------

@dlt.view 

def SCD2_gold_view():

    df = spark.readStream.table("LIVE.SCD2_gold")
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ##### **Creating an empty straming table**

# COMMAND ----------

dlt.create_streaming_table("gold_products")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### **Using apply changes API to use SCD Type 2**

# COMMAND ----------

dlt.apply_changes(
    target = "ecommerce_catalog.gold.gold_products",
    keys = ["product_id"],
    sequence_by = "product_id",
    stored_as_scd_type = 2
)



# COMMAND ----------

