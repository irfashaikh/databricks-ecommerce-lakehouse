# Databricks notebook source
# MAGIC %md
# MAGIC #### **Bronze Layer Workflow**
# MAGIC
# MAGIC ✔ Creating a Bronze layer
# MAGIC ✔ Adding metadata (ingest_timestamp, ingest_date)
# MAGIC ✔ Writing data in Delta format
# MAGIC ✔ Storing raw data in a structured bronze location

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, TimestampType , NumericType , DateType

# Defining Schema for the Dataframe.
schema = StructType([
    StructField("order_id", NumericType(), True),
    StructField("order_date", DateType(), True),
    StructField("customer_id", NumericType(), True),
    StructField("customer_name", StringType(), True),
    StructField("country", StringType(), True),
    StructField("product_id", NumericType(), True),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("quantity", NumericType(), True),
    StructField("unit_price", NumericType(), True),
    StructField("total_amount", NumericType(), True),
    StructField("payment_method", StringType(), True),
    StructField("order_status", StringType(), True),
    StructField("delivery_days", DateType(), True),
    StructField("delivery_date", DateType(), True)
])


# COMMAND ----------

Source_path = '/Volumes/ecommerce_catalog/source/ecommerce_source_volume'
File_Name = 'ecommerce_data.csv'

df = spark.read.format('CSV')\
    .option('header' , True)\
    .option('schema' , schema)\
    .load(f"{Source_path}/{File_Name}")

# Reading the data from the source and writing it into the bronze layer as a delta table.


# COMMAND ----------

df.write.format("delta").mode("Append").saveAsTable("ecommerce_catalog.bronze.bronze_ecommerce")


# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from ecommerce_catalog.bronze.bronze_ecommerce limit 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Adding Metadata to help with tracking , auditing and debugging**

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, input_file_name , to_date

Bronze_df = df.withColumn('ingest_Timestamp', current_timestamp())\
    .withColumn("ingest_date", to_date("ingest_timestamp"))

# COMMAND ----------

Bronze_df.limit(10).display()

# COMMAND ----------

Bronze_df.write.format("delta").mode("overwrite").option("mergeSchema" , True).saveAsTable("ecommerce_catalog.bronze.bronze_ecommerce")

# we are overwriting the data in the bronze layer. if you encounter a error called schema issue than use mergeSchema option.