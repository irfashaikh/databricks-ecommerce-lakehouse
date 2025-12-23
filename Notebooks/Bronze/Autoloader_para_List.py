# Databricks notebook source
# use this array as a input for out Autoloader Notebook.

datasets = [
    {
        "filename" : "orders"
    },
    {
        "filename" : "customers"
    },
    {
        "filename" : "products"
    }
]

# COMMAND ----------

dbutils.jobs.taskValues.set("Output_dataset", datasets)

# With the help this utility we can return this dataset to the Autoloader notebook as a input at job level.

# after that create a job :-
    # 1. first notebook will be the parameter notebook which serves as a input for the Autoloader Notebook.
    # 2. second Autoloader notebook which will be triggered by the parameter notebook in a loop. so that all the Array of tables get loaded in the bronze layer. 

# COMMAND ----------

