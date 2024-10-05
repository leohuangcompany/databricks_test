# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC This notebook shows you how to create and query a table or DataFrame loaded from data stored in Azure Blob storage.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Step 1: Set the data location and type
# MAGIC
# MAGIC There are two ways to access Azure Blob storage: account keys and shared access signatures (SAS).
# MAGIC
# MAGIC To get started, we need to set the location and type of the file.

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DROP TABLE IF EXISTS cht_catalog.cht_schema.cht;

# COMMAND ----------

storage_account_name = "account_name"
storage_account_access_key = "account_access_key"

# COMMAND ----------

blob_container_name = "cht"

#https://xxx.blob.core.windows.net/cht/

file_location = f"wasbs://{blob_container_name}@{storage_account_name}.blob.core.windows.net/"
#file_location = "abfss://store@xxx.dfs.core.windows.net"
file_type = "csv"

# COMMAND ----------

spark.conf.set(
  "fs.azure.account.key."+storage_account_name+".blob.core.windows.net",
  storage_account_access_key)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Step 2: Read the data
# MAGIC
# MAGIC Now that we have specified our file metadata, we can create a DataFrame. Notice that we use an *option* to specify that we want to infer the schema from the file. We can also explicitly set this to a particular schema if we have one already.
# MAGIC
# MAGIC First, let's create a DataFrame in Python.

# COMMAND ----------

from pyspark.sql.functions import to_date
from pyspark.sql.types import *

# 讀取 CSV 文件
df = spark.read.format(file_type).option("inferSchema", "true").option("header", "true").load(file_location)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Step 3: Query the data
# MAGIC
# MAGIC Now that we have created our DataFrame, we can query it. For instance, you can identify particular columns to select and display.

# COMMAND ----------

spark.conf.set('spark.sql.legacy.timeParserPolicy', 'LEGACY')
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Step 4: (Optional) Create a view or table
# MAGIC
# MAGIC If you want to query this data as a table, you can simply register it as a *view* or a table.

# COMMAND ----------

df.createOrReplaceTempView("cht")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC We can query this view using Spark SQL. For instance, we can perform a simple aggregation. Notice how we can use `%sql` to query the view from SQL.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Since this table is registered as a temp view, it will be available only to this notebook. If you'd like other users to be able to query this table, you can also create a table from the DataFrame.

# COMMAND ----------

# Write data to Unity catalog
# Replace invalid characters in column names
df_cleaned = df
for column in df_cleaned.columns:
    new_column_name = column.replace(" ", "_").replace(",", "").replace(";", "").replace("{", "").replace("}", "").replace("(", "").replace(")", "").replace("\n", "").replace("\t", "").replace("=", "")
    #.replace(".", "")
    df_cleaned = df_cleaned.withColumnRenamed(column, new_column_name)

spark.sql("DROP TABLE IF EXISTS cht_catalog.cht_schema.cht;")

# Save the cleaned DataFrame to Delta
# df.write.format("delta").mode("append").saveAsTable("demo_jk_data.walsin_schema.steel")

# df = df_cleaned.withColumn("`SFC產出時間`", to_date(df["date_string"], "yyyy-MM-dd"))
df_cleaned.write.format("delta").mode("append").saveAsTable("cht_catalog.cht_schema.cht")

# COMMAND ----------

# Write data to Unity catalog
# df.write.format("delta").mode("append").saveAsTable("walsin_catalog.walsin_schema.test")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC This table will persist across cluster restarts and allow various users across different notebooks to query this data.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM cht_catalog.cht_schema.cht

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM cht_catalog.cht_schema.cht
