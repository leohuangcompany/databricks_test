# Databricks notebook source
# MAGIC %pip install dbdemos

# COMMAND ----------

import dbdemos
dbdemos.install('lakehouse-retail-c360', catalog='main', schema='dbdemos_retail_c360')

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
