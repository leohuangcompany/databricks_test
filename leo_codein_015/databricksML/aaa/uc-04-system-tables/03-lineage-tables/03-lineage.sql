-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Lineage with Databricks Unity Catalog System Tables
-- MAGIC
-- MAGIC Databricks tracks lineage across all your Unity Catalog items.
-- MAGIC
-- MAGIC Databricks Lineage is available from the [Data Explorer UI](/explore/data), where you can analyze your graph.
-- MAGIC
-- MAGIC This contains information on downstream (where the data is coming from) and upstream (who is using it) from all items:
-- MAGIC
-- MAGIC - Tables
-- MAGIC - Queries
-- MAGIC - Dashboards
-- MAGIC - Jobs
-- MAGIC - ML/AI
-- MAGIC - ...
-- MAGIC
-- MAGIC For more details, open this recording to <a href="https://app.getreprise.com/launch/MnqjQDX" target="_blank">discover Unity Catalog from the UI</a>
-- MAGIC
-- MAGIC ## Table and Column lineage
-- MAGIC
-- MAGIC Unity Catalog track the lineage at 2 levels:
-- MAGIC
-- MAGIC * Column level
-- MAGIC * Table level
-- MAGIC
-- MAGIC
-- MAGIC ## Query example 
-- MAGIC
-- MAGIC The following queries are some example that you can run to explore the lineage.
-- MAGIC
-- MAGIC Make sure you have read access to the system catalog to be able to run the following queries (by default available to admin metastore).
-- MAGIC
-- MAGIC <!-- Collect usage data (view). Remove it to disable collection or disable tracker during installation. View README for more details.  -->
-- MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=governance&org_id=4472318973269700&notebook=%2F03-lineage-tables%2F03-lineage&demo_name=uc-04-system-tables&event=VIEW&path=%2F_dbdemos%2Fgovernance%2Fuc-04-system-tables%2F03-lineage-tables%2F03-lineage&version=1">

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ### A cluster has been created for this demo
-- MAGIC To run this demo, just select the cluster `dbdemos-uc-04-system-tables-leo_huang` from the dropdown menu ([open cluster configuration](https://adb-4472318973269700.0.azuredatabricks.net/#setting/clusters/0424-175106-anby6n4a/configuration)). <br />
-- MAGIC *Note: If the cluster was deleted after 30 days, you can re-create it with `dbdemos.create_cluster('uc-04-system-tables')` or re-install the demo: `dbdemos.install('uc-04-system-tables')`*

-- COMMAND ----------

SHOW TABLES IN system.access

-- COMMAND ----------

SELECT * FROM system.access.table_lineage

-- COMMAND ----------

-- DBTITLE 1,Review all entities accessing your table (workflows, notebook, DLT, DBSQL...)
SELECT DISTINCT(entity_type) FROM system.access.table_lineage

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Column-level lineage information
-- MAGIC
-- MAGIC Unity Catalog also tracks all informations at a column level.
-- MAGIC
-- MAGIC This is useful to track downstream dependencies and evaluate potential data change impact, including GDPR implication.

-- COMMAND ----------

SELECT * FROM system.access.column_lineage
