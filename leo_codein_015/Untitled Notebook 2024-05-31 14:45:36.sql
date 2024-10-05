-- Databricks notebook source
SELECT custom_tags.key, sku_name, usage_unit, SUM(usage_quantity) as `DBUs consumed`
FROM system.billing.usage
WHERE custom_tags.key = ''
GROUP BY 1, 2

-- COMMAND ----------

ALTER TABLE `quickstart_catalog`.`quickstart_schema`.`steelgold` SET TBLPROPERTIES (delta.enableChangeDataFeed = true)

-- COMMAND ----------

ALTER TABLE `quickstart_catalog`.`quickstart_schema`.`quickstart_table` SET TBLPROPERTIES (delta.enableChangeDataFeed = true)
