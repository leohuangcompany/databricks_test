-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC # Databricks System Tables - Billing logs
-- MAGIC
-- MAGIC Databricks collects and update your billing logs using the `system.billing.usage` table.
-- MAGIC
-- MAGIC This table contains all your consumption usage and lets you track your spend across all your workspaces.
-- MAGIC
-- MAGIC This main table contains the following information: 
-- MAGIC
-- MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/uc/system_tables/dashboard-governance-billing.png?raw=true" width="450px" style="float: right">
-- MAGIC
-- MAGIC - `account_id`: ID of the Databricks account or Azure Subscription ID
-- MAGIC - `workspace_id`: ID of the workspace this usage was associated with
-- MAGIC - `record_id`: unique id for the record
-- MAGIC - `sku_name`: name of the sku
-- MAGIC - `cloud`: cloud this usage is associated to 
-- MAGIC - `usage_start_time`: start time of usage record
-- MAGIC - `usage_end_time`: end time of usage record 
-- MAGIC - `usage_date`: date of usage record
-- MAGIC - `custom_tags`: tag metadata associated to the usage 
-- MAGIC - `usage_unit`: unit this usage measures (i.e. DBUs)
-- MAGIC - `usage_quantity`: number of units consumed
-- MAGIC - `usage_metadata`: other relevant information about the usage  
-- MAGIC
-- MAGIC <!-- Collect usage data (view). Remove it to disable collection or disable tracker during installation. View README for more details.  -->
-- MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=governance&org_id=4472318973269700&notebook=%2F01-billing-tables%2F01-billing-tables-overview&demo_name=uc-04-system-tables&event=VIEW&path=%2F_dbdemos%2Fgovernance%2Fuc-04-system-tables%2F01-billing-tables%2F01-billing-tables-overview&version=1">

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ### A cluster has been created for this demo
-- MAGIC To run this demo, just select the cluster `dbdemos-uc-04-system-tables-leo_huang` from the dropdown menu ([open cluster configuration](https://adb-4472318973269700.0.azuredatabricks.net/#setting/clusters/0424-175106-anby6n4a/configuration)). <br />
-- MAGIC *Note: If the cluster was deleted after 30 days, you can re-create it with `dbdemos.create_cluster('uc-04-system-tables')` or re-install the demo: `dbdemos.install('uc-04-system-tables')`*

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC *Looking for the notebook used to create the [forecasting dashboard](/sql/dashboards/1e7a9180-8487-48d9-b070-eae013d13e17)? Jump to the [02-forecast-billing-tables]($./02-forecast-billing-tables) notebook.*

-- COMMAND ----------

-- DBTITLE 1,Init setup
-- MAGIC %run ../_resources/00-setup $reset_all_data=false

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ## A note on pricing tables
-- MAGIC Note that Pricing tables (containing the price information in `$` for each SKU) is available as a system table.
-- MAGIC
-- MAGIC **Please consider these numbers as estimates which do not include any add-ons or discounts. It is using list price, not contractual. Please review your contract for more accurate information.**

-- COMMAND ----------

-- DBTITLE 1,Review our billing table
select * from system.billing.usage limit 50

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Billing query examples 
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Jobs Usage 
-- MAGIC
-- MAGIC Jobs are scheduled code and have extremely predictable usage over time. Since jobs are automated it is important to monitor which jobs are put into production to avoid unnecessary spend. Let's take a look at job spend over time. 

-- COMMAND ----------

select
  u.account_id,
  u.workspace_id,
  u.sku_name,
  u.cloud,
  u.usage_start_time,
  u.usage_end_time,
  u.usage_date,
  date_format(u.usage_date, 'yyyy-MM') as YearMonth,
  u.usage_unit,
  u.usage_quantity,
  lp.pricing.default as list_price,
  lp.pricing.default * u.usage_quantity as list_cost,
  u.usage_metadata.*
from
  system.billing.usage u 
  inner join system.billing.list_prices lp on u.cloud = lp.cloud and
    u.sku_name = lp.sku_name and
    u.usage_start_time >= lp.price_start_time and
    (u.usage_end_time <= lp.price_end_time or lp.price_end_time is null)

where
  usage_metadata.job_id is not Null

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ### Interactive Jobs 
-- MAGIC
-- MAGIC Interactive (All Purpose) compute are clusters meant to be used during the development process. Once a solution is developed it is considered a best practice to move them to job clusters. We will want to keep an eye on how many jobs are created on all purpose and alert the users when that happens to make the change. 

-- COMMAND ----------

-- DBTITLE 0,Interactive Jobs
with created_jobs as (
  select
    workspace_id,
    event_time as created_time,
    user_identity.email as creator,
    request_id,
    event_id,
    get_json_object(response.result, '$.job_id') as job_id,
    request_params.name as job_name,
    request_params.job_type,
    request_params.schedule
  from
    system.access.audit
  where
    action_name = 'create'
    and service_name = 'jobs'
    and response.status_code = 200
),
deleted_jobs as (
  select
    request_params.job_id,
    workspace_id
  from
    system.access.audit
  where
    action_name = 'delete'
    and service_name = 'jobs'
    and response.status_code = 200
)
select
  a.workspace_id,
  a.sku_name,
  a.cloud,
  a.usage_date,
  date_format(usage_date, 'yyyy-MM') as YearMonth,
  a.usage_unit,
  d.pricing.default as list_price,
  sum(a.usage_quantity) total_dbus,
  sum(a.usage_quantity) * d.pricing.default as list_cost,
  a.usage_metadata.*,
  case
    when b.job_id is not null then TRUE
    else FALSE
  end as job_created_flag,
  case
    when c.job_id is not null then TRUE
    else FALSE
  end as job_deleted_flag
from
  system.billing.usage a
  left join created_jobs b on a.workspace_id = b.workspace_id
  and a.usage_metadata.job_id = b.job_id
  left join deleted_jobs c on a.workspace_id = c.workspace_id
  and a.usage_metadata.job_id = c.job_id
  left join system.billing.list_prices d on a.cloud = d.cloud and
    a.sku_name = d.sku_name and
    a.usage_start_time >= d.price_start_time and
    (a.usage_end_time <= d.price_end_time or d.price_end_time is null)
where
  usage_metadata.job_id is not Null
  and contains(a.sku_name, 'ALL_PURPOSE')
group by
  all

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Model Inference Usage 
-- MAGIC
-- MAGIC Databricks has the ability to host and deploy serverless model endpoints for highly available and cost effective REST APIs. Endpoints can scale all the way down to zero and quickly come up to provide a response to the end user optimizing experience and spend. Let's keep and eye on how many models we have deployed the the usage of those models. 

-- COMMAND ----------

select
  u.account_id,
  u.workspace_id,
  u.sku_name,
  u.cloud,
  u.usage_start_time,
  u.usage_end_time,
  u.usage_date,
  date_format(u.usage_date, 'yyyy-MM') as YearMonth,
  u.usage_unit,
  u.usage_quantity,
  lp.pricing.default as list_price,
  lp.pricing.default * u.usage_quantity as list_cost,
  u.custom_tags.Team, -- parse out custom tags if available
  u.usage_metadata.*
from
  system.billing.usage u 
  inner join system.billing.list_prices lp on u.cloud = lp.cloud and
    u.sku_name = lp.sku_name and
    u.usage_start_time >= lp.price_start_time 
where
  contains(u.sku_name, 'INFERENCE')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Next: leverage Databricks Lakehouse AI capabilities to forecast your billing
-- MAGIC
-- MAGIC Let's create a new table to extend our billing dataset with forecasting and alerting capabilities.
-- MAGIC
-- MAGIC We'll train a custom model for each Workspace and SKU, predicting the consumption for the next quarter.
-- MAGIC
-- MAGIC Open the [02-forecast-billing-tables notebook]($./02-forecast-billing-tables) to train your model.
