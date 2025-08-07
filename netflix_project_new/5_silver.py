# Databricks notebook source
##parameter
dbutils.widgets.text("weekdays", "7")

# COMMAND ----------

#variable
var = int(dbutils.widgets.get("weekdays"))

# COMMAND ----------

dbutils.jobs.taskValues.set(key="weekoutput", value=var)

# COMMAND ----------

