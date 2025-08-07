# Databricks notebook source
# MAGIC %md
# MAGIC # incrementaloding

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a catalog (if it doesn't exist)
# MAGIC CREATE CATALOG netflix_catalog;
# MAGIC
# MAGIC -- Create a schema inside that catalog
# MAGIC CREATE SCHEMA netflix_catalog.net_schema;
# MAGIC

# COMMAND ----------

checkpoint_location = "abfss://silver@rgnetflixstorage.dfs.core.windows.net/checkpoint"

# COMMAND ----------

df = spark.readStream\
          .format("cloudFiles")\
          .option("cloudFiles.format", "csv")\
          .option("cloudFiles.schemaLocation", checkpoint_location)\
          .load("abfss://raw@rgnetflixstorage.dfs.core.windows.net")

# COMMAND ----------

display(df)

# COMMAND ----------

df.writeStream\
  .option("checkpointLocation", checkpoint_location)\
  .trigger(processingTime="10 seconds")\
  .start("abfss://bronze@rgnetflixstorage.dfs.core.windows.net/netflix_titles")

# COMMAND ----------

df = spark.read.format("csv")\
          .option("header", True)\
          .option("inferSchema", True)\
          .load("abfss://bronze@rgnetflixstorage.dfs.core.windows.net/netflix_directors")
display(df) 

# COMMAND ----------

df.write.format("delta")\
        .mode("append")\
          .option("path", "abfss://silver@rgnetflixstorage.dfs.core.windows.net/netflix_directors")\
          .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ## parameters 
# MAGIC

# COMMAND ----------

dbutils.widgets.text("source_folder", "netflix_directors")
dbutils.widgets.text("target_folder", "netflix_directors")

# COMMAND ----------

# MAGIC %md
# MAGIC ### variables

# COMMAND ----------

var_src_folder = dbutils.widgets.get("source_folder")   
var_trg_folder = dbutils.widgets.get("target_folder")   

# COMMAND ----------

print(var_src_folder)
print(var_trg_folder)

# COMMAND ----------

df = spark.read.format("csv")\
          .option("header", True)\
          .option("inferSchema", True)\
          .load(f"abfss://bronze@rgnetflixstorage.dfs.core.windows.net/{var_src_folder}")
display(df) 

# COMMAND ----------

df.write.format("delta")\
        .mode("append")\
          .option("path", f"abfss://silver@rgnetflixstorage.dfs.core.windows.net/{var_trg_folder}")\
          .save()

# COMMAND ----------

