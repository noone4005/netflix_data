# Databricks notebook source
# MAGIC %md
# MAGIC # silver Data Transformation

# COMMAND ----------

df = spark.read.format("delta")\
               .options(header="true")\
               .options (inferSchema="true")\
               .load("abfss://bronze@rgnetflixstorage.dfs.core.windows.net/netflix_titles")
display(df)

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import col

# COMMAND ----------

df = df.fillna({"duration_minutes" : 0, "duration_seasons" : 1})

# COMMAND ----------

df.display()

# COMMAND ----------

from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType

df = df.withColumn("duration_minutes", col("duration_minutes").cast(IntegerType()))
df = df.withColumn("duration_seasons", col("duration_seasons").cast(IntegerType()))


# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md 
# MAGIC # slipt column 

# COMMAND ----------

df = df.withColumn("shorttitle", split(col("title"), ':')[0])

# COMMAND ----------

df.display()

# COMMAND ----------

df = df.withColumn("rating", split(col("rating"), '-').getItem(0))
df.display()

# COMMAND ----------

df = df.withColumn("type_flag", when(col('type')=='Movie',1)\
                               .when(col('type')=='TV Show',2)\
                               .otherwise(0))
df.display()

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import col, dense_rank

# COMMAND ----------

df = df.withColumn("duration_ranking", dense_rank().over(Window.orderBy(col("duration_minutes").desc())))
df.display()

# COMMAND ----------

df.createOrReplaceTempView("temp_view")

# COMMAND ----------

df = spark .sql("""
            
            select * from temp_view
                
                
                """)

df.display()

# COMMAND ----------

df.createOrReplaceGlobalTempView("global_temp_view")
df = spark .sql("""
                    
                    select * from global_temp.global_temp_view
                    """)
df.display()

# COMMAND ----------

df_vis = df.groupBy("type").agg(count("*").alias("total_count"))
df_vis.display()    

# COMMAND ----------

df_vis = df_vis.write.format("delta")\
               .mode("overwrite")\
               .option("path", "abfss://silver@rgnetflixstorage.dfs.core.windows.net/netflix_titles")\
               .save()