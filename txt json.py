# Databricks notebook source



# COMMAND ----------

df = spark.read.format('csv').options(header='True', delimiter=";", escape=',').load('abfss://landing@storageansh.dfs.core.windows.net/pokemons/pokemon_josn.csv')

# COMMAND ----------

df.show(truncate=False)

# COMMAND ----------

df = spark.read.text('abfss://landing@storageansh.dfs.core.windows.net/pokemons/pokemon_josn.csv')

# COMMAND ----------

import pyspark.sql.types as T
 
#schema to represent out json data
schema = T.StructType(
  [
    T.StructField('name', T.StringType(), True),
    T.StructField('type', T.StringType(), True),
    T.StructField('hp', T.IntegerType(), True),
    T.StructField('attact', T.IntegerType(), True)
  ]
)

# COMMAND ----------

df.show(truncate=False)

# COMMAND ----------


# COMMAND ----------

df.columns

# COMMAND ----------

s = F.split(df['id,json_data'], ',')
df.withColumn("id", s.getItem(0))\
    .withColumn("id", s.getItem(1))\
    .withColumn("id", s.getItem(2)).show()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

import pyspark.sql.functions as F
df.withColumn("json_data", F.from_json("json_data", schema=schema)).show()

# COMMAND ----------

result_df = df_parsed.select(
    "id",
    F.col("parsed_json.name").alias("name"),
    F.col("parsed_json.type").alias("type"),
    F.col("parsed_json.hp").alias("hp"),
    F.col("parsed_json.attack").alias("attack")
)

# Show the result
result_df.show()

# COMMAND ----------

df2 = spark.read.format('csv').load('abfss://landing@storageansh.dfs.core.windows.net/pokemons/pokemon_josn2.csv')

# COMMAND ----------

df2.show(truncate=False)

# COMMAND ----------

from pyspark.sql import functions as F
df2.select(F.regexp_replace("_c1", "\{\"name\":", "")).show()

# COMMAND ----------


df = spark.read.text('abfss://landing@storageansh.dfs.core.windows.net/pokemons/pokemon_josn2.csv')


# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

df = df.filter("value != 'FullName, FullLabel, Type'") \
    .withColumn(
    "value",
    F.split(F.col("value"), ',(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)')
).select(
    F.col("value")[0].alias("FullName"),
    F.col("value")[1].alias("FullLabel"),
    F.col("value")[2].alias("Type")
)

# COMMAND ----------

df.withColumn("FullLabel2", F.regexp_replace(F.col("FullLabel"),"name","")).show()

# COMMAND ----------

