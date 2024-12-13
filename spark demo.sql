-- Databricks notebook source
-- MAGIC %md
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Basic table creation using sql & pyspark

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC spark.conf.set("fs.azure.account.auth.type.storageansh.dfs.core.windows.net", "OAuth")
-- MAGIC spark.conf.set("fs.azure.account.oauth.provider.type.storageansh.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
-- MAGIC spark.conf.set("fs.azure.account.oauth2.client.id.storageansh.dfs.core.windows.net", "73b19484-4367-47ae-9797-29c5c0eddafc")
-- MAGIC spark.conf.set("fs.azure.account.oauth2.client.secret.storageansh.dfs.core.windows.net", "GT98Q~8OthIGccstIBhrF1IXU3Tz1df6HJgXGbVQ")
-- MAGIC spark.conf.set("fs.azure.account.oauth2.client.endpoint.storageansh.dfs.core.windows.net", "https://login.microsoftonline.com/55cca473-f9ef-4ccb-ba85-dec3acbc0a2c/oauth2/token")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.read\
-- MAGIC     .format('csv')\
-- MAGIC     .option('header', 'true')\
-- MAGIC     .load('abfss://landing@storageansh.dfs.core.windows.net/spark_performance/Spotify_Listening_Activity.csv')

-- COMMAND ----------

drop table spotify.activity1


-- COMMAND ----------

drop schema spotify

-- COMMAND ----------

create schema if not exists spotify 


-- COMMAND ----------

 create table if not exists spotify.activity1 (
    activity_id string,
    song_id string,
    listen_date timestamp,
    listen_duration int
  )
using delta


-- COMMAND ----------

-- MAGIC %python
-- MAGIC path='abfss://landing@storageansh.dfs.core.windows.net/spark_performance/Spotify_Listening_Activity.csv'

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df_read = spark.read\
-- MAGIC     .format('csv')\
-- MAGIC     .option('header', 'true')\
-- MAGIC     .schema('activity_id string, song_id string, listen_date timestamp, listen_duration int')\
-- MAGIC     .load('abfss://landing@storageansh.dfs.core.windows.net/spark_performance/Spotify_Listening_Activity.csv')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df_read.write\
-- MAGIC     .mode('append')\
-- MAGIC     .saveAsTable('spotify.activity1')

-- COMMAND ----------

select * from spotify.activity1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Rename the table

-- COMMAND ----------

alter table spotify.activity1 rename to spotify.activity

-- COMMAND ----------

describe spotify.activity

-- COMMAND ----------

describe history spotify.activity

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### No. of partitions

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df0 = spark.table("spotify.activity")
-- MAGIC
-- MAGIC # Get the number of partitions
-- MAGIC num_partitions = df0.rdd.getNumPartitions()
-- MAGIC print(f"Number of partitions: {num_partitions}")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Streaming

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df_read_st = spark.readStream\
-- MAGIC     .format('csv')\
-- MAGIC     .option('header', 'true')\
-- MAGIC     .schema('activity_id string, song_id string, listen_date timestamp, listen_duration int')\
-- MAGIC     .load('abfss://landing@storageansh.dfs.core.windows.net/spark_performance/')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(df_read_st)

-- COMMAND ----------

truncate table spotify.activity

-- COMMAND ----------

select count(*) from spotify.activity

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df_read_st.writeStream\
-- MAGIC     .format('delta')\
-- MAGIC     .outputMode('append')\
-- MAGIC     .option('checkpointLocation', '/tmp/checkpoint')\
-- MAGIC     .toTable('spotify.activity')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.head('dbfs:/tmp/checkpoint/commits/0')
-- MAGIC #spark.read.csv('dbfs:/tmp/checkpoint/commits/0', header=True, inferSchema=True).show()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### After adding a new file

-- COMMAND ----------

select count(*) from spotify.activity

-- COMMAND ----------

select * from spotify.activity
where cast(activity_id as int)>11779

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Autoloder

-- COMMAND ----------

truncate table spotify.activity

-- COMMAND ----------

select * from spotify.activity

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df_read_st_auto = spark.readStream\
-- MAGIC     .format('cloudFiles')\
-- MAGIC     .option('cloudFiles.format', 'csv')\
-- MAGIC      .option('cloudFiles.schemaEvaluation', 'addNewColumns')\
-- MAGIC     .option("cloudFiles.validateOptions", "false")\
-- MAGIC     .option('cloudFiles.schemaLocation', '/tmp/schema')\
-- MAGIC     .option("mergeSchema", "true")\
-- MAGIC     .option('header', 'true')\
-- MAGIC     .schema('activity_id string, song_id string, listen_date timestamp, listen_duration int, Class string')\
-- MAGIC     .load('abfss://landing@storageansh.dfs.core.windows.net/spark_performance/')
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(df_read_st_auto)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df_read_st_auto.writeStream\
-- MAGIC     .format('delta')\
-- MAGIC     .outputMode('append')\
-- MAGIC     .option('checkpointLocation', '/tmp/checkpoint8')\
-- MAGIC     .option("mergeSchema", "true")\
-- MAGIC     .toTable('spotify.activity')

-- COMMAND ----------

select * from spotify.activity


-- COMMAND ----------

select * from spotify.activity
where cast(activity_id as int)>11779

-- COMMAND ----------

-- MAGIC %python
-- MAGIC path_base = 'abfss://landing@storageansh.dfs.core.windows.net'

-- COMMAND ----------

-- MAGIC %python
-- MAGIC read_update = spark.read\
-- MAGIC     .format('csv')\
-- MAGIC     .option('header', 'true')\
-- MAGIC     .schema('activity_id string, song_id string, listen_date timestamp, listen_duration int, Class string, valid int')\
-- MAGIC     .option('schemaMerge', 'true')\
-- MAGIC     .load(f'{path_base}/update/')
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(read_update)

-- COMMAND ----------

truncate table spotify.updated_activity

-- COMMAND ----------

-- MAGIC %python
-- MAGIC read_update.write\
-- MAGIC     .format('delta')\
-- MAGIC     .mode('overwrite')\
-- MAGIC     .saveAsTable('spotify.updated_activity')

-- COMMAND ----------

select * from spotify.updated_activity

-- COMMAND ----------

merge into spotify.activity as t
using spotify.updated_activity as s 
on t.activity_id = s.activity_id 
when matched 
then update set *
when not matched
then insert *

-- COMMAND ----------

select * from spotify.activity
where cast(activity_id as int)>11779

-- COMMAND ----------

describe history spotify.activity

-- COMMAND ----------

restore table spotify.activity to version as of 20

-- COMMAND ----------

select * from spotify.activity
where cast(activity_id as int)>11779

-- COMMAND ----------

merge into spotify.activity as t
using spotify.updated_activity as s 
on t.activity_id = s.activity_id 
when matched 
then delete and insert
when not matched
then insert *

-- COMMAND ----------

select * from spotify.activity
where cast(activity_id as int)>11779

-- COMMAND ----------

describe history spotify.activity

-- COMMAND ----------

restore table spotify.activity to version as of 5

-- COMMAND ----------

select * from spotify.activity
where cast(activity_id as int)>11786

-- COMMAND ----------

merge WITH SCHEMA EVOLUTION into spotify.activity as t
using updated_activity as s 
on t.activity_id = s.activity_id 
when matched 
then delete 
when not matched
then insert *

-- COMMAND ----------

select * from spotify.activity
where cast(activity_id as int)>11779

-- COMMAND ----------

describe spotify.activity

-- COMMAND ----------

ALTER TABLE spotify.activity ADD COLUMNS (validTo TIMESTAMP);

-- COMMAND ----------

merge WITH SCHEMA EVOLUTION into spotify.activity as t
using spotify.updated_activity as s 
on t.activity_id = s.activity_id 
when matched 
then update set 
  t.activity_id=s.activity_id,
  t.song_id=s.song_id,
  t.listen_date=s.listen_date,
  t.listen_duration=s.listen_duration,
  t.Class=s.Class,
  t.validTo=current_timestamp()
when not matched
then insert 
  (activity_id,
  song_id,
  listen_date,
  listen_duration,
  Class,
  validTo)
values
  ( s.activity_id,
  s.song_id,
  s.listen_date,
  s.listen_duration,
  s.Class,
  current_timestamp())




-- COMMAND ----------

select * from spotify.activity
where cast(activity_id as int)>11779

-- COMMAND ----------

