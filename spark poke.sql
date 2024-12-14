-- Databricks notebook source
-- MAGIC %python
-- MAGIC


-- COMMAND ----------

-- MAGIC %md
-- MAGIC _1. Load the dataset into a Spark SQL table named pokemon_data._

-- COMMAND ----------

create schema poke

-- COMMAND ----------

CREATE TABLE poke.pokemon_data (
    pokemon_id STRING,
    pokemon_name STRING,
    type1 STRING,
    type2 STRING,
    total STRING,
    hp STRING,
    attack STRING,
    defense STRING,
    speed STRING,
    region STRING
)
USING delta



-- COMMAND ----------

-- MAGIC %python
-- MAGIC schema = 'pokemon_id STRING,\
-- MAGIC     pokemon_name STRING,\
-- MAGIC     type1 STRING,\
-- MAGIC     type2 STRING,\
-- MAGIC     total STRING,\
-- MAGIC     hp STRING,\
-- MAGIC     attack STRING,\
-- MAGIC     defense STRING,\
-- MAGIC     speed STRING,\
-- MAGIC     region STRING'

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.read.csv('abfss://landing@storageansh.dfs.core.windows.net/pokemons/pokes.csv', header=True, schema=schema)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df.write.format("delta").mode("overwrite").saveAsTable("poke.pokemon_data")

-- COMMAND ----------

select * from poke.pokemon_data

-- COMMAND ----------

-- MAGIC %md
-- MAGIC _2. Alter the column names:_
-- MAGIC - Rename pokemon_id to id.
-- MAGIC - Rename pokemon_name to name.

-- COMMAND ----------

alter table poke.pokemon_data
rename column pokemon_id to id

-- COMMAND ----------

ALTER TABLE poke.pokemon_data SET TBLPROPERTIES (
    'delta.columnMapping.mode' = 'name'
  )

-- COMMAND ----------

alter table poke.pokemon_data
rename column pokemon_id to id

-- COMMAND ----------

alter table poke.pokemon_data
rename column pokemon_name to name

-- COMMAND ----------

select * from poke.pokemon_data LIMIT 5

-- COMMAND ----------

-- MAGIC %md
-- MAGIC _Change the data types:_
-- MAGIC - Convert id from string to INT.
-- MAGIC - Convert total, hp, attack, defense, and speed from string to INT.
-- MAGIC - Ensure type1 and type2 are of type STRING.

-- COMMAND ----------

ALTER table poke.pokemon_data
alter column id int

-- COMMAND ----------

select * from poke.pokemon_data1 LIMIT 5

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.types import IntegerType
-- MAGIC df_poke = spark.read.table("poke.pokemon_data")
-- MAGIC df_poke = df_poke.withColumn('id', F.col("id").cast((IntegerType())))
-- MAGIC df_poke.write.format("delta").option('mergeSchema', 'true').mode("overwrite").saveAsTable("poke.pokemon_data")

-- COMMAND ----------

describe poke.pokemon_data

-- COMMAND ----------

create table poke.pokemon_data_A
select
cast(id as int) as id,
name,
type1,
type2,
cast(total as int) as total,
cast(hp as int) as hp,
cast(attack as int) as attack,
cast(defense as int) as defense,
cast(speed as int) as speed,
region
from poke.pokemon_data

-- COMMAND ----------

select * from poke.pokemon_data_A LIMIT 5

-- COMMAND ----------

drop table if exists poke.pokemon_data;
drop table if exists poke.pokemon_data1;
drop table if exists poke.pokemon_data2;
drop table if exists poke.pokemon_data5;

-- COMMAND ----------

alter table poke.pokemon_data_a rename to poke.pokemon_data;

-- COMMAND ----------

describe poke.pokemon_data

-- COMMAND ----------

-- MAGIC %md
-- MAGIC _Use CTEs for Transformation_
-- MAGIC - Create a CTE to calculate the average total for each Pokémon type1.
-- MAGIC - Use the CTE to return all Pokémon where the total is greater than the average for their type.

-- COMMAND ----------

with c1 as (
  select type1, round(avg(total),1) as avg_total
  from poke.pokemon_data
  group by type1
)
select pd.*, c1.*
from poke.pokemon_data pd left join c1
on pd.type1=c1.type1
where pd.total > c1.avg_total

-- COMMAND ----------

-- MAGIC %md
-- MAGIC _Find Minimum and Maximum Stats Using PARTITION BY_
-- MAGIC - Find the Pokémon with the highest attack within each type1.
-- MAGIC - Find the Pokémon with the lowest defense within each type1.
-- MAGIC - Include columns: id, name, type1, attack, and defense.
-- MAGIC - Sort the result in descending order by attack.

-- COMMAND ----------

select *, 
  row_number() over(partition by type1 order by attack desc) max_attack,
  row_number() over(partition by type1 order by defense) min_defense
  from poke.pokemon_data
  order by type1, attack desc
  

-- COMMAND ----------

with c1 as (
  select *, 
  row_number() over(partition by type1 order by attack desc) max_attack,
  row_number() over(partition by type1 order by defense) min_defense
  from poke.pokemon_data
  order by type1, attack desc
)
select * from c1
where max_attack=1 or min_defense=1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC _Add Derived Columns with Spark SQL_
-- MAGIC - Add a new column speed_rating:
-- MAGIC - Use a CASE statement to categorize speed as:
-- MAGIC - Fast if speed >= 80.
-- MAGIC - Average if speed >= 50 AND speed < 80.
-- MAGIC - Slow otherwise.
-- MAGIC - Save the transformed data as a new table pokemon_transformed.

-- COMMAND ----------

create table poke.pokemon_transformed
select *, case when speed >= 80 then 'fast'
when speed >= 50 then 'average'
else 'slow' end as speed_rating
from poke.pokemon_data

-- COMMAND ----------

select * from poke.pokemon_transformed

-- COMMAND ----------

-- MAGIC %md
-- MAGIC _Combine CTEs with Window Functions_
-- MAGIC - Calculate the rank of Pokémon by total within each type1 using the RANK() window function.
-- MAGIC - Filter the result to include only the top 2 Pokémon from each type1

-- COMMAND ----------

with c1 as (
  select *, rank() over(partition by type1 order by total desc) r
  from poke.pokemon_data
)
select * from c1
where r<3

-- COMMAND ----------

