-- Databricks notebook source
-- MAGIC %python
-- MAGIC x = 5

-- COMMAND ----------

create schema leet

-- COMMAND ----------

CREATE TABLE leet.tasks1 (
    task_id INT,
    subtasks_count INT
);

INSERT INTO leet.tasks1 VALUES
(1, 3),
(2, 2),
(3, 4);

-- COMMAND ----------

CREATE TABLE leet.executed1 (
    task_id INT,
    subtask_id INT
);

INSERT INTO leet.executed1 VALUES
(1, 2),
(3, 1),
(3, 2),
(3, 3),
(3, 4);

-- COMMAND ----------

select * from leet.tasks1

-- COMMAND ----------

select *
from leet.tasks1 t join leet.executed1 e
on t.task_id=e.task_id 
and t.subtasks_count>=e.subtask_id
LATERAL VIEW EXPLODE(ARRAY(30, 60)) tableName AS c_age  -- like cross join

-- COMMAND ----------

select explode(sequence(1, 3))

-- COMMAND ----------

select *
from leet.tasks1

-- COMMAND ----------

select *, explode(sequence(1, subtasks_count)) col
from leet.tasks1

-- COMMAND ----------

select * from leet.executed1
order by task_id

-- COMMAND ----------

with c1 as 
    (select *, 
            explode(sequence(1, subtasks_count)) col
    from leet.tasks1)
select task_id, 
       col
from c1 
except 
select * from leet.executed1
order by task_id
