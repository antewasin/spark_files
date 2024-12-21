-- Databricks notebook source
-- MAGIC %md
-- MAGIC https://leetcode.ca/2023-01-21-2494-Merge-Overlapping-Events-in-the-Same-Hall/

-- COMMAND ----------

CREATE TABLE HallEvents (
    hall_id INT,
    start_day DATE,
    end_day DATE
);

-- Insert data into HallEvents table
INSERT INTO HallEvents (hall_id, start_day, end_day) VALUES
(1, '2023-01-13', '2023-01-14'),
(1, '2023-01-14', '2023-01-17'),
(1, '2023-01-18', '2023-01-25'),
(2, '2022-12-09', '2022-12-23'),
(2, '2022-12-13', '2022-12-17'),
(3, '2022-12-01', '2023-01-30');

-- COMMAND ----------

select * from HallEvents

-- COMMAND ----------

with c1 as
  (select *,
          lead(start_day) over(partition by hall_id
                               order by hall_id, start_day) lead_sd,
          lead(end_day) over(partition by hall_id
                             order by hall_id, start_day) lead_ed 
from HallEvents),
     c2 as
  (select *,
          iff(lead_sd<=end_day, lead_ed, end_day) new
   from c1),
     c3 as
  (select hall_id,
          new as end_day,
                 min(start_day) start_day
   from c2 
   group by 1,2)
select *
from c2


-- COMMAND ----------

with c1 as (select *, 
            lead(start_day) over(partition by hall_id order by hall_id, start_day) lead_sd,
            lead(end_day) over(partition by hall_id order by hall_id, start_day) lead_ed
            --lag(end_day) over(partition by hall_id order by hall_id, start_day) lag_ed,
            --max(end_day) over(partition by hall_id) max_ed
from HallEvents),
c2 as (
  select *, iff(lead_sd<=end_day, lead_ed, end_day) new
  from c1 
),
c3 as (
  select hall_id, new as end_day, min(start_day) start_day
  from c2
  --where end_day<=nsd
  group by 1,2
)
select hall_id, start_day, end_day
from c3

-- COMMAND ----------

WITH
    S AS (
        SELECT
            hall_id,
            start_day,
            end_day,
            max(end_day) OVER (
                PARTITION BY hall_id
                ORDER BY start_day
            ) AS cur_max_end_day
        FROM HallEvents
    ),
    T AS (
        SELECT
            *,
            if(
                start_day <= lag(cur_max_end_day) OVER (
                    PARTITION BY hall_id
                    ORDER BY start_day
                ),
                0,
                1
            ) AS start
        FROM S
    ),
    P AS (
        SELECT
            *,
            sum(start) OVER (
                PARTITION BY hall_id
                ORDER BY start_day
            ) AS gid
        FROM T
    )
SELECT hall_id, min(start_day) AS start_day, max(end_day) AS end_day
FROM P
GROUP BY hall_id, gid;

-- COMMAND ----------

WITH
    S AS (
        SELECT
            hall_id,
            start_day,
            end_day,
            max(end_day) OVER (
                PARTITION BY hall_id
                ORDER BY start_day
            ) AS cur_max_end_day
        FROM HallEvents
    ),
    T AS (
        SELECT
            *,
            lag(cur_max_end_day) OVER (
                    PARTITION BY hall_id
                    ORDER BY start_day) as ll,
            if(
                start_day <= lag(cur_max_end_day) OVER (
                    PARTITION BY hall_id
                    ORDER BY start_day
                ),
                0,
                1
            ) AS start
        FROM S
    ),
    P AS (
        SELECT
            *,
            sum(start) OVER (
                PARTITION BY hall_id
                ORDER BY start_day
            ) AS gid
        FROM T
    )
SELECT *
FROM T


-- COMMAND ----------

WITH
    S AS (
        SELECT
            hall_id,
            start_day,
            end_day,
            max(end_day) OVER (
                PARTITION BY hall_id
                ORDER BY start_day
            ) AS cur_max_end_day
        FROM HallEvents
    ),
    T AS (
        SELECT
            *,
            lag(cur_max_end_day) OVER (
                    PARTITION BY hall_id
                    ORDER BY start_day) as ll,
            if(
                start_day <= lag(cur_max_end_day) OVER (
                    PARTITION BY hall_id
                    ORDER BY start_day
                ),
                0,
                1
            ) AS start
        FROM S
    ),
    P AS (
        SELECT
            *,
            sum(start) OVER (
                PARTITION BY hall_id
                ORDER BY start_day
            ) AS gid
        FROM T
    )
SELECT *
FROM P

-- COMMAND ----------


