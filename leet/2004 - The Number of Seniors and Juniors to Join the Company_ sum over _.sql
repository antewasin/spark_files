-- Databricks notebook source
-- MAGIC %md
-- MAGIC https://leetcode.ca/2021-09-18-2004-The-Number-of-Seniors-and-Juniors-to-Join-the-Company/

-- COMMAND ----------

CREATE TABLE Employees2 (
    employee_id INT NOT NULL,  -- Unique identifier for the employee
    experience VARCHAR(50),    -- Experience level of the employee (Junior, Senior)
    salary INT                -- Salary of the employee
);

-- COMMAND ----------

INSERT INTO Employees2 (employee_id, experience, salary)
VALUES
    (1, 'Junior', 10000),
    (9, 'Junior', 10000),
    (2, 'Senior', 20000),
    (11, 'Senior', 20000),
    (13, 'Senior', 50000),
    (4, 'Junior', 40000);

-- COMMAND ----------

  select employee_id, sum(salary) cc
  from Employees2
  where experience='Senior'
 group by rollup(employee_id)


-- COMMAND ----------

 
select c1.employee_id, c1.experience, c1.cc, ccc
from (
  select employee_id, experience, salary cc, lead(salary) over(order by salary) ccc, lag(salary) over(order by salary) ccc
  from Employees2
  where experience='Senior'
  order by salary asc 
) as c1

-- COMMAND ----------

WITH SeniorHires AS (
    -- Select all senior employees sorted by salary in ascending order
    SELECT employee_id, salary
    FROM Employees2
    WHERE experience = 'Senior'
    ORDER BY salary ASC
),
JuniorHires AS (
    -- Select all junior employees sorted by salary in ascending order
    SELECT employee_id, salary
    FROM Employees2
    WHERE experience = 'Junior'
    ORDER BY salary ASC
),
HiredSeniors AS (
    -- Hire as many seniors as possible within the budget
    SELECT employee_id, salary
    FROM SeniorHires
    WHERE (SELECT SUM(salary) FROM SeniorHires WHERE salary <= SeniorHires.salary) <= 70000
),
RemainingBudget AS (
    -- Calculate the remaining budget after hiring seniors
    SELECT 70000 - SUM(salary) AS remaining_budget FROM HiredSeniors
),
HiredJuniors AS (
    -- Hire juniors within the remaining budget
    SELECT employee_id, salary
    FROM JuniorHires
    WHERE salary <= (SELECT remaining_budget FROM RemainingBudget)
)
SELECT * FROM HiredSeniors
UNION ALL
SELECT * FROM HiredJuniors;

-- COMMAND ----------

WITH SeniorCandidates AS (
    -- Select all senior employees ordered by salary
    SELECT salary
    FROM Employees2
    WHERE experience = 'Senior'
    ORDER BY salary ASC
),
JuniorCandidates AS (
    -- Select all junior employees ordered by salary
    SELECT salary
    FROM Employees2
    WHERE experience = 'Junior'
    ORDER BY salary ASC
),
SeniorsHired AS (
    -- Hire seniors while the budget allows
    SELECT salary
    FROM SeniorCandidates
    WHERE (SELECT SUM(salary) FROM SeniorCandidates WHERE salary <= SeniorCandidates.salary) <= 70000
),
RemainingBudget AS (
    -- Calculate the remaining budget after hiring seniors
    SELECT 70000 - SUM(salary) AS remaining_budget
    FROM SeniorsHired
),
JuniorsHired AS (
    -- Hire juniors with the remaining budget
    SELECT salary
    FROM JuniorCandidates
    WHERE salary <= (SELECT remaining_budget FROM RemainingBudget)
)
SELECT 'Senior' AS experience, COUNT(*) AS accepted_candidates
FROM SeniorsHired
UNION ALL
SELECT 'Junior', COUNT(*)
FROM JuniorsHired;

-- COMMAND ----------

WITH SeniorSalaries AS (
    SELECT
        salary
    FROM
        Employees2
    WHERE
        experience = 'Senior'
    ORDER BY
        salary
),
JuniorSalaries AS (
    SELECT
        salary
    FROM
        Employees2
    WHERE
        experience = 'Junior'
    ORDER BY
        salary
),
SeniorCount AS (
    SELECT
        COUNT(*) AS senior_count
    FROM (
        SELECT salary
        FROM SeniorSalaries
        WHERE SUM(salary) OVER (ORDER BY salary) <= 70000
    ) AS affordable_senior
),
RemainingBudget AS (
    SELECT 70000 - (SELECT SUM(salary) FROM SeniorSalaries WHERE SUM(salary) OVER (ORDER BY salary) <= 70000) AS remaining_budget
),
JuniorCount AS (
    SELECT
        COUNT(*) AS junior_count
    FROM (
        SELECT salary
        FROM JuniorSalaries
        WHERE salary <= (SELECT remaining_budget FROM RemainingBudget)
    ) AS affordable_junior
)
SELECT
    'Senior' AS experience,
    (SELECT senior_count FROM SeniorCount) AS accepted_candidates
UNION ALL
SELECT
    'Junior' AS experience,
    (SELECT junior_count FROM JuniorCount) AS accepted_candidates;

-- COMMAND ----------

with s as
  (select employee_id,
          sum(salary) over(
                           order by salary) cur
   from Employees2
   where experience = 'Senior' ),
     j as
  (select employee_id,
          ifnull(
                   (select max(cur)
                    from s
                    where cur <= 70000 ), 0) + sum(salary) over(
                                                                order by salary) cur
   from Employees2
   where experience = 'Junior' )
select 'Senior' experience,
                count(employee_id) accepted_candidates
from s
where cur <= 70000
  union
all
  select 'Junior' experience,
                  count(employee_id) accepted_candidates
  from j where cur <= 70000;


-- COMMAND ----------

  select * from Employees2

-- COMMAND ----------

with s as
  (select employee_id,
          sum(salary) over(
                           order by salary) cur
   from Employees2
   where experience = 'Senior' ),
   j as
  (select employee_id,
          ifnull(
                   (select max(cur)
                    from s
                    where cur <= 70000 ), 0) + sum(salary) over(
                                                                order by salary) cur
   from Employees2
   where experience = 'Junior' )
select 'Senior' experience, 
                count(employee_id) accepted_candidates
from s
where cur <= 70000
group by 1

-- COMMAND ----------


