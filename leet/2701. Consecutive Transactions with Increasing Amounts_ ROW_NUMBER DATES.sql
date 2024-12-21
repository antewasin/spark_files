-- Databricks notebook source
-- MAGIC %md
-- MAGIC https://leetcode.ca/2023-04-23-2701-Consecutive-Transactions-with-Increasing-Amounts/

-- COMMAND ----------

CREATE TABLE Transactions (
    transaction_id INT,
    customer_id INT,
    transaction_date DATE,
    amount INT
)
USING DELTA;

INSERT INTO Transactions VALUES
    (1, 101, '2023-05-01', 100),
    (2, 101, '2023-05-02', 150),
    (3, 101, '2023-05-03', 200),
    (4, 102, '2023-05-01', 50),
    (5, 102, '2023-05-03', 100),
    (6, 102, '2023-05-04', 200),
    (7, 105, '2023-05-01', 100),
    (8, 105, '2023-05-02', 150),
    (9, 105, '2023-05-03', 200),
    (10, 105, '2023-05-04', 300),
    (11, 105, '2023-05-12', 250),
    (12, 105, '2023-05-13', 260),
    (13, 105, '2023-05-14', 270);

-- COMMAND ----------

select * from transactions

-- COMMAND ----------

with A as (
       select *, 
       lag(customer_id) over(order by customer_id, transaction_date) lag_cust,
       lag(transaction_date) over(partition by customer_id order by transaction_date) lag_td,
       lead(transaction_date) over(partition by customer_id order by transaction_date) lead_td,
       lag(amount) over(partition by customer_id order by transaction_date) lag_am,
       lead(amount) over(partition by customer_id order by transaction_date) lead_am,
       first_value(transaction_date) over(partition by customer_id) fv_td,
       last_value(transaction_date) over(partition by customer_id) last_td
       from transactions),
       B as (
        select *,
               case when ((date_add(transaction_date, 1)=lead_td or (date_add(transaction_date, -1)=lag_td))  and (lead_am>amount or lag_am<amount))
               then 1 else 0 end ab2
        from A
       ),
       C as (
        select *,
        row_number() over(partition by customer_id order by transaction_date) rn,
        transaction_date - row_number() over(partition by customer_id order by transaction_date) rnd
        
        from B
       )

select customer_id, min(transaction_date), max(transaction_date), count(*)
from C
group by customer_id, rnd
having count(*)> 2

-- COMMAND ----------

with A as (
        select *,
        row_number() over(partition by customer_id order by transaction_date) rn,
        transaction_date - row_number() over(partition by customer_id order by transaction_date) rnd
        from Transactions
       )

select *
from A


-- COMMAND ----------

with A as (
        select *,
        row_number() over(partition by customer_id order by transaction_date) rn,
        transaction_date - row_number() over(partition by customer_id order by transaction_date) rnd
        from Transactions
       )

select customer_id, min(transaction_date), max(transaction_date), count(*)
from A
group by customer_id, rnd
having count(*)> 2


-- COMMAND ----------


