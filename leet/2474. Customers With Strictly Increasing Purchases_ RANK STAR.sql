-- Databricks notebook source
-- MAGIC %md
-- MAGIC https://leetcode.ca/2023-01-01-2474-Customers-With-Strictly-Increasing-Purchases/

-- COMMAND ----------

CREATE TABLE Orders3 (
    order_id INT,
    customer_id INT,
    order_date DATE,
    price INT
);

-- Insert data into Orders table
INSERT INTO Orders3 (order_id, customer_id, order_date, price) VALUES
(1, 1, '2019-07-01', 1100),
(2, 1, '2019-11-01', 1200),
(3, 1, '2020-05-26', 3000),
(4, 1, '2021-08-31', 3100),
(5, 1, '2022-12-07', 4700),
(6, 2, '2015-01-01', 700),
(7, 2, '2017-11-07', 1000),
(8, 3, '2017-01-01', 900),
(9, 3, '2018-11-07', 900);

-- COMMAND ----------

select * from orders3

-- COMMAND ----------

with c1 as
  (select customer_id,
          year(order_date) yr,
          sum(price) price--over(partition by customer_id order by year(order_date)) cc
from orders3
   group by 1,
            2
   order by customer_id,
            yr),
     c2 as
  (select customer_id,
          yr,
          price,
          lead(yr) over(partition by customer_id
                        order by yr) ll
   from c1),
     c3 as
  (select distinct customer_id
   from c2
   where ll<>yr+1),
     c4 as
  (SELECT *
   FROM c2
   where customer_id not in
       (select customer_id
        from c3) ),
     c5 as
  (select *,
          lead(price) over(partition by customer_id
                           order by yr) t
   from c4),
     c6 as
  (select *,
          case
              when t>price then 1
              when t<=price then -1
              else 0
          end as h
   from c5)
select distinct customer_id
from c6
where customer_id not in
    (select customer_id
     from c6
     where h=-1)


-- COMMAND ----------

select customer_id
from
  (select customer_id,
          year(order_date),
          sum(price) as total,
          year(order_date) - rank() over(partition by customer_id
                                         order by sum(price)) as rk
   from Orders3
   group by customer_id,
            year(order_date)) t
group by customer_id
having count(distinct rk) = 1;


-- COMMAND ----------

select customer_id,
          year(order_date),
          sum(price) as total,
          rank() over(partition by customer_id
                                         order by sum(price)) as rk1,
          year(order_date) - rank() over(partition by customer_id
                                         order by sum(price)) as rk
   from Orders3
      group by customer_id,
            year(order_date) 

-- COMMAND ----------

select customer_id, rk
from 
(
select customer_id,
          year(order_date),
          sum(price) as total,
          rank() over(partition by customer_id
                                         order by sum(price)) as rk1,
          year(order_date) - rank() over(partition by customer_id
                                         order by sum(price)) as rk
   from Orders3
      group by customer_id,
            year(order_date) 
)
--group by 1
--having count(distinct rk)=1

-- COMMAND ----------

select customer_id
from 
(
select customer_id,
          year(order_date),
          sum(price) as total,
          rank() over(partition by customer_id
                                         order by sum(price)) as rk1,
          year(order_date) - rank() over(partition by customer_id
                                         order by sum(price)) as rk
   from Orders3
      group by customer_id,
            year(order_date) 
)
group by customer_id
having count(distinct rk)=1

-- COMMAND ----------


