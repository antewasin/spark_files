-- Databricks notebook source
-- MAGIC %md
-- MAGIC 1479 - Sales by Day of the Week
-- MAGIC
-- MAGIC https://leetcode.ca/2019-12-18-1479-Sales-by-Day-of-the-Week/

-- COMMAND ----------

create schema leet

-- COMMAND ----------

CREATE TABLE leet.orders (
    order_id INT,
    customer_id INT,
    order_date DATE,
    item_id INT,
    quantity INT
);

INSERT INTO leet.orders VALUES
(1, 1, '2020-06-01', 1, 10),
(2, 1, '2020-06-08', 2, 10),
(3, 2, '2020-06-02', 1, 5),
(4, 3, '2020-06-03', 3, 5),
(5, 4, '2020-06-04', 4, 1),
(6, 4, '2020-06-05', 5, 5),
(7, 5, '2020-06-05', 1, 10),
(8, 5, '2020-06-14', 4, 5),
(9, 5, '2020-06-21', 3, 5);

-- COMMAND ----------

select * from leet.orders

-- COMMAND ----------

drop table leet.items

-- COMMAND ----------

CREATE TABLE leet.items (
    item_id INT,
    item_name STRING,
    item_category STRING
);

INSERT INTO leet.items VALUES
(1, 'LC Alg. Book', 'Book'),
(2, 'LC DB. Book', 'Book'),
(3, 'LC SmarthPhone', 'Phone'),
(4, 'LC Phone 2020', 'Phone'),
(5, 'LC SmartGlass', 'Glasses'),
(6, 'LC T-Shirt XL', 'T-Shirt');

-- COMMAND ----------

select * from leet.items

-- COMMAND ----------

select * from leet.orders

-- COMMAND ----------

create view t as (select *, date_format(order_date, 'EEEE' ) dt from leet.orders)

-- COMMAND ----------

select * from t

-- COMMAND ----------

select li.item_category,
       ifnull(sum(Monday), 0),
       ifnull(sum(Tuesday), 0),
       ifnull(sum(Wednesday), 0),
       ifnull(sum(Friday), 0),
       ifnull(sum(Sunday), 0)
from t pivot(SUM(quantity) as ss
             for dt in ('Monday' as Monday, 'Tuesday' as Tuesday, 'Wednesday' as Wednesday, 'Friday' as Friday, 'Sunday' as Sunday))
right join leet.items li on t.item_id=li.item_id
group by li.item_category

-- COMMAND ----------

SELECT
    item_category AS category,
    SUM(IF(DAYOFWEEK(order_date) = '2', quantity, 0)) AS Monday,
    SUM(IF(DAYOFWEEK(order_date) = '3', quantity, 0)) AS Tuesday,
    SUM(IF(DAYOFWEEK(order_date) = '4', quantity, 0)) AS Wednesday,
    SUM(IF(DAYOFWEEK(order_date) = '5', quantity, 0)) AS Thursday,
    SUM(IF(DAYOFWEEK(order_date) = '6', quantity, 0)) AS Friday,
    SUM(IF(DAYOFWEEK(order_date) = '7', quantity, 0)) AS Saturday,
    SUM(IF(DAYOFWEEK(order_date) = '1', quantity, 0)) AS Sunday
FROM
    leet.orders AS o
    RIGHT JOIN leet.items AS i ON o.item_id = i.item_id
GROUP BY category
ORDER BY category;

-- COMMAND ----------


