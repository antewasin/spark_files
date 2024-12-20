-- Databricks notebook source
-- MAGIC %md
-- MAGIC https://leetcode.ca/2022-05-24-2252-Dynamic-Pivoting-of-a-Table/

-- COMMAND ----------

CREATE TABLE Products (
    product_id INT,
    store VARCHAR(50),
    price INT
);

INSERT INTO Products (product_id, store, price) VALUES
(1, 'Shop', 110),
(1, 'LC_Store', 100),
(2, 'Nozama', 200),
(2, 'Souq', 190),
(3, 'Shop', 1000),
(3, 'Souq', 1900);

-- COMMAND ----------

select * from products

-- COMMAND ----------

SELECT 
    product_id,
    LC_Store,
    Nozama,
    Souq,
    Shop
FROM 
    products AS p
PIVOT (
    SUM(price) 
    FOR store IN ('LC_Store' LC_Store, 'Nozama' Nozama, 'Souq' Souq, 'Shop' Shop)
)

-- COMMAND ----------

select product_id,
       sum(iff(store='LC_Store',price,null )) LC_Store,
       sum(iff(store='Nozama',price,null )) Nozama,
       sum(iff(store='Souq',price,null )) Souq,
       sum(iff(store='Shop',price,null )) Shop
from products p
group by 1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC https://leetcode.ca/2022-05-25-2253-Dynamic-Unpivoting-of-a-Table/

-- COMMAND ----------

CREATE TABLE Products2 (
    product_id INT,
    LC_Store INT,
    Nozama INT,
    Shop INT,
    Souq INT
);

INSERT INTO Products2 (product_id, LC_Store, Nozama, Shop, Souq) VALUES
(1, 100, NULL, 110, NULL),
(2, NULL, 200, NULL, 190),
(3, NULL, NULL, 1000, 1900);

-- COMMAND ----------

select * from products2

-- COMMAND ----------


select product_id, store, price
from products2 as p
UNPIVOT (
    price
    FOR store IN ( LC_Store, Nozama, Souq,  Shop)
)

-- COMMAND ----------


