-- Databricks notebook source
-- MAGIC %md
-- MAGIC https://leetcode.ca/2022-09-11-2362-Generate-the-Invoice/

-- COMMAND ----------

-- Create Products table
CREATE TABLE Products3 (
    product_id INT,
    price INT
);

-- Insert data into Products table
INSERT INTO Products3 (product_id, price) VALUES
(1, 100),
(2, 200);



-- COMMAND ----------

-- Create Purchases table
CREATE TABLE Purchases3 (
    invoice_id INT,
    product_id INT,
    quantity INT
);

-- Insert data into Purchases table
INSERT INTO Purchases3 (invoice_id, product_id, quantity) VALUES
(1, 1, 2),
(3, 2, 1),
(2, 2, 3),
(2, 1, 4),
(4, 1, 10);

-- COMMAND ----------

select * from Products3

-- COMMAND ----------

select * from Purchases3

-- COMMAND ----------

select *, price * quantity cash from Purchases3 pur join Products3 pd
on pur.product_id=pd.product_id

-- COMMAND ----------

describe (select *, price * quantity cash from Purchases3 pur join Products3 pd
on pur.product_id=pd.product_id)

-- COMMAND ----------

create temp view temp1
as
with c1 as
  (select invoice_id,
          sum(price * quantity) cash
   from Purchases3 pur
   join Products3 pd on pur.product_id=pd.product_id
   group by 1),
     c2 as
  (select *,
          rank() over(partition by cash
                      order by invoice_id, invoice_id) r
   from c1)
select *
from c2
where r=1

-- COMMAND ----------

with c1 as
  (select invoice_id,
          sum(price * quantity) cash
   from Purchases3 pur
   join Products3 pd on pur.product_id=pd.product_id
   group by 1),
     c2 as
  (select *,
          rank() over(partition by cash
                      order by invoice_id, invoice_id) r
   from c1),
     c3 as
  (select pur.invoice_id,
          pur.product_id,
          pur.quantity,
          pd.price,
          pd.price * quantity cash
   from Purchases3 pur
   join Products3 pd on pur.product_id=pd.product_id
   where pur.invoice_id in
       (select invoice_id
        from c2)),
     c4 as
  (select c3.*,
          c2.r
   from c3
   join c2 on c2.invoice_id=c3.invoice_id
   where r=1),
     c5 as
  (select *,
          rank() over(partition by invoice_id
                      order by c4.price desc) r2
   from c4)
select product_id,
       quantity,
       cash
from c5
where c5.r2=1

-- COMMAND ----------


