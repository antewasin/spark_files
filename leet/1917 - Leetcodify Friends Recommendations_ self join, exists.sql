-- Databricks notebook source
-- MAGIC %md
-- MAGIC https://leetcode.ca/2021-08-01-1917-Leetcodify-Friends-Recommendations/

-- COMMAND ----------

CREATE TABLE Songs (
    user_id INT,
    song_id INT,
    day DATE
);

-- Insert data into the Songs table
INSERT INTO Songs (user_id, song_id, day) VALUES
(1, 10, '2021-03-15'),
(1, 11, '2021-03-15'),
(1, 12, '2021-03-15'),
(2, 10, '2021-03-15'),
(2, 11, '2021-03-15'),
(2, 12, '2021-03-15'),
(3, 10, '2021-03-15'),
(3, 11, '2021-03-15'),
(3, 12, '2021-03-15'),
(4, 10, '2021-03-15'),
(4, 11, '2021-03-15'),
(4, 13, '2021-03-15'),
(5, 10, '2021-03-16'),
(5, 11, '2021-03-16'),
(5, 12, '2021-03-16');



-- COMMAND ----------

-- Create the Friendship table
CREATE TABLE Friendship2 (
    user1_id INT,
    user2_id INT
);

-- Insert data into the Friendship table
INSERT INTO Friendship2 (user1_id, user2_id) VALUES
(1, 2);

-- COMMAND ----------

select * from songs

-- COMMAND ----------

select * from Friendship2

-- COMMAND ----------

select s1.user_id,
       s2.user_id,
       count(*) cc
from songs s1
join songs s2 on s1.song_id=s2.song_id
and s1.user_id<>s2.user_id
and s1.day=s2.day
where (s1.user_id,
       s2.user_id) not in
    (select f.user1_id,
            f.user2_id
     from Friendship2 f)
  and (s1.user_id,
       s2.user_id) not in
    (select f.user2_id,
            f.user1_id
     from Friendship2 f)
group by 1,
         2
having count(*)>=3
order by s1.user_id

-- COMMAND ----------

-- Select user_id pairs and count the number of matching songs
select s1.user_id,          -- The first user
       s2.user_id,          -- The second user who matches with the first user
       count(*) as cc       -- Count of common songs they listened to on the same day
from songs s1
join songs s2 
  on s1.song_id = s2.song_id  -- Match songs listened to by both users
 and s1.user_id <> s2.user_id -- Ensure users are not the same
 and s1.day = s2.day          -- Ensure they listened to the songs on the same day
where 
  -- Exclude users who are already friends (direct relationship in Friendship table)
  (s1.user_id, s2.user_id) not in 
    (select f.user1_id, 
            f.user2_id 
     from Friendship2 f)     -- Friendship direction 1 -> 2
  and 
  (s1.user_id, s2.user_id) not in 
    (select f.user2_id, 
            f.user1_id 
     from Friendship2 f)     -- Friendship direction 2 -> 1
group by 
  s1.user_id,                -- Group by first user
  s2.user_id                 -- Group by second user
having count(*) >= 3         -- Only include pairs with at least 3 matching songs
order by 
  s1.user_id;                -- Sort the results by the first user

-- COMMAND ----------

-- Select user_id pairs and count the number of matching songs
select s1.user_id,          -- The first user
       s2.user_id,          -- The second user who matches with the first user
       count(*) as cc       -- Count of common songs they listened to on the same day
from songs s1
join songs s2 
  on s1.song_id = s2.song_id  -- Match songs listened to by both users
 and s1.user_id <> s2.user_id -- Ensure users are not the same
 and s1.day = s2.day          -- Ensure they listened to the songs on the same day
where 
  -- Exclude users who are already friends (direct relationship in Friendship table)
  not exists (
    select 1
    from Friendship2 f
    where (f.user1_id = s1.user_id and f.user2_id = s2.user_id) -- Friendship direction 1 -> 2
       or (f.user1_id = s2.user_id and f.user2_id = s1.user_id) -- Friendship direction 2 -> 1
  )
group by 
  s1.user_id,                -- Group by first user
  s2.user_id                 -- Group by second user
having count(*) >= 3         -- Only include pairs with at least 3 matching songs
order by 
  s1.user_id;                -- Sort the results by the first user

-- COMMAND ----------


