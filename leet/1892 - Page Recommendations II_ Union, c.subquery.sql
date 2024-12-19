-- Databricks notebook source
-- MAGIC %md
-- MAGIC https://leetcode.ca/2021-07-25-1892-Page-Recommendations-II/

-- COMMAND ----------

CREATE TABLE Friendship (
    user1_id INT,
    user2_id INT
);

-- Insert data into the Friendship table
INSERT INTO Friendship VALUES
(1, 2),
(1, 3),
(1, 4),
(2, 3),
(2, 4),
(2, 5),
(6, 1);

-- Create the Likes table
CREATE TABLE Likes (
    user_id INT,
    page_id INT
);

-- Insert data into the Likes table
INSERT INTO Likes VALUES
(1, 88),
(2, 23),
(3, 24),
(4, 56),
(5, 11),
(6, 33),
(2, 77),
(3, 77),
(6, 88);

-- COMMAND ----------

select * from Likes

-- COMMAND ----------

select * from Friendship

-- COMMAND ----------

select f.user1_id as user_id,
       l.page_id,
       count(*) as friends_likes
from
  (select user1_id,
          user2_id
   from Friendship
   union select user2_id,
                user1_id
   from Friendship) as f
inner join Likes l on f.user2_id = l.user_id
where not exists
    (select *
     from Likes
     where user_id = f.user1_id
       and page_id = l.page_id )
group by f.user1_id,
         l.page_id;


-- COMMAND ----------

WITH BiDirectionalFriendship AS (
    -- Create a unified table for both user1 -> user2 and user2 -> user1 relationships
    SELECT user1_id, user2_id 
    FROM Friendship
    UNION 
    SELECT user2_id AS user1_id, user1_id AS user2_id 
    FROM Friendship
),
FriendsLikes AS (
    -- Join the bidirectional friendship table with Likes to get pages liked by friends
    SELECT 
        f.user1_id AS user_id, 
        l.page_id
    FROM 
        BiDirectionalFriendship f
    INNER JOIN 
        Likes l
    ON 
        f.user2_id = l.user_id
),
FilteredFriendsLikes AS (
    -- Exclude pages already liked by the user
    SELECT 
        fl.user_id, 
        fl.page_id
    FROM 
        FriendsLikes fl
    WHERE 
        NOT EXISTS (
            SELECT 1
            FROM Likes l
            WHERE 
                l.user_id = fl.user_id 
                AND l.page_id = fl.page_id
        )
),
FriendsLikesCount AS (
    -- Count the number of friends who liked each page for each user
    SELECT 
        user_id, 
        page_id, 
        COUNT(*) AS friends_likes
    FROM 
        FilteredFriendsLikes
    GROUP BY 
        user_id, 
        page_id
)
-- Final Result
SELECT 
    user_id, 
    page_id, 
    friends_likes
FROM 
    FriendsLikesCount
ORDER BY 
    user_id, page_id;

-- COMMAND ----------


