/*
 Determining the Order of Succession
 
 
 Given a table Successors with columns: name, birthday, and gender, write a SQL query to list the names of the King's children in order of their succession to the throne and their birthday("name", "birthday"). Succession is based on age seniority. Prefix the name with "King" for males and "Queen" for females. The result should be sorted by birthday in ascending order to determine the succession order.
 These are the tables to query for this question:
 playground.successors
 
 name string
 birthday date
 gender string
 
 Your answer should include these columns:
 
 name varchar
 birthday date
 */
SELECT CASE
        WHEN gender = 'M' THEN 'King ' || name
        WHEN gender = 'F' THEN 'Queen ' || name
    END AS name,
    birthday
FROM playground.successors
ORDER BY birthday