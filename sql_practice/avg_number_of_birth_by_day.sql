/*
 Average Number of Births by Day of the Week
 
 
 Create a SQL query that finds the average number of births for each day of the week across all years in the playground.us_birth_stats table. Cast the average as an integer. Order the results by the day of the week.
 These are the tables to query for this question:
 playground.us_birth_stats
 
 year int
 month int
 date_of_month int
 day_of_week int
 births int
 
 Your answer should include these columns:
 
 day_of_week integer
 average_births integer
 */
SELECT day_of_week,
    CAST(AVG(births) AS INT) AS average_births
FROM playground.us_birth_stats
GROUP BY day_of_week
ORDER BY day_of_week