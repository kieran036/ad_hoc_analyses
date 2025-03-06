/*
 Total Number of Births Per Year
 
 Write a SQL query to calculate the total number of births recorded for each year in the playground.us_birth_stats table. Order the results by year. These are the tables to query for this question:
 playground.us_birth_stats
 
 year int
 month int
 date_of_month int
 day_of_week int
 births int
 
 Your answer should include these columns:
 
 year integer
 total_births integer
 
 */
SELECT year,
    SUM(births) AS total_births
FROM playground.us_birth_stats
GROUP BY year
ORDER BY year