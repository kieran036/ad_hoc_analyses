/*
 Month with the Highest Total Births
 
 
 Determine the month with the highest total number of births in the playground.us_birth_stats table. The output should show the month and the total number of births.
 These are the tables to query for this question:
 playground.us_birth_stats
 
 year int
 month int
 date_of_month int
 day_of_week int
 births int
 
 Your answer should include these columns:
 
 month integer
 total_births integer
 
 Notes:
 -RANK () OVER (expression)
 */
WITH t_births AS (
    SELECT "month",
        SUM(births) AS total_births,
        RANK() OVER(
            ORDER BY SUM(births) DESC
        ) as rank
    FROM playground.us_birth_stats
    GROUP BY "month"
)
SELECT "month",
    total_births
FROM t_births
WHERE rank = 1