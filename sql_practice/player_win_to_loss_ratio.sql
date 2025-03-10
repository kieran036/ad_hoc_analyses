/*
 A casino tracks the win slash loss ratios of its players. Given the results of the players' games, write a query to fetch the names of the players and the ratio of the wins to losses. Order the results by ratio descending, then by name. Note, round the value of the ratio to two decimal places and include trailing zeros, for example 5.00.
 
 The schema involves two tables.
 - players, with columns id (int) and pname varchar(30)
 - games, with columns id (int) and result varchar(5) as either 'won' or 'lost'.
 */
WITH summary AS (
    SELECT id,
        SUM (
            CASE
                WHEN result = 'won' THEN 1
                ELSE 0
            END
        ) AS wins,
        SUM (
            CASE
                WHEN result = 'lost' THEN 1
                ELSE 0
            END
        ) AS losses
    FROM games
),