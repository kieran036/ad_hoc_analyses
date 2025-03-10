/*
 A casino tracks the win slash loss ratios of its players. Given the results of the players' games, write a query to fetch the names of the players and the ratio of the wins to losses. Order the results by ratio descending, then by name. Note, round the value of the ratio to two decimal places and include trailing zeros, for example 5.00.
 
 The schema involves two tables.
 - players, with columns id (int) and pname varchar(30)
 - games, with columns id (int) and result varchar(5) as either 'won' or 'lost'.
 */
WITH summary_1 AS (
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
    GROUP BY id
),
summary_2 AS(
    SELECT id,
        ROUND (
            CASE
                WHEN losses = 0 THEN wins -- Avoid division by zero
                ELSE wins * 1.00 / losses -- Convert to float
            END,
            2
        ) AS ratio
    FROM summary_1
)
SELECT p.pname AS player_name,
    s.ratio AS ratio
FROM summary_2
    INNER JOIN players AS p ON summary_2.id = p.id
ORDER BY ratio DESC,
    player_name ASC;