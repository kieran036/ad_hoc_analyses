/*
 Find Viewers with Multiple Article Views in a Day
 
 
 Using the table playground.views, write a SQL query to identify all viewers who viewed more than one article on the same day. The table includes columns viewer_id (the ID of the viewer), article_id (the ID of the article viewed), and view_date (the date of the view). The result should contain a single column named viewer_id, listing each viewer who meets the criteria without duplicates, and should be sorted in ascending order of viewer_id.
 These are the tables to query for this question:
 playground.views
 
 article_id int
 author_id int
 viewer_id int
 view_date date
 
 Your answer should include these columns:
 
 viewer_id integer
 
 Notes:
 -Comma only inbetween CTE, not after final CTE
 -Make rows distinct first
 -Having uses the expression but should minimise having
 */
WITH t_unique AS (
    SELECT DISTINCT viewer_id,
        view_date,
        article_id,
        author_id
    FROM playground.views
),
t_articles AS (
    SELECT viewer_id,
        COUNT(article_id) AS article_views,
        view_date
    FROM t_unique
    GROUP BY viewer_id,
        view_date
)
SELECT DISTINCT viewer_id
FROM t_articles
WHERE article_views > 1
ORDER BY viewer_id ASC