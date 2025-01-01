-- 2. **Cumulative table generation query:**
WITH min_year AS (
    SELECT MIN(YEAR)
    FROM actor_films
),
last_year AS (
    SELECT *
    FROM actor_films
    WHERE current_year = min_year - 1
),
this_year AS (
    SELECT *
    FROM actor_films
    WHERE Year = min_year
)
INSERT INTO players
SELECT COALESCE(last_year.actorid, this_year.actorid) AS actorid,
    COALESCE(last_year.actor, this_year.actor) AS actor,
    COALESCE(last_year.films, this_year.films) AS films,
    COALESCE(last_year.quality_class, this_year.quality_class) AS quality_class,
    COALESCE(last_year.is_active, this_year.is_active) AS is_active
FROM last_year ly
    FULL OUTER JOIN this_year ty ON ly.actorid = ty.actorid