-- 2. **Cumulative table generation query:**
-- Build cumulative table from the year before the min year. This acts like a blank query to which you join on the min year which is where data begins.
WITH min_year AS (
    SELECT MIN(YEAR)
    FROM actor_films
),
last_year AS (
    SELECT * -- Last year comes from the cumulative table we're building. For the first build we start with year = min_year - 1
    FROM actors
    WHERE current_year = min_year - 1
),
this_year AS (
    SELECT * -- This year comes from the data we have in the actor_films table of all the events.
    FROM actor_films
    WHERE Year = min_year
) -- I now change approach to work in smaller steps, follow tutorial side-by-side and starting back from full outer join
INSERT INTO players
SELECT COALESCE(ly.actorid, ty.actorid) AS actorid,
    COALESCE(ly.actor, ty.actor) AS actor,
    -- To add films[]
    -- To add quality_class[]
    current_year = min_year,
    """
    COALESCE(ly.films, ty.films) AS films,
    COALESCE(ly.quality_class, this_year.quality_class) AS quality_class,
    COALESCE(last_year.is_active, this_year.is_active) AS is_active
    """
FROM last_year ly
    FULL OUTER JOIN this_year ty ON ly.actorid = ty.actorid