-- 2. **Cumulative table generation query:**
-- Build cumulative table from the year before the min year. This is the seed query to which you join on the min year which is where data begins.
WITH min_year AS (
    SELECT MIN(YEAR)
    FROM actor_films;
),
last_year AS (
    SELECT *
    FROM actors -- Last year comes from the cumulative actors table we're building. For seed query we start with year = min_year - 1.
    WHERE current_year = min_year - 1;
),
this_year AS (
    SELECT *
    FROM actor_films -- This year comes from actor_films table with all the events.
    WHERE Year = min_year;
);
-- I now change approach to work in smaller steps, follow tutorial side-by-side and starting back from full outer join
INSERT INTO players
SELECT COALESCE(ly.actorid, ty.actorid) AS actorid,
    COALESCE(ly.actor, ty.actor) AS actor,
    -- COALESCE non-temporal fields
    CASE
        WHEN ly.films IS NULL -- No film history for this actor
        THEN ARRAY [ROW(
            ty.film,
            ty.votes,
            ty.rating,
            ty.filmid
        )::films],
        WHEN ty.films IS NOT NULL -- New film to add
        THEN ly.films || ARRAY [ROW(
            ty.film,
            ty.votes,
            ty.rating,
            ty.filmid
        )::films] ARRAY []
        ELSE ly.films -- No new film to add
    END AS films,
    -- To add quality_class[]
    --is_active = current_year = min_year,
    COALESCE(ty.current_year, ly.current_year + 1) AS current_year,
    """
    COALESCE(ly.films, ty.films) AS films,
    COALESCE(ly.quality_class, this_year.quality_class) AS quality_class,
    COALESCE(last_year.is_active, this_year.is_active) AS is_active
    """
FROM last_year ly
    FULL OUTER JOIN this_year ty ON ly.actorid = ty.actorid