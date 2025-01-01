-- 1. **DDL for `actors` table:**
-- a. An array of struct
CREATE TYPE films AS (
    film TEXT,
    votes INTEGER,
    rating REAL,
    filmid TEXT
);
-- b. An enumeration of quality_class
CREATE TYPE quality_class AS ENUM ('star', 'good', 'average', 'bad');
-- c. 'actors' table with required and relevant fields
CREATE TABLE actors (
    actorid TEXT,
    actor TEXT,
    -- Array of the `films` type
    films films [],
    -- Array of the `quality_class` enum type
    quality_class quality_class [],
    is_active BOOLEAN NOT NULL,
    current_year INTEGER NOT NULL,
    PRIMARY KEY (actorid)
);