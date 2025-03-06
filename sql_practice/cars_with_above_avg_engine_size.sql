/*
 Cars with Above Average Engine Size
 
 
 Using the table playground.automobile, Create a SQL query to identify cars that have an engine size above the average across all cars in the dataset. The result should include the brand, fuel_type, and engine size, ordered by engine size in descending order and then brand_name in asc order.
 These are the tables to query for this question:
 playground.automobile
 
 brand_name string
 fuel_type string
 aspiration string
 door_panel string
 design string
 wheel_drive string
 engine_location string
 engine_type string
 cylinder_count string
 engine_size int
 fuel_system string
 bore double
 stroke double
 compression_ratio double
 horse_power int
 top_RPM int
 city_mileage int
 highway_mileage int
 price_in_dollars int
 
 Your answer should include these columns:
 
 brand_name varchar
 fuel_type varchar
 engine_size integer
 */
SELECT brand_name,
    fuel_type,
    engine_size
FROM playground.automobile
WHERE engine_size > (
        SELECT AVG(engine_size) as avg_engine
        FROM playground.automobile
    )
ORDER BY engine_size DESC,
    brand_name ASC