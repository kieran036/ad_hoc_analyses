/*
 Three tables,
 - country table with columns ID and country name
 - city table with columns ID, city name, postal code, and country ID
 - customer table with columns ID, customer name, city ID, customer address, contact person, email, and phone.
 Write a query which will return all cities with more customers than the average number of customers of all cities. For each city, return the country name, city name, and the number of customers. Order the result by country name ascending.
 */
WITH avg_all AS (
    SELECT COUNT(id) * 1.0 / COUNT(DISTINCT city_id) AS avg_customers
    FROM customer
),
city_summary AS (
    SELECT city_id,
        COUNT(id) AS num_customers
    FROM customer
    GROUP BY city_id
) final_cities AS (
    SELECT city_id,
        num_customers
    FROM city_summary
    WHERE num_customers > (
            --Bring avg_all into WHERE clause
            SELECT avg_customers
            FROM avg_all
        )
)
SELECT co.country_name AS country_name,
    c.city_name AS city_name,
    fc.num_customers AS num_customers
FROM final_cities AS fc
    JOIN city AS c ON fc.city_id = c.id
    JOIN country AS co ON c.country_id = co.id
ORDER BY country_name ASC;