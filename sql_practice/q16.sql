/*
 Salary Range Difference
 
 
 Calculate the difference between the sum of the highest salaries and the sum of the lowest salaries in the company. The table "playground.employees_salary" contains columns "id" (unique employee ID), "name" (employee's name), and "salary" (employee's salary as a positive integer). The result should be a single column "difference" with one row representing the calculated difference. If the "playground.employees_salary" table is empty, "difference" should be 0.
 These are the tables to query for this question:
 playground.employees_salary
 
 id int
 name string
 salary int
 
 Your answer should include these columns:
 
 difference integer
 */
WITH t_avg_sal AS (
    SELECT AVG(salary) avg_sal
    FROM playground.employees_salary
),
diff AS(
    SELECT id,
        name,
        CAST(salary AS INT) AS salary,
        avg_sal,
CASE
            WHEN salary IS NULL THEN 0
            WHEN salary IS NOT NULL THEN salary - avg_sal
        END AS difference
    FROM playground.employees_salary as s
        CROSS JOIN t_avg_sal AS t
)
SELECT SUM(difference) AS difference
FROM diff