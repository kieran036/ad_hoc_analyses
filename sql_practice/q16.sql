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
WITH summary AS (
    SELECT MAX(salary) AS max_salary,
        MIN(salary) AS min_salary
    FROM playground.employees_salary
)
SELECT COALESCE(
        (
            SELECT SUM(salary)
            FROM playground.employees_salary
            WHERE salary = (
                    SELECT max_salary
                    FROM summary
                )
        ),
        0
    ) - COALESCE(
        (
            SELECT SUM(salary)
            FROM playground.employees_salary
            WHERE salary = (
                    SELECT min_salary
                    FROM summary
                )
        ),
        0
    ) AS difference;