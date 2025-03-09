/*
 Identifying Empty Departments
 
 
 Given two tables, playground.employees and playground.departments, with employees containing id, full_name, and department, and departments containing id (unique department ID) and dep_name (department name), write a SQL query to build a table with one column, dep_name. This table should list all the departments that currently have no employees, sorted by the department id.
 These are the tables to query for this question:
 playground.employees
 
 id int
 full_name string
 department int
 
 playground.departments
 
 id int
 dep_name string
 
 Your answer should include these columns:
 
 dep_name varchar
 */
SELECT dep_name
FROM playground.departments AS d
    LEFT JOIN playground.employees AS e ON d.id = e.department
WHERE e.id IS NULL
    /*
     SELECT
     dep_name
     FROM playground.departments AS d
     LEFT JOIN playground.employees AS e
     ON d.id = e.department
     GROUP BY dep_name
     HAVING COUNT(e.id) = 0
     */