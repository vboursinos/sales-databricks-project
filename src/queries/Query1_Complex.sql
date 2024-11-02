-- 1. Recursive CTE
WITH RECURSIVE employee_hierarchy AS (
    SELECT employee_id, manager_id, employee_name
    FROM employees
    WHERE manager_id IS NULL
    UNION ALL
    SELECT e.employee_id, e.manager_id, e.employee_name
    FROM employees e
    INNER JOIN employee_hierarchy eh ON e.manager_id = eh.employee_id
)
SELECT * FROM employee_hierarchy;

-- 2. Semi-structured Data Handling
SELECT
    json_data:customer.name AS customer_name,
    json_data:order.amount AS order_amount
FROM orders
WHERE json_data:order.amount > 100;

-- 3. Time Travel
SELECT *
FROM my_table
AT (TIMESTAMP => '2023-01-01 00:00:00');
