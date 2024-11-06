-- Snowflake SQL Script
-- Create EMPLOYEES table
CREATE OR REPLACE TABLE EMPLOYEES (
    employee_id INT,
    manager_id INT,
    employee_name STRING
);

-- Insert dummy data into EMPLOYEES
INSERT INTO EMPLOYEES (employee_id, manager_id, employee_name) VALUES
    (1, NULL, 'Alice Johnson'),
    (2, 1, 'Bob Smith'),
    (3, 1, 'Charlie Brown'),
    (4, 2, 'David Wilson'),
    (5, 2, 'Eva Green'),
    (6, 3, 'Frank White');

-- Create ORDERS table
CREATE OR REPLACE TABLE ORDERS (
    order_id INT,
    json_data VARIANT
);

-- Fixed JSON data insertion using PARSE_JSON
INSERT INTO ORDERS (order_id, json_data)
SELECT 1, PARSE_JSON('{"customer": {"name": "John Doe"}, "order": {"amount": 150}}')
UNION ALL
SELECT 2, PARSE_JSON('{"customer": {"name": "Jane Smith"}, "order": {"amount": 50}}')
UNION ALL
SELECT 3, PARSE_JSON('{"customer": {"name": "Alice Johnson"}, "order": {"amount": 200}}')
UNION ALL
SELECT 4, PARSE_JSON('{"customer": {"name": "Bob Brown"}, "order": {"amount": 75}}');

-- Create MY_TABLE with Time Travel enabled
CREATE OR REPLACE TABLE MY_TABLE (
    id INT,
    data STRING,
    created_at TIMESTAMP
)
DATA_RETENTION_TIME_IN_DAYS = 1; -- Enable Time Travel with 1 day retention

-- Insert dummy data into MY_TABLE
INSERT INTO MY_TABLE (id, data, created_at) VALUES
    (1, 'Sample data 1', CURRENT_TIMESTAMP()),
    (2, 'Sample data 2', CURRENT_TIMESTAMP() - INTERVAL '1 hour'),
    (3, 'Sample data 3', CURRENT_TIMESTAMP() - INTERVAL '2 hours');

-- Verify the data
SELECT * FROM EMPLOYEES;
SELECT * FROM ORDERS;
SELECT * FROM MY_TABLE;

-- 1. Recursive CTE to retrieve employee hierarchy
WITH RECURSIVE employee_hierarchy AS (
    SELECT employee_id, manager_id, employee_name
    FROM EMPLOYEES
    WHERE manager_id IS NULL
    UNION ALL
    SELECT e.employee_id, e.manager_id, e.employee_name
    FROM EMPLOYEES e
    INNER JOIN employee_hierarchy eh ON e.manager_id = eh.employee_id
)
SELECT * FROM employee_hierarchy;

-- 2. Semi-structured Data Handling
-- Selecting customer name and order amount from JSON data
SELECT
    json_data:customer.name::STRING AS customer_name,
    json_data:order.amount::FLOAT AS order_amount
FROM ORDERS
WHERE json_data:order.amount::FLOAT > 100;

