-- First create the necessary tables if they don't exist
CREATE OR REPLACE TABLE employees (
    employee_id INT,
    employee_name STRING,
    employee_details STRING  -- JSON string
);

CREATE OR REPLACE TABLE orders (
    order_id INT,
    order_data STRING  -- JSON string
);

-- Insert sample data
INSERT INTO employees (employee_id, employee_name, employee_details) VALUES
(1, 'John Doe', '{"department": "Sales", "salary": 50000}'),
(2, 'Jane Smith', '{"department": "IT", "salary": 60000}');

INSERT INTO orders (order_id, order_data) VALUES
(1, '{"customer": {"name": "ABC Corp"}, "items": [{"product": "Widget A", "price": 100}, {"product": "Widget B", "price": 200}]}'),
(2, '{"customer": {"name": "XYZ Inc"}, "items": [{"product": "Widget C", "price": 150}, {"product": "Widget D", "price": 300}]}');

-- The complex query
WITH employee_data AS (
    SELECT 
        employee_id,
        employee_name,
        PARSE_JSON(employee_details) AS employee_details_json
    FROM employees
),

order_data AS (
    SELECT 
        order_id,
        PARSE_JSON(order_data) AS order_data_json
    FROM orders
),

flattened_orders AS (
    SELECT 
        order_id,
        order_data_json:customer.name::STRING AS customer_name,
        item.value:product::STRING AS product,
        item.value:price::NUMBER AS price
    FROM order_data,
    LATERAL FLATTEN(INPUT => order_data_json:items) item
)

SELECT
    LISTAGG(employee_name, ', ') WITHIN GROUP (ORDER BY employee_id) AS employee_names,
    ARRAY_AGG(OBJECT_CONSTRUCT('id', employee_id,
                               'name', employee_name,
                               'details', employee_details_json)) AS employee_details_array,
    CAST(employee_id AS STRING) AS employee_id_str,
    ARRAY_AGG(OBJECT_CONSTRUCT('customer_name', customer_name,
                               'product', product,
                               'price', price)) AS order_details
FROM employee_data
CROSS JOIN flattened_orders
GROUP BY employee_id;