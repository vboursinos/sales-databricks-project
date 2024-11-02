-- models/complex_query.sql

WITH employee_data AS (
    SELECT 
        employee_id,
        employee_name,
        PARSE_JSON(employee_details) AS employee_details_json
    FROM {{ ref('employees') }}
),

order_data AS (
    SELECT 
        order_id,
        PARSE_JSON(order_data) AS order_data_json
    FROM {{ ref('orders') }}
),

flattened_orders AS (
    SELECT 
        order_id,
        order_data_json:customer.name AS customer_name,
        item.value:product AS product,
        item.value:price AS price
    FROM order_data,
    LATERAL FLATTEN(INPUT => order_data_json:items) item
)

SELECT 
    LISTAGG(employee_name, ', ') WITHIN GROUP (ORDER BY employee_id) AS employee_names,
    TO_OBJECT(ARRAY_AGG(OBJECT_CONSTRUCT('id', employee_id, 'name', employee_name, 'details', employee_details_json))) AS employee_object,
    TRY_CAST(employee_id AS STRING) AS employee_id_str,
    ARRAY_AGG(OBJECT_CONSTRUCT('customer_name', customer_name, 'product', product, 'price', price)) AS order_details
FROM employee_data, flattened_orders
GROUP BY employee_id;



# models/schema.yml

version: 2

models:
  - name: complex_query
    description: "A complex query using LISTAGG, PARSE_JSON, and FLATTEN"
    columns:
      - name: employee_names
        description: "Concatenated employee names"
      - name: employee_object
        description: "Employee details as an object"
      - name: employee_id_str
        description: "Employee ID as a string"
      - name: order_details
        description: "Order details as an array of objects"



dbt run --models complex_query   