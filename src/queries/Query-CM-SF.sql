-- Assuming we have two tables: sales and customers

-- Create the sales table
CREATE OR REPLACE TABLE sales (
    sale_id INT,
    customer_id INT,
    sale_date DATE,
    sale_amount DECIMAL(10, 2),
    product_category STRING,
    sale_details STRING
);

-- Create the customers table
CREATE OR REPLACE TABLE customers (
    customer_id INT,
    customer_name STRING,
    customer_region STRING
);

-- Insert sample data into sales
INSERT INTO sales (sale_id, customer_id, sale_date, sale_amount, product_category, sale_details) VALUES
(1, 101, '2023-01-01', 100.00, 'Electronics', '{"warranty": "1 year", "color": "black"}'),
(2, 102, '2023-01-02', 200.00, 'Clothing', '{"size": "M", "color": "red"}'),
(3, 101, '2023-01-03', 150.00, 'Electronics', '{"warranty": "2 years", "color": "white"}'),
(4, 103, '2023-01-04', 300.00, 'Furniture', '{"material": "wood", "color": "brown"}'),
(5, 104, '2023-01-05', 250.00, 'Electronics', '{"warranty": "1 year", "color": "black"}');

-- Insert sample data into customers
INSERT INTO customers (customer_id, customer_name, customer_region) VALUES
(101, 'John Doe', 'North'),
(102, 'Jane Smith', 'South'),
(103, 'Alice Johnson', 'East'),
(104, 'Bob Brown', 'West');

-- Query to retrieve and manipulate data
SELECT
    c.customer_name,
    c.customer_region,
    s.product_category,
    s.sale_date,
    s.sale_amount,
    SUM(s.sale_amount) OVER (PARTITION BY c.customer_region ORDER BY s.sale_date) AS running_total,
    RANK() OVER (PARTITION BY c.customer_region ORDER BY s.sale_amount DESC) AS sales_rank,
    DATE_TRUNC('month', s.sale_date) AS sale_month,
    CONCAT(c.customer_name, ' - ', c.customer_region) AS customer_info,
    CASE
        WHEN s.sale_amount > 200 THEN 'High'
        WHEN s.sale_amount BETWEEN 100 AND 200 THEN 'Medium'
        ELSE 'Low'
    END AS sale_category,
    ARRAY_AGG(s.product_category) OVER (PARTITION BY c.customer_region ORDER BY s.sale_date) AS product_categories,
    PARSE_JSON(s.sale_details):warranty AS warranty_info
FROM
    sales s
JOIN
    customers c ON s.customer_id = c.customer_id
WHERE
    s.sale_date BETWEEN '2023-01-01' AND '2023-01-31'
ORDER BY
    c.customer_region, s.sale_date;


