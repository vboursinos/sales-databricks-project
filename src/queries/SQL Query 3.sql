CREATE OR REPLACE PROCEDURE process_data()
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
try {
    var result = snowflake.execute(
        `SELECT 
            LISTAGG(employee_name, ', ') WITHIN GROUP (ORDER BY employee_id) AS employee_names,
            TO_OBJECT(ARRAY_AGG(OBJECT_CONSTRUCT('id', employee_id, 'name', employee_name, 'details', PARSE_JSON(employee_details)))) AS employee_object,
            TRY_CAST(employee_id AS STRING) AS employee_id_str,
            ARRAY_AGG(OBJECT_CONSTRUCT('customer_name', order_data:customer.name, 'product', item.value:product, 'price', item.value:price)) AS order_details
         FROM employees, LATERAL FLATTEN(INPUT => PARSE_JSON(order_data):items) item`
    );

    var output = "";
    while (result.next()) {
        var employee_names = result.getColumnValue("employee_names");
        var employee_object = result.getColumnValue("employee_object");
        var employee_id_str = result.getColumnValue("employee_id_str");
        var order_details = result.getColumnValue("order_details");

        output += "Employee Names: " + employee_names + "\n";
        output += "Employee Object: " + JSON.stringify(employee_object) + "\n";
        output += "Employee ID as String: " + employee_id_str + "\n";
        output += "Order Details: " + JSON.stringify(order_details) + "\n";
    }

    return output;
} catch (err) {
    return "Error: " + err.message;
}
$$;


