## Installation 

To install the package, you can use the following command:

```bash
pip install -r requirements.txt
```

## Usage

To run the package, you can use the following command:

```bash
python src/snowflake_executor.py src/sql_queries/Query-CM-SF.sql src/sql_queries/Query-SF-Array.sql src/sql_queries/Query-10-PL-SQL.sql snowflake
```

### Arguments
- The first argument is the path to the first SQL file.
- The second argument is the path to the second SQL file.
- The third argument is the path to the third SQL file.
- The fourth argument is the name of the database you want to connect to.

### Explanation
* You can you as many SQL files as you want, just make sure to add the path to the file as an argument.
* The database type can be either snowflake or databricks

