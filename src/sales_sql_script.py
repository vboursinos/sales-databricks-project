import argparse

from dotenv import load_dotenv

from databricks_executor import DatabricksExecutor
from snowflake2_executor import SnowflakeExecutor


def main():
    # Set up command line argument parsing
    parser = argparse.ArgumentParser(description='Execute SQL scripts against a database.')
    parser.add_argument('sql_files', nargs='+', type=str, help='A list of SQL files to execute')
    parser.add_argument('database_type', type=str, help='Type of the database (e.g., snowflake or databricks)')

    args = parser.parse_args()

    # Load the appropriate environment file based on the database type
    if args.database_type.lower() == 'snowflake':
        load_dotenv('./src/snowflake.env')
        print("Loaded snowflake.env")
        executor = SnowflakeExecutor()
    elif args.database_type.lower() == 'databricks':
        load_dotenv('./src/databricks.env')
        print("Loaded databricks.env")
        executor = DatabricksExecutor()
    else:
        print("Unsupported database type. Please specify either 'Snowflake' or 'Databricks'.")
        return

    # Get the list of SQL file names from arguments
    sql_file_names = args.sql_files
    print(f"Executing SQL files: {', '.join(sql_file_names)} on database type: {args.database_type}")

    if executor.connection:
        try:
            # Execute the provided SQL files
            results = executor.execute_sql_files(sql_file_names)
            print("\nExecution Summary:")
            for file_name, file_results in results.items():
                if file_results:
                    print(f"{file_name}: Success")
                else:
                    print(f"{file_name}: Failed")
        finally:
            executor.connection.close()
            print("\nConnection closed")
    else:
        print("Failed to establish connection")


if __name__ == "__main__":
    main()