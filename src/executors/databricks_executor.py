import os
from databricks import sql  # Importing Databricks SQL connector


class DatabricksExecutor:
    def __init__(self):
        """Initialize Databricks connection using environment variables"""
        self.connection = self._establish_connection()

    def _establish_connection(self):
        """Establish connection to Databricks using environment variables"""
        try:
            print(os.getenv('DATABRICKS_SERVER_HOSTNAME'))
            return sql.connect(
                server_hostname=os.getenv('DATABRICKS_SERVER_HOSTNAME'),
                http_path=os.getenv('DATABRICKS_HTTP_PATH'),
                access_token=os.getenv('DATABRICKS_ACCESS_TOKEN'),
            )
        except Exception as e:
            print(f"Connection error: {str(e)}")
            return None

    def execute_query(self, query, description=""):
        """Execute a single query and raise an exception on failure"""
        if not self.connection:
            raise ConnectionError("No connection established with Databricks.")

        cursor = None
        try:
            cursor = self.connection.cursor()
            print(f"Executing {description}...")
            cursor.execute(query)
            results = cursor.fetchall()
            print(f"Successfully executed {description}")
            return results
        except Exception as e:
            error_message = f"Error executing {description}: {str(e)}"
            print(error_message)
            # raise RuntimeError(error_message)  # Raise an exception instead of returning None
        finally:
            if cursor:
                cursor.close()

    def load_and_execute_sql_file(self, file_path):
        """Load and execute SQL file, raising an exception on any query failure"""
        try:
            with open(file_path, 'r') as file:
                sql_content = file.read()
            statements = sql_content.split(';')
            results = []
            for statement in statements:
                if statement.strip():
                    result = self.execute_query(
                        statement,
                        f"query from {os.path.basename(file_path)}"
                    )
                    results.append(result)
            return results
        except Exception as e:
            error_message = f"Error processing file {file_path}: {str(e)}"
            print(error_message)
            # raise RuntimeError(error_message)  # Raise an exception if file processing fails

    def execute_sql_files(self, sql_file_names):
        """Execute a list of SQL files, raising an exception if any file fails"""
        results = {}
        for sql_file in sql_file_names:
            print(f"\nProcessing {sql_file}...")
            file_results = self.load_and_execute_sql_file(sql_file)
            results[os.path.basename(sql_file)] = file_results
        return results
