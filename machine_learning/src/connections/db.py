import os
from dotenv import load_dotenv
import pandas as pd
import psycopg2

class PostgreSQLConnection:
    def __init__(self):
        load_dotenv(".env")
        self.user = os.getenv("DatabaseUserStaging")
        self.password = os.getenv("DatabasePasswordStaging")
        self.host = os.getenv("DatabaseHostStaging")
        self.port = os.getenv("DatabasePortStaging")
        self.database = os.getenv("DatabaseNameStaging")
        self.mydb = None
        self.mycursor = None

    def open_connection(self):
        try:
            self.mydb = psycopg2.connect(
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port,
                database=self.database
            )
            self.mycursor = self.mydb.cursor()
            return "Connected to PostgreSQL database!"
        except Exception as e:
            return f"Error connecting to PostgreSQL database: {e}"
    
    #Close connection to the database
    def close_connection(self):
        if self.mycursor:
            try:
                # Make sure to consume all unread results.
                self.mycursor.fetchall()
            except psycopg2.Error:
                pass  # Ignore errors if no results to consume
            self.mycursor.close()
        if self.mydb:
            self.mydb.close()
        return "Closed connection"
    
    def open_query(self, query_path, table_name=None):
        # Attempt to open the file and read the content
        with open(query_path, 'r', encoding='utf-8') as file:
            select_sql_script = file.read()
        # Replace {{table_name}} with the value of table_name only if it is not None
        if table_name is not None:
            query = select_sql_script.replace("{{table_name}}", table_name)
        else:
            query = select_sql_script  # Keeps the original query if table_name is None
        return query
        
    # Decorator defined inside the class
    def connection_decorator(func):
        def wrapper(self, *args, **kwargs):
            self.open_connection()  # Open the connection
            try:
                return func(self, *args, **kwargs)  # Call the original method
            finally:
                self.close_connection()  # Ensure the connection is closed
        return wrapper

    @connection_decorator
    def run_query(self, query, params=None):
        try:
            self.mycursor.execute(query, params) # Use the parameters here
            self.mydb.commit()
            return "✓ Query executed successfully."
        except psycopg2.Error as e:
            raise Exception(f"✗ Error executing query: {e}") 
    @connection_decorator
    #Run select query without commit
    def run_select_query(self, query, params=None):
        try:
            self.mycursor.execute(query, params)
            results = self.mycursor.fetchall()
            return results
        except psycopg2.Error as e:
            return f"✗ Error when executing the SELECT query:: {e}"

    
    @connection_decorator
    def insert_data_from_sql(self, sql_file_path):
        try:
            # Reuse the open_query function to read the content of the SQL file
            sql_script = self.open_query(sql_file_path)
            
            # Check if the connection is still open
            if self.mydb.closed != 0:
                print("Connection closed. Reopening...")
                self.open_connection()

            queries = sql_script.strip().split(';')  # Split the script into individual queries
            batch_size = 3000  # Define the batch size
            total_queries = len(queries) - 1  # Exclude the last empty query

            with self.mydb.cursor() as cursor:
                for i in range(0, total_queries, batch_size):
                    batch_queries = queries[i:i + batch_size]  # Select the batch of queries
                    batch_queries = [query.strip() for query in batch_queries if query.strip()]  # Clean up empty queries
                    
                    if batch_queries:  # Ensure there are queries in the batch
                        try:
                            for query in batch_queries:
                                cursor.execute(query)  # Execute each query in the batch
                            self.mydb.commit()  # Commit the changes
                            print(f"✓ Successfully inserted batch starting from query index {i}.")
                        except Exception as e:
                            print(f"✗ Error inserting batch starting from query index {i}: {e}")
                            self.mydb.rollback()  # Roll back the transaction in case of error
                            raise  # Raise the exception to stop execution

            return "✓ Data successfully inserted from SQL file."
        except psycopg2.OperationalError as e:
            print(f"✗ OperationalError: {e}")
            self.mydb.rollback()  # Roll back the transaction in case of error
            raise
        except psycopg2.InterfaceError as e:
            print(f"✗ InterfaceError: {e}")
            # Reopen the connection if it was closed unexpectedly
            self.open_connection()
            raise
        except Exception as e:
            print(f"✗ Unexpected error: {e}")
            raise
    
    @connection_decorator
    #Create dataframe from query
    def create_dataframe(self, query_path, table_name):
        try:
            select_sql_script = self.open_query(query_path, table_name)
            rows =  self.run_select_query(select_sql_script)# Get the results after executing the query
            colnames = [desc[0] for desc in self.mycursor.description]
            df = pd.DataFrame(rows, columns=colnames)
            print("✓ DataFrame created successfully.")
            return df
        except psycopg2.Error as e:
            return f"Error creating DataFrame: {e}"

