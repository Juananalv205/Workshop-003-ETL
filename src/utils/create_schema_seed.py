import pandas as pd
import numpy as np
class CreateSchemaSeed:
    def __init__(self) -> None:
        pass
    
    def infer_schema_postgres(self, df, table_name, file_path):
        """
        Function to infer the schema of a table from a Pandas DataFrame and generate the SQL script
        to create the table in PostgreSQL.

        Args:
        df (pd.DataFrame): Pandas DataFrame from which the schema will be inferred.
        table_name (str): Name of the table to be created.

        Returns:
        str: SQL script to create the table in PostgreSQL.
        """
        
        # Map pandas data types to PostgreSQL types
        type_mapping = {
            'int64': 'INTEGER',
            'float64': 'FLOAT',
            'object': 'TEXT',
            'bool': 'BOOLEAN',
            'datetime64[ns]': 'TIMESTAMP',
            'timedelta[ns]': 'INTERVAL'
        }
        try:
            # Start building the SQL script
            sql_script = f'CREATE TABLE "{table_name}" (\n'
            
            # Iterate through the DataFrame columns to generate columns and data types
            for col in df.columns:
                if col not in ['key', 'explicit']:
                    col_type = str(df[col].dtype)  # Get column data type
                    postgres_type = type_mapping.get(col_type, 'TEXT')  # Map the pandas data type to a PostgreSQL type; defaults to 'TEXT' if not found
                    sql_script += f'    "{col}" {postgres_type},\n'
                elif col == 'key':
                    col_type = str(df[col].dtype)
                    postgres_type = type_mapping.get(col_type, 'TEXT')
                    sql_script += f'    "key_column" {postgres_type},\n'
                else:
                    col_type = str(df[col].dtype)
                    postgres_type = type_mapping.get(col_type, 'TEXT')
                    sql_script += f'    "explicit_column" {postgres_type},\n'
            
            # Remove the last comma and add the closing parenthesis
            sql_script = sql_script.rstrip(",\n") + "\n);"
            
            # Save the script to the specified location
            with open(file_path, 'w', encoding='utf-8') as file:
                file.write(sql_script)
            
            return f"✓ The SQL script has been successfully saved to {file_path}"
        except Exception as e:
            return f"✗ An error occurred: {e}"

    def create_seed_postgres(self, df, table_name, file_path):
        """
        Function to generate an SQL script for inserting data into a PostgreSQL table from a Pandas DataFrame.

        Args:
        df (pd.DataFrame): Pandas DataFrame from which the insert script will be generated.
        table_name (str): Name of the table into which the data will be inserted.

        Returns:
        str: SQL script to insert data into the PostgreSQL table.
        """
        try:
            # Start building the SQL script
            sql_script = ""
            df = df.replace({np.nan: None})
            # Iterate through the rows of the DataFrame to generate the insert values
            for _, row in df.iterrows():
                # Create the value string for each row
                values = ', '.join([
                    "'" + str(val).replace('"', ' ').replace("'", ' ').replace(';', ' ') + "'" if isinstance(val, str) 
                    else f"{val:.6f}" if isinstance(val, float) and not val.is_integer() 
                    else str(int(val)) if isinstance(val, float) and val.is_integer() 
                    else 'NULL' if val is None
                    else str(val) 
                    for val in row.values
                ])
                sql_script += f'INSERT INTO "{table_name}" VALUES ({values});\n'

            # Save the script to the specified location with UTF-8 encoding
            with open(file_path, 'w', encoding='utf-8') as file:
                file.write(sql_script)
            
            return f"✓ The SQL script has been successfully saved to {file_path}"
        
        except Exception as e:
            return f"✗ An error occurred: {e}"

