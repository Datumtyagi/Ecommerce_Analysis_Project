import pandas as pd
import mysql.connector
import os

# Mapping CSV files to their respective MySQL table names
csv_file_mappings = [
    ('customers.csv', 'customers'),
    ('orders.csv', 'orders'),
    ('sales.csv', 'sales'),
    ('products.csv', 'products'),
    ('delivery.csv', 'delivery'),
    ('payments.csv', 'payments')  # Included for tailored handling
]

# Establish a connection to the MySQL database
connection = mysql.connector.connect(
    host='your_host',
    user='your_username',
    password='your_password',
    database='your_database'
)
db_cursor = connection.cursor()

# Directory where CSV files are located
csv_directory = 'path_to_your_folder'

def determine_sql_type(dtype):
    if pd.api.types.is_integer_dtype(dtype):
        return 'INT'
    elif pd.api.types.is_float_dtype(dtype):
        return 'FLOAT'
    elif pd.api.types.is_bool_dtype(dtype):
        return 'BOOLEAN'
    elif pd.api.types.is_datetime64_any_dtype(dtype):
        return 'DATETIME'
    else:
        return 'TEXT'

for csv_file, table_name in csv_file_mappings:
    full_file_path = os.path.join(csv_directory, csv_file)
    
    # Load the CSV file into a DataFrame
    dataframe = pd.read_csv(full_file_path)
    
    # Handle NaN values by replacing them with None for SQL compatibility
    dataframe = dataframe.where(pd.notnull(dataframe), None)
    
    # For diagnostic purposes: Output NaN value counts before processing
    print(f"Currently processing: {csv_file}")
    print(f"NaN count per column before transformation:\n{dataframe.isnull().sum()}\n")

    # Sanitize column names for MySQL compatibility
    dataframe.columns = [col.replace(' ', '_').replace('-', '_').replace('.', '_') for col in dataframe.columns]

    # Construct the CREATE TABLE SQL statement with the appropriate column types
    columns_definition = ', '.join([f'`{col}` {determine_sql_type(dataframe[col].dtype)}' for col in dataframe.columns])
    create_table_sql = f'CREATE TABLE IF NOT EXISTS `{table_name}` ({columns_definition})'
    db_cursor.execute(create_table_sql)

    # Insert rows from the DataFrame into the MySQL table
    for _, record in dataframe.iterrows():
        # Convert record to tuple, ensuring NaN values are treated as None
        record_values = tuple(None if pd.isna(x) else x for x in record)
        insert_sql = f"INSERT INTO `{table_name}` ({', '.join(['`' + col + '`' for col in dataframe.columns])}) VALUES ({', '.join(['%s'] * len(record))})"
        db_cursor.execute(insert_sql, record_values)

    # Commit the transaction after processing each CSV file
    connection.commit()

# Terminate the database connection
connection.close()
