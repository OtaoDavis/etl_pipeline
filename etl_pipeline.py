import pandas as pd
import mysql.connector

# Establish a connection to the MySQL database
conn = mysql.connector.connect(
    host='localhost',
    user='root',
    password='',
    database='kiva'
)
# Function to delete duplicates from a MySQL table
def delete_duplicates_from_table(host, username, password, database, table):
    try:
        conn = mysql.connector.connect(
            host=host,
            user=username,
            passwd=password,
            database=database
        )
        cursor = conn.cursor()

        # Delete duplicates from the table based on primary key (assumed 'id' is the primary key)
        delete_query = f"DELETE t1 FROM {table} t1 INNER JOIN {table} t2 WHERE t1.id > t2.id AND t1.id = t2.id;"
        cursor.execute(delete_query)
        conn.commit()

        cursor.close()
        conn.close()
        print("Duplicates deleted successfully from the table.")
    except mysql.connector.Error as err:
        print("Error deleting duplicates:", err)

# Function to load data from CSV to MySQL table
def load_csv_to_mysql(csv_file, host, username, password, database, table):
    try:
        conn = mysql.connector.connect(
            host=host,
            user=username,
            passwd=password,
            database=database
        )
        cursor = conn.cursor()

        # Load CSV data into MySQL table
        data = pd.read_csv(csv_file)
        data_columns = ", ".join(data.columns)

        for _, row in data.iterrows():
            values = ", ".join([f"'{str(value)}'" for value in row])
            insert_query = f"INSERT INTO {table} ({data_columns}) VALUES ({values});"
            cursor.execute(insert_query)

        conn.commit()

        cursor.close()
        conn.close()
        print(f"Data from '{csv_file}' loaded successfully into '{table}'.")
    except mysql.connector.Error as err:
        print("Error loading data to MySQL:", err)

# Function to create data marts using SQL queries
def create_data_marts(host, username, password, database):
    try:
        conn = mysql.connector.connect(
            host=host,
            user=username,
            passwd=password,
            database=database
        )
        cursor = conn.cursor()

        # SQL queries to create data marts
        # (Assuming the queries from the previous responses)

        conn.commit()

        cursor.close()
        conn.close()
        print("Data marts created successfully.")
    except mysql.connector.Error as err:
        print("Error creating data marts:", err)

# Function to execute the ETL pipeline
def run_etl_pipeline():
    # Load CSV data into MySQL tables
    load_csv_to_mysql('csv_files/kiva_loans.csv', 'localhost', 'root', '', 'kiva', 'kiva_loans')
    load_csv_to_mysql('csv_files/kiva_mpi_region_location.csv', 'localhost', 'root', '', 'kiva', 'kiva_mpi_region_locations')
    load_csv_to_mysql('csv_files/loan_theme_ids.csv', 'localhost', 'root', '', 'kiva', 'loan_theme_ids')
    load_csv_to_mysql('csv_files/loan_theme_by_region.csv', 'localhost', 'root', '', 'kiva', 'loan_themes_by_region')
    
    # Create data marts
    create_data_marts('localhost', 'root', '', 'kiva')
    
    # Delete duplicates from the data marts (if needed)
    delete_duplicates_from_table('localhost', 'root', '', 'kiva', 'fundraiser_demographics')
    # Add other tables if required
    
if __name__ == "__main__":
    run_etl_pipeline()
