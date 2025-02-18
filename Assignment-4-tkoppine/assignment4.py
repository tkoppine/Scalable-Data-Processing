# Import required libraries
# Do not install/import any additional libraries
import psycopg2
import psycopg2.extras
import json
import csv
import math 


# Lets define some of the essentials
# We'll define these as global variables to keep it simple
username = "postgres"
password = "postgres"
dbname = "assignment4"
host = "127.0.0.1"


def get_open_connection():
    """
    Connect to the database and return connection object
    
    Returns:
        connection: The database connection object.
    """

    return psycopg2.connect(f"dbname='{dbname}' user='{username}' host='{host}' password='{password}'")



def load_data(table_name, csv_path, connection, header_file):
    """
    Create a table with the given name and load data from the CSV file located at the given path.

    Args:
        table_name (str): The name of the table where data is to be loaded.
        csv_path (str): The path to the CSV file containing the data to be loaded.
        connection: The database connection object.
        header_file (str): The path to where the header file is located
    """

    cursor = connection.cursor()

    # Creating the table
    with open(header_file) as json_data:
        header_dict = json.load(json_data)

    table_rows_formatted = (", ".join(f"{header} {header_type}" for header, header_type in header_dict.items()))
    create_table_query = f'''
        CREATE TABLE IF NOT EXISTS {table_name} (
            {table_rows_formatted}
            )'''

    cursor.execute(create_table_query)
    connection.commit()


    # # TODO: Implement code to insert data here
    with open(csv_path, 'r') as file:
        cursor.copy_expert(f"COPY {table_name} FROM STDIN WITH CSV HEADER", file)
    connection.commit()




def range_partition(data_table_name, partition_table_name, num_partitions, header_file, column_to_partition, connection):
    """
    Use this function to partition the data in the given table using a range partitioning approach.

    Args:
        data_table_name (str): The name of the table that contains the data loaded during load_data phase.
        partition_table_name (str): The name of the table to be created for partitioning.
        num_partitions (int): The number of partitions to create.
        header_file (str): path to the header file that contains column headers and their data types
        column_to_partition (str): The column based on which we are creating the partition.
        connection: The database connection object.
    """

    # TODO: Implement code to perform range_partition here
    cursor = connection.cursor()

    with open(header_file) as json_data:
        header_dict = json.load(json_data)

    table_rows_formatted = (", ".join(f"{header} {header_type}" for header, header_type in header_dict.items()))

    parent_table_query = f'''
        CREATE TABLE IF NOT EXISTS {partition_table_name} (
            {table_rows_formatted}
        ) PARTITION BY RANGE ({column_to_partition});
    '''
    cursor.execute(parent_table_query)
    connection.commit()

    min_max_query = f"SELECT MIN({column_to_partition}), MAX({column_to_partition}) FROM {data_table_name}"
    cursor.execute(min_max_query)

    mini_value, maxi_value = cursor.fetchone()
    numerator = maxi_value - mini_value + 1
    range_size = math.ceil(numerator / num_partitions)

    for i in range(num_partitions):
        start_val = mini_value + (i * range_size)
        end_val = start_val + range_size
        partition_name = f"{partition_table_name}{i}"
        create_partition_query = f'''
            CREATE TABLE IF NOT EXISTS {partition_name} PARTITION OF {partition_table_name}
            FOR VALUES FROM ({start_val}) TO ({end_val});
        '''
        cursor.execute(create_partition_query)
    connection.commit()

    insert_query = f'''
        INSERT INTO {partition_table_name} 
        SELECT * FROM {data_table_name};
    '''
    cursor.execute(insert_query)
    connection.commit()



def round_robin_partition(data_table_name, partition_table_name, num_partitions, header_file, connection):
    """
    Use this function to partition the data in the given table using a round-robin approach.

    Args:
        data_table_name (str): The name of the table that contains the data loaded during load_data phase.
        partition_table_name (str): The name of the table to be created for partitioning.
        num_partitions (int): The number of partitions to create.
        header_file (str): path to the header file that contains column headers and their data types
        connection: The database connection object.
    """

    # TODO: Implement code to perform round_robin_partition here
    cursor = connection.cursor()

    with open(header_file) as json_data:
        header_dict = json.load(json_data)

    table_rows_formatted = (", ".join(f"{header} {header_type}" for header, header_type in header_dict.items()))

    parent_tabl_qury = f'''
        CREATE TABLE IF NOT EXISTS {partition_table_name} (
            {table_rows_formatted}
        );
    '''
    cursor.execute(parent_tabl_qury)
    connection.commit()

    for i in range(num_partitions):
        partition_name = f"{partition_table_name}{i}"
        child_table_creation = f'''
            CREATE TABLE IF NOT EXISTS {partition_name} (
                LIKE {partition_table_name} INCLUDING ALL
            ) INHERITS ({partition_table_name});
        '''
        cursor.execute(child_table_creation)
    connection.commit()

    drop_seq = f"DROP SEQUENCE IF EXISTS {partition_table_name}_seq"
    create_seq = f"CREATE SEQUENCE {partition_table_name}_seq START 0 INCREMENT 1 MINVALUE 0 MAXVALUE {num_partitions - 1} CYCLE"
    cursor.execute(drop_seq)
    cursor.execute(create_seq)
    connection.commit()

    round_robin_func = f'''
        CREATE OR REPLACE FUNCTION round_robin_insert() RETURNS TRIGGER AS $$
        DECLARE
            partition_idx INT;
            partition_name TEXT;
        BEGIN
            partition_idx := nextval('{partition_table_name}_seq');
            partition_name := '{partition_table_name}' || partition_idx;
            
            EXECUTE format('INSERT INTO %I VALUES ($1.*)', partition_name) USING NEW;
            
            RETURN NULL;
        END;
        $$ LANGUAGE plpgsql;
    '''
    cursor.execute(round_robin_func)

    trigger_qury = f'''
        CREATE TRIGGER round_robin_trigger
        BEFORE INSERT ON {partition_table_name}
        FOR EACH ROW EXECUTE FUNCTION round_robin_insert();
    '''
    cursor.execute(trigger_qury)
    connection.commit()

    insertion_qury = f"INSERT INTO {partition_table_name} SELECT * FROM {data_table_name}"
    cursor.execute(insertion_qury)
    connection.commit()

def delete_partitions(table_name, num_partitions, connection):
    """
    This function in NOT graded and for your own testing convinience.
    Use this function to delete all the partitions that are created by you.

    Args:
        table_name (str): The name of the table containing the partitions to be deleted.
        num_partitions (int): The number of partitions to be deleted.
        connection: The database connection object.
    """

    # TODO: UNGRADED: Implement code to delete partitions here
    cursor = connection.cursor()
    for i in range(num_partitions):
        cursor.execute(f"DROP TABLE IF EXISTS {table_name}{i}")
    connection.commit()