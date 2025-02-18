# Import required libraries
import psycopg2
import psycopg2.extras
import json
import csv
import math 


# Lets define some of the essentials
# We'll define these as global variables to keep it simple
username = "postgres"
password = "enter pwd here"
dbname = "assignment5"
host = "127.0.0.1"



def point_query(parent_partition_table_name, utc_val, save_table_name, connection):
    """
    Use this function to perform a point query on the given table. 
    The table input is either range (range_part) or round-roublin (rrobin_part) partitioned.
    The output should be saved in a table with the name "save_table_name".
    Make sure the ouptu is stored in asc order

    Args:
        parent_partition_table_name (str): The name of the table containing the partitions to be queried.
        utc_val (str): The UTC value to be queried.
        save_table_name (str): The name of the table where the output is to be saved.
        connection: The database connection object.
    """

    # TODO: Implement code to insert data here
    try:
        cursor = connection.cursor()
        #check if table already exists
        drop_table = f"DROP TABLE IF EXISTS {save_table_name}"
        cursor.execute(drop_table)

        #create the table with the title save_table_name and insert the data where value of utc_val = created_utc
        table_creation = f"""
            CREATE TABLE {save_table_name} AS
            SELECT * 
            FROM {parent_partition_table_name}
            WHERE created_utc = %s;
        """
        utc_value = (utc_val,)
        cursor.execute(table_creation, utc_value)

        #commit the changes
        connection.commit()

        cursor.close()
        
    except Exception as e:
        print(f"Error in the function point_query: {e}")
        raise e


def range_query(parent_partition_table_name, utc_min_val, utc_max_val, save_table_name, connection):
    """
    Use this function to perform a range query on the given table. 
    The table is either range (range_part) or round-roublin (rrobin_part) partitioned.
    The output should be saved in a table with the name "save_table_name".
    Make sure the ouptu is stored in asc order

    Args:
        parent_partition_table_name (str): The name of the table containing the partitions to be queried.
        utc_min_val (str): The minimum UTC value to be queried.
        utc_max_val (str): The maximum UTC value to be queried.
        save_table_name (str): The name of the table where the output is to be saved.
        connection: The database connection object.
    """

    # TODO: Implement code to insert data here
    try:
        cursor = connection.cursor()

        #drop the table if already exists
        drop_table = f"DROP TABLE IF EXISTS {save_table_name}"
        cursor.execute(drop_table)

        #creating the table with the table title save_table_name and consider the rows where created_utc values are greater than utc_min_val and less than or equal to utc_max_val and order the rows in ascending order by created_utc
        table_creation = f"""
            CREATE TABLE {save_table_name} AS
            SELECT * 
            FROM {parent_partition_table_name}
            WHERE created_utc > %s AND created_utc <= %s
            ORDER BY created_utc ASC;
        """
        utc_values = (utc_min_val, utc_max_val)
        cursor.execute(table_creation, utc_values)

        connection.commit()

        cursor.close()
    except Exception as e:
        print(f"Error in the function range_query: {e}")
        raise e
    
