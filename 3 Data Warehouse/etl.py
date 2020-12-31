import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Loads staging tables: copies data from S3 to staging tables on Redshift by executing an copy-query
    """
    
    print('>> loading staging tables...')
    for query in copy_table_queries:
        try:
            cur.execute(query)
            print('>>    ' + query[:50])
            print('   ...done.')
        except psycopg2.Error as e:
            print("Error with load_staging_tables: " + query)
            print (e)
        
        conn.commit()


def insert_tables(cur, conn):
    """
    Inserts data into final tables: copies data from staging tables into analytics tables on Redshift
    """
          
    print('>> inserting data into final tables...')
    for query in insert_table_queries:
        try:
            cur.execute(query)
            print('>>    ' + query[:50])
            print('   ...done.')
        except psycopg2.Error as e:
            print("Error with insert_table_queries: " + query)
            print (e)
        
        conn.commit()


def main():
    """
    - Establishes connection with the sparkify database with help of a config-file
    
    - Loads staging tables
    
    - Loads analytic tables from staging tables
    
    - Closes the connection
    """
    
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    try:
        conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    except psycopg2.Error as e: 
        print("Error: Could not make connection to the sparkify database")
        print(e)
    
    try:
        cur = conn.cursor()
    except psycopg2.Error as e: 
        print("Error: Could not get cursor to the Database")
        print(e)
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()