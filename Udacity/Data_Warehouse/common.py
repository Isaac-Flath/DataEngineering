import csv

#config
import configparser
# DB Api
import psycopg2
# credential store backed by dynamodb
import credstash


def connect(config_fpath):
    """
    Establishes a connection to a PostgreSQL database using the configuration
    provided in the specified file.

    Parameters:
        config_fpath (str): The file path to the configuration file in INI format.
                            This file should contain necessary database connection
                            information.

    Returns:
        psycopg2.extensions.connection: A connection object to the PostgreSQL database.

    Usage:
        The configuration file should be in INI format with a section named 'CLUSTER'.

        If DB_PASSWORD is not provided, it will attempt to fetch from credstash.

        Example configuration file ('dwh.cfg'):
        [CLUSTER]
        HOST = your_database_host
        DB_NAME = your_database_name
        DB_USER = your_database_user
        DB_PORT = your_database_port
        DB_PASSWORD = your_database_password

        Example Usage:
        connection = connect('dwh.cfg')
        cursor = connection.cursor()
        # Perform database operations here
        cursor.close()
        connection.close()
    """

    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    config = config['CLUSTER']
    
    if 'DB_PASSWORD' in config:
        password = config['DB_PASSWORD']
    else:
        password = credstash.getSecret('aws.infrastructure.redshift_cluster_1.master.pass', 
                                     region='us-east-1',
                                     profile_name='isaac')

    # infrastructure as code for this is at github.com/isaac-flath/Infrastructure
    conn = psycopg2.connect(
                host = config['HOST'],
                dbname = config['DB_NAME'],
                user =  config['DB_USER'],
                port = config['DB_PORT'], 
                password=password
                )
    return conn


def run_queries(cur, conn, queries):
    """
    Execute a list of SQL queries on a database cursor and commit the changes.

    This function takes a database cursor (`cur`), a database connection (`conn`), 
    and a list of SQL queries (`queries`) and iterates through the queries. 
    Each query is executed on the provided cursor, and any changes are committed 
    to the database using the provided connection.

    Parameters:
    - cur (psycopg2.extensions.cursor): The database cursor to execute queries on.
    - conn (psycopg2.extensions.connection): The database connection to commit changes to.
    - queries (list of str): A list of SQL queries to be executed.

    Returns:
    None
    """
    for query in queries:
        print(query)
        cur.execute(query)
        conn.commit() 
