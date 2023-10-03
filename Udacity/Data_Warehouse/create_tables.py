from sql_queries import create_table_queries, drop_table_queries
from common import connect, run_queries

def main():
    conn = connect('dwh.cfg')
    cur = conn.cursor()
    print('Connection Success')
    # Drop tables if exist to start with clean environment
    run_queries(cur, conn, drop_table_queries)
    print('Dropped')
    # Create empty tables for landing and final tables
    run_queries(cur, conn, create_table_queries)

    print('Created')
    conn.close()


if __name__ == "__main__":
    main()
