

from sql_queries import copy_table_queries, insert_table_queries
from common import run_queries, connect
import csv

def main():
    conn = connect('dwh.cfg')
    cur = conn.cursor()

    # Load raw data into landing tables

    try:
        run_queries(cur, conn, copy_table_queries)
    except Exception as e:
        # Write log errors to file if failure
        qry = '''select top 25 * from stl_load_errors order by starttime desc'''
        conn.rollback()
        cur.execute(qry)

        data = cur.fetchall()
        cols = tuple(desc[0].strip() for desc in cur.description)

        with open('load_errors.csv','w') as out:
            csv_out = csv.writer(out)
            csv_out.writerow(cols)
            for row in data:
                csv_out.writerow(tuple(str(v).strip() for v in row))
        raise e

    # Create clean star schema data tables from raw data tables
    run_queries(cur, conn, insert_table_queries)

    conn.close()


if __name__ == "__main__":
    main()
