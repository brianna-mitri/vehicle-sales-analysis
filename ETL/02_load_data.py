# imports
import csv, io, os, psycopg2
from dotenv import load_dotenv
from psycopg2 import sql

# -------------------------------------
# Prepare variables
# -------------------------------------
dest_table = 'raw_orders_csv'
data_path = '../Resources/sales_data_sample.csv'

# pull credentials from .env
load_dotenv('../.env')

target_db = 'order_mgmt'
super_user = os.getenv('super_user')  #role allowed to create db
pg_password = os.getenv('pg_password')
host = os.getenv('host')
port = os.getenv('port')

target_dsn = f'dbname={target_db} user={super_user} password={pg_password} host={host} port={port}'

# -------------------------------------
# Load raw data into database
# -------------------------------------
def main() -> None:
    # connect to postgresql server
    conn = psycopg2.connect(target_dsn)
    cur = conn.cursor()

    with open(data_path, newline='', encoding='latin1') as fh:
        # read each row as a dict
        reader = csv.DictReader(fh)
        # get header names
        csv_cols = [c.lower() for c in reader.fieldnames]

        # put reordered rows in an in memory buffer ready for copy
        buf = io.StringIO()
        writer = csv.writer(buf, lineterminator='\n')
        for row in reader:
            writer.writerow([row.get(c.upper(), row.get(c)) for c in csv_cols])
        buf.seek(0)

        # bulk load
        copy_sql = sql.SQL(
            "COPY {tbl} ({cols}) FROM STDIN WITH (FORMAT CSV)"
        )
        cur.copy_expert(
            copy_sql.format(
                tbl = sql.Identifier(dest_table),
                cols = sql.SQL(',').join((map(sql.Identifier, csv_cols)))
            ),
            buf
        )

    # commit changes & close network
    conn.commit()
    cur.close()
    conn.close()

if __name__ == '__main__':
    main()