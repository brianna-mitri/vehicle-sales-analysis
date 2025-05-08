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
    conn = cur = None
    try:
        # connect to postgresql server
        conn = psycopg2.connect(target_dsn)
        cur = conn.cursor()
        print(f"☑ Connected to database")

        # create temp staging table with no constraints
        cur.execute(sql.SQL(
            """
            CREATE TEMP TABLE stage AS
            SELECT * FROM {dest} LIMIT 0
            """
        ).format(dest=sql.Identifier(dest_table)))
        print(f"☑ Staging table created")

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

            # copy into staging
            copy_sql = sql.SQL(
                "COPY stage ({cols}) FROM STDIN WITH (FORMAT CSV)"
            )
            cur.copy_expert(
                copy_sql.format(cols=sql.SQL(',').join(map(sql.Identifier, csv_cols))),
                buf
            )
            print(f"☑ Data copied into staging")

            # insert from staging into actual destination table
            insert_sql = sql.SQL(
                """
                INSERT INTO {dest} ({cols})
                SELECT {cols} FROM stage
                ON CONFLICT (ordernumber, orderlinenumber) DO NOTHING
                """
            ).format(
                dest = sql.Identifier(dest_table),
                cols = sql.SQL(',').join(map(sql.Identifier, csv_cols))
            )
            cur.execute(insert_sql)
            new_records = cur.rowcount

            # commit changes and display
            conn.commit()
            print(f"☑ {new_records} new records loaded to {target_db}'s table: {dest_table}")
    except (psycopg2.Error, OSError, csv.Error) as e:
        if conn:
            conn.rollback()
            print(f"☐ Data NOT loaded due to error: {e}")
            raise e
    finally:
        # close network
        if cur:
            cur.close()
        if conn:
            conn.close()
        print(f"☑ Database, {target_db}, connection closed")
        

if __name__ == '__main__':
    main()