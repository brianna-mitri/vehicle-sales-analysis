# imports
import csv, io, os, psycopg2
from dotenv import load_dotenv
from psycopg2 import sql

# -------------------------------------
# Prepare variables
# -------------------------------------
# check if first load
first_load = True

# paths
dest_table = 'raw_orders_csv'
raw_path = '../data/sales_data_sample.csv'
iso_codes_path = '../data/iso_codes.csv'

# pull credentials from .env
load_dotenv('../.env')

target_db = 'order_mgmt'
target_dsn = f'''
dbname={target_db} 
user={os.getenv('super_user')} 
password={os.getenv('pg_password')} 
host={os.getenv('host')} 
port={os.getenv('port')}
'''
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

        # ---------------------------------
        # load raw orders data
        # ---------------------------------
        with open(raw_path, newline='', encoding='latin1') as fh:
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

            print(f"☑ {new_records} new records loaded to {target_db}'s table: {dest_table}")

        # ---------------------------------
        # first load: load country codes
        # ---------------------------------
        if first_load:
            with open(iso_codes_path, 'r', encoding='utf-8') as f:
                cur.copy_expert(
                    '''
                    COPY iso_country_codes(alpha3, name) 
                    FROM STDIN CSV HEADER
                    ''',
                    f
                )
            print(f'☑ ISO country codes table filled')
        else:
            print(f'☐ ISO country codes table NOT filled--> skipping...')
        
        # commit changes and display
        conn.commit()


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