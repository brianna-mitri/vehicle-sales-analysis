# imports
import csv, io, os, psycopg2, pycountry
from pathlib import Path
from dotenv import load_dotenv
from psycopg2 import sql

# -------------------------------------
# Prepare variables
# -------------------------------------
# database to create
target_db = 'order_mgmt'

# paths
dest_table = 'raw_orders_csv'
raw_path = Path('../data/sales_data_sample.csv')
refresh_core_sql = Path('../db/02_refresh_core.sql')

# pull credentials from .env
load_dotenv('../.env')

target_dsn = f'''
dbname={target_db} 
user={os.getenv('super_user')} 
password={os.getenv('pg_password')} 
host={os.getenv('host')} 
port={os.getenv('port')}
'''
# -------------------------------------
# ISO country tables
# -------------------------------------
# define common aliases (alias: country_code)
aliases = [
    ('USA', 'USA'),
    ('UK', 'GBR')
]

# function that builds an in memory csv of aliases
def build_buf(rows, headers) -> io.StringIO:
    '''
    takes rows (list of tuples) and list of headers
    '''
    buf = io.StringIO()
    writer = csv.writer(buf, lineterminator='\n', quoting=csv.QUOTE_MINIMAL)
    
    # header
    writer.writerow(headers)
    
    # rows
    for r in rows:
        writer.writerow(r)
    buf.seek(0)
    
    # return
    return buf

# -------------------------------------
# Functions: load raw data into database
# -------------------------------------
# -------------------------- raw orders table --------------------------
def load_raw_orders(cur) -> int:
    '''
    Load new records for raw orders csv table
    '''
    # create empty staging table 
    cur.execute(sql.SQL(
            """
            CREATE TEMP TABLE stage AS
            SELECT * FROM {dest} LIMIT 0
            """
        ).format(dest=sql.Identifier(dest_table)))
    
    # copy data into staging
    with open(raw_path, newline='', encoding='latin-1') as fh:
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
        print(f"\t☑ Data copied into staging")

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
    
    # return new raw records count
    return cur.rowcount
    
# -------------------------- iso country codes/aliases tables --------------------------
def load_country_codes_tables(cur) -> None:
    '''
    Load iso_country_codes table ONCE &
    Load iso_country_aliases table
    '''
    # ------------- Load ISO country codes -------------
    # check if table is empty
    cur.execute("SELECT 1 FROM iso_country_codes LIMIT 1;")
    already_loaded = cur.fetchone() is not None

    # if table not filled already then load
    if not already_loaded:
        # create in memory csv of iso codes
        #iso_codes_dict = {country.alpha_3: country.name for country in pycountry.countries}
        iso_codes_rows = [(c.alpha_3, c.alpha_2, c.name) for c in pycountry.countries]
        iso_codes_buf = build_buf(iso_codes_rows, ['alpha3', 'alpha2', 'name'])

        # load
        cur.copy_expert(
            '''
            COPY iso_country_codes(alpha3, alpha2, name) 
            FROM STDIN CSV HEADER
            ''',
            iso_codes_buf
        )
        print(f'\t☑ ISO country codes table filled')
    else:
        print(f'\t☐ ISO country codes already present--> skipping...')
    
    # ------------- Load ISO country aliases -------------
    # create temp table
    cur.execute(
        '''
        CREATE TEMP TABLE alias_stage (LIKE iso_country_aliases)
        '''
    )

    # load into temp from in memory csv of aliases
    alias_buf = build_buf(aliases, ['alias', 'alpha3'])
    cur.copy_expert(
        '''
        COPY alias_stage(alias, alpha3)
        FROM STDIN CSV HEADER
        ''',
        alias_buf
    )
    cur.execute(
        '''
        INSERT INTO iso_country_aliases (alias, alpha3)
        SELECT alias, alpha3 FROM alias_stage
        ON CONFLICT (alias) DO NOTHING
        '''
    )
    
    print(f'\t☑ ISO country aliases table filled')

# -------------------------- apply core refresh sql --------------------------
def refresh_core_tables(cur) -> None:
    # execute text version of sql file
    sql_text = refresh_core_sql.read_text()
    cur.execute(sql_text)

# -------------------------- main driver --------------------------
def main():
    print(f"Starting connection to {target_db}....")
    try:
        # connect to postgresql server
        with psycopg2.connect(target_dsn) as conn:
            with conn.cursor() as cur:
                print(f"\t☑ Connected to database")

                # load raw table
                print("Load 1: load raw orders csv table")
                new_records = load_raw_orders(cur)
                print(f"\t☑ {new_records} new records loaded to {dest_table}")

                # load country iso/alias tables
                print("Load 2: prepare iso country codes/aliases tables")
                load_country_codes_tables(cur)
                print(f"\t☑ ISO country codes/aliases tables ready")

                # load core tables
                print("Load 3: load core tables")
                refresh_core_tables(cur)
                print(f"\t☑ Loaded core tables with the {new_records} new records")
            
            print(f"Finished loading data into {target_db}...")
            conn.commit()
            print(f"\t☑ Commited changes to database")
        print(f"\t☑ Database connection closed")

    except Exception as e:
        print(f"⚠ ERROR: data NOT loaded: {e}")
        raise

if __name__ == '__main__':
    main()