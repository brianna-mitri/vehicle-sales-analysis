# imports
import csv, io, os, psycopg2, pycountry
from dotenv import load_dotenv
from psycopg2 import sql

# -------------------------------------
# Prepare variables
# -------------------------------------
# database to create
target_db = 'order_mgmt'

# paths
dest_table = 'raw_orders_csv'
raw_path = '../data/sales_data_sample.csv'

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
aliases = {
    'USA': 'USA',
    'UK': 'GBR'
}

# function that builds an in memory csv of aliases
def build_buf(dict, col1='col1', col2='col2') -> io.StringIO:
    buf = io.StringIO()
    writer = csv.writer(buf, lineterminator='\n', quoting=csv.QUOTE_MINIMAL)
    
    # header
    writer.writerow([col1, col2])
    
    # rows
    for key, val in dict.items():
        writer.writerow([key, val])
    buf.seek(0)
    
    # return
    return buf

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
        # check if table is empty
        cur.execute("SELECT 1 FROM iso_country_codes LIMIT 1;")
        already_loaded = cur.fetchone() is not None

        # if table not filled already then load
        if not already_loaded:
            # ------------- Load ISO country codes -------------
            # create in memory csv of iso codes
            iso_codes_dict = {country.alpha_3: country.name for country in pycountry.countries}
            iso_codes_buf = build_buf(iso_codes_dict, col1='alpha3', col2='name')

            # load
            cur.copy_expert(
                '''
                COPY iso_country_codes(alpha3, name) 
                FROM STDIN CSV HEADER
                ''',
                iso_codes_buf
            )
            print(f'☑ ISO country codes table filled')

            # ------------- Load ISO country aliases -------------
            alias_buf = build_buf(aliases, col1='alias', col2='alpha3')
            cur.copy_expert(
                '''
                COPY iso_country_aliases(alias,alpha3)
                FROM STDIN CSV HEADER
                ''',
                alias_buf
            )
            print(f'☑ ISO country aliases table filled')
        else:
            print(f'☐ ISO country codes already present--> skipping...')
        
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