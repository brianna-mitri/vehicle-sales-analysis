# imports
import os, psycopg2
from pathlib import Path
from dotenv import load_dotenv


# ----- variables --------------------------------------------------
# setup dsn
target_db = 'order_mgmt'

load_dotenv('../.env')
dsn = f'''
dbname={target_db} 
user={os.getenv('super_user')} 
password={os.getenv('pg_password')} 
host={os.getenv('host')} 
port={os.getenv('port')}
'''

# set up paths
rfm_csv = Path('../data/derived/rfm_labels.csv')
sql_file = Path('../db/03_load_rfm.sql')


# ---- load rfm data into db ---------------------------------------
print(f"Starting to load rfm results to {target_db}....")
try:
    # get sql file commands
    sql_text = open(sql_file).read()

    # connect to db and read rfm data file
    with psycopg2.connect(dsn) as conn, conn.cursor() as cur, \
        open(rfm_csv, 'r', encoding='utf-8') as f:
        
        # import rfm data into db
        cur.copy_expert(sql_text, f)
        conn.commit()
    print("☑ Completed loading.")

except Exception as e:
        print(f"⚠ ERROR! Data NOT loaded: {e}")
        raise