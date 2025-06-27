# imports
import os, json, psycopg2
from pathlib import Path
from dotenv import load_dotenv

# -------------------------------------
# 1) Prepare variables
# -------------------------------------
# ----- initial variables --------------------------------------------------
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
date_range = Path('../config/rfm_dates.json')

# ---- get date values --------------------------------------------
try:
    # get date values from config json file
    with open(date_range, 'r') as f:
          date_data = json.load(f)

    start_date = date_data.get('start_date')
    end_date = date_data.get('end_date')
except Exception as e:
    print(f"⚠ ERROR! RFM analysis date range not collected: {e}")
    raise

# -------------------------------------
# 2) Execute Load RFM SQL File
# -------------------------------------       
# ---- load rfm data into db ---------------------------------------
print(f"Starting to load rfm results to {target_db}....")
try:
    # get sql file commands then split based on pre copy and then start copy
    sql_text = open(sql_file).read()
    sql_pre_copy, sql_start_copy = sql_text.split('STEP 2', 1)

    # connect to db and read rfm data file
    with psycopg2.connect(dsn) as conn, conn.cursor() as cur:
        # ---- 1) store run_id in session --------------------
        cur.execute(sql_pre_copy, {
             'v_start_date': start_date,
             'v_end_date': end_date
        })
        print("☑ Run ID registered and session variable set")

        # ---- 2) copy rfm data into temp table and properly upload
        with open(rfm_csv, 'r', encoding='utf-8') as f:
            # import rfm data into db
            cur.copy_expert(sql_start_copy, f)
        conn.commit()
        print("☑ Completed loading.")

except Exception as e:
        print(f"⚠ ERROR! Data NOT loaded: {e}")
        raise