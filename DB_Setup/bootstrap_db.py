# ----------------------------------------
# Python helper: 
#   - creates target db if missing
#   - executes schema.sql
# ----------------------------------------

# imports
import os, pathlib, psycopg2, psycopg2.sql as sql
from dotenv import load_dotenv
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

# ---------------------------------------------------------
# Load Credentials
# ---------------------------------------------------------
# pull credentials from .env
load_dotenv('../.env')

maintenance_db = os.getenv('maintenance_db')
super_user = os.getenv('super_user')  #role allowed to create db
pg_password = os.getenv('pg_password')
host = os.getenv('host')
port = os.getenv('port')

# build admin dsn --> role that is allowed to create db
admin_dsn = f'dbname={maintenance_db} user={super_user} password={pg_password} host={host} port={port}'

# target database to create
target_db = 'order_mgmt'

# read schema as text
schema = pathlib.Path('schema.sql').read_text()

# ---------------------------------------------------------
# Build Functions
# ---------------------------------------------------------

# function: create target db if it doesn't exist
def create_db():   
    # open connection to maintenance db
    conn = psycopg2.connect(admin_dsn)
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    
    # try to create db but skip if it exists
    try: 
        with conn.cursor() as cur:
            cur.execute(
                sql.SQL('CREATE DATABASE {}')
                .format(sql.Identifier(target_db))
            )
        print(f'☑ Created database: {target_db}')
    except psycopg2.errors.DuplicateDatabase:
        print(f'☐ Already created database: {target_db} --> skipping...')
    finally:
        conn.close()

# function: connect to the new db 
def apply_schema():
    # same credentials but replace database name
    target_dsn = admin_dsn.replace(maintenance_db, target_db, 1)

    # switches connection to target db
    with psycopg2.connect(target_dsn) as conn, conn.cursor() as cur:
        # conn.autocommit = False
        # apply schema if not already applied
        try:
            cur.execute(schema)
            conn.commit()
            print(f'☑ Schema applied to {target_db}')
        except psycopg2.errors.DuplicateTable:
            print(f'☐ Duplicate table detected--already applied schema --> skipping... ')

# ---------------------------------------------------------
# Run when functions executed
# ---------------------------------------------------------
if __name__ == '__main__':
    create_db()
    apply_schema()