# -------------------------------------------
#   - uses Google's libphonenumber library
#   - validates and reformats numbers
# -------------------------------------------

#################################################
# ------------------- SETUP ------------------- #
#################################################
# imports
import os, math, time, requests, psycopg2, socket, phonenumbers
from dotenv import load_dotenv
from phonenumbers import NumberParseException, is_valid_number, format_number, PhoneNumberFormat  

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

#################################################
# ---------- UPDATING PHONE FUNCTION ---------- #
#################################################
def update_phone(raw_phone, country):
    '''
    takes phone number and alpha2 code and returns valid phone or none
    '''
    # try to parse number
    try:
        parsed_phone = phonenumbers.parse(raw_phone, country)
    except NumberParseException:
        return None
    
    # return reformatted number if valid
    if is_valid_number(parsed_phone):
        return format_number(parsed_phone, PhoneNumberFormat.INTERNATIONAL)
    else:
        return None


#################################################
# ---------- VALIDATE/UPDATE PHONE ------------ #
#################################################
# connect to db
with psycopg2.connect(dsn) as conn, conn.cursor() as cur:
    # -------------------------- get new customers' info --------------------------  
    # get last customer id 
    cur.execute(
        '''
        SElECT last_id FROM etl_watermark
        WHERE target = 'phone_val'
        '''
    )
    last_id = cur.fetchone()[0]

    # get newest records
    cur.execute(
    '''
    SELECT  customer_id, phone, phone_valid
    FROM customers
    WHERE customer_id > %s
        AND phone_valid is NULL
    ''', (last_id,))
    
    records = cur.fetchall()
    fields = [field.name for field in cur.description]

    # display
    record_cnt = len(records)
    if record_cnt == 1:
        plural_or_no = 'phone number'
    elif record_cnt > 1:
        plural_or_no = 'phone numbers'
    else:
        plural_or_no = None

    print(f'\nValidating/updating {record_cnt} {plural_or_no}...') if plural_or_no else print('\n☐ No phone numbers to check --> skipping...')

    # track high watermark
    max_seen = last_id

    # -------------------------- update phone numbers --------------------------


print(update_phone('2125557818', 'US'))
