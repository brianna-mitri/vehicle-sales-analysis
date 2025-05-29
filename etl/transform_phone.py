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
    phone = raw_phone
    is_valid = False

    # try to validate
    try:
        parsed_phone = phonenumbers.parse(raw_phone, country)

        if is_valid_number(parsed_phone):
            phone = format_number(parsed_phone, PhoneNumberFormat.INTERNATIONAL)
            is_valid = True
    except NumberParseException:
        print('passing...')
        pass
    
    return {
        'phone': phone,
        'phone_valid': is_valid
    }


#################################################
# ---------- VALIDATE/UPDATE PHONE ------------ #
#################################################
# connect to db
with psycopg2.connect(dsn) as conn, conn.cursor() as cur:
    # -------------------------- get new customers' info --------------------------  
    # get last customer id 
    cur.execute(
        '''
        SELECT last_id FROM etl_watermark
        WHERE target = 'phone_val'
        '''
    )
    last_id = cur.fetchone()[0]

    # get newest records
    cur.execute(
    '''
    SELECT	c.customer_id,
		    c.phone,
		    c.phone_valid,
		    ic.alpha2
    FROM customers c
    JOIN addresses a ON c.customer_id = a.customer_id
    JOIN iso_country_codes ic ON a.country_code = ic.alpha3
    WHERE c.customer_id > %s
        AND c.phone_valid IS NULL
    ORDER BY c.customer_id
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
    for r in records:
        # set variables
        phone_info = dict(zip(fields, r))
        max_seen = max(max_seen,phone_info['customer_id'])
        raw_phone = phone_info['phone']
        phone_valid = phone_info['phone_valid']
        country = phone_info['alpha2']
        
        # validate phone
        print(f'\tChecking customer_id {phone_info['customer_id']}')

        parsed_dict = update_phone(raw_phone, country)
        phone = parsed_dict['phone']
        phone_valid = parsed_dict['phone_valid']

        try:
            # update phones
            cur.execute(
                '''
                UPDATE customers
                    SET phone       = %s,
                        phone_valid = %s
                WHERE customer_id = %s
                    AND phone_valid IS NULL
                ''', (
                    phone, phone_valid, phone_info['customer_id']
                )
            )
        except Exception as e:
            print(f'\n⚠ ERROR: {e}')
        else:
            conn.commit()

    # -------------------------- advance watermark --------------------------
    if max_seen > last_id:
        cur.execute(
            '''
            UPDATE etl_watermark
                SET last_id = %s,
                    updated_at = now()
                WHERE target = 'phone_val'
            ''', (max_seen,)
        )
        conn.commit()

print('\n☑ Updating/validating phone numbers done')