# -------------------------------------------
# 1) Update/validates phone numbers
#   - uses Google's libphonenumber library
#   - validates and reformats numbers
# 2) Updates/validates 
#   - uses ArcGIS REST API geocoding service
#   - updates new addresses with a score > 80
# -------------------------------------------

#################################################
# ------------------- SETUP ------------------- #
#################################################
# imports
import os, phonenumbers, time, requests, psycopg2, socket
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
# set up api credentials
token = os.getenv('access_token')
url = 'https://geocode-api.arcgis.com/arcgis/rest/services/World/GeocodeServer/findAddressCandidates'

out_fields = ','.join([
    'StAddr',
    'SubAddr',
    'City',
    'Region',
    'Postal',
    'CountryCode',
    'Status' #quality/diagnostics
])

score_threshold = 80  # threshold to update addresses


#################################################
# ----------------- FUNCTIONS ----------------- #
#################################################
# -------------------------- updating phone numbers --------------------------
def update_phone(raw_phone, country):
    '''
    returns international format (E.164 phone) or None, and is valid flag
    '''
    # initialize
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

# -------------------------- geocoding --------------------------
# function: empty string if null instance
def na_to_empty(field):
    return '' if field is None else field

# function: geocoding address
def geocode(record, id_no):
    '''
    return geocoded address if found or none
    '''
    # setup multifield query
    query = {
        'address':      na_to_empty(record['st_addr']),
        'address2':     na_to_empty(record['sub_addr']),
        'city':         na_to_empty(record['city']),
        'region':       na_to_empty(record['region']),
        'postal':       na_to_empty(record['postal_code']),
        'countryCode':  na_to_empty(record['country_code']),
        
        # base
        'f':            'pjson',
        'token':        token,
        'maxLocations': 1,
        'forStorage':   'false',
        'langCode':     'ENG',
        'outFields':    out_fields
    }
    
    # drop blanks from query
    query = {field: input for field, input in query.items() if input}

    # geocoding
    try:
        # make api request
        address_data = requests.get(url, params=query, timeout=10).json()
        print(f'Address ID {id_no}:')
        print(address_data)
        
        candidate = address_data.get('candidates', [None])[0]
        time.sleep(0.1)
        return candidate

    except Exception as e:
        print(f'\n⚠ GEOCODE ERROR: {e}')
        return None

#################################################
# ---------------- ETL DRIVER ----------------- #
#################################################
# connect to db
with psycopg2.connect(dsn) as conn, conn.cursor() as cur:
    # -----------------------------------------------
    # 1. PHONE VALIDATION
    # -----------------------------------------------
    
    # -------------------------- get new customers' info --------------------------  
    # get last customer id 
    cur.execute(
        '''
        SELECT last_id FROM etl_watermark
        WHERE target = 'phone_val'
        '''
    )
    last_cust_id = cur.fetchone()[0]

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
    ''', (last_cust_id,))
    
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
    max_cust_id = last_cust_id

    # -------------------------- update phone numbers --------------------------
    for r in records:
        # set variables
        phone_info = dict(zip(fields, r))
        max_cust_id = max(max_cust_id,phone_info['customer_id'])
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
    if max_cust_id > last_cust_id:
        cur.execute(
            '''
            UPDATE etl_watermark
                SET last_id = %s,
                    updated_at = now()
                WHERE target = 'phone_val'
            ''', (max_cust_id,)
        )
        conn.commit()
    # -----------------------------------------------
    # 2. GEOCODING
    # -----------------------------------------------

    # -------------------------- get new addresses --------------------------  
    # get last address id 
    cur.execute(
        '''
        SElECT last_id FROM etl_watermark
        WHERE target = 'addr_geocode'
        '''
    )
    last_addr_id = cur.fetchone()[0]

    # -------------------------- update swedish addresses --------------------------  
    print('Updating Swedish addresses...')
    cur.execute(
        r'''
        UPDATE addresses
        SET st_addr =
            regexp_replace(
                st_addr,
                '^(berguvs[^\s]*)(\s+.*)$',
                'Berguvsvägen\2',
                'i'
            ),
            city = 'Luleå'
        WHERE similarity(
                unaccent(lower(split_part(st_addr, ' ', 1))),
                unaccent('berguvsvagen')
            ) > 0.6
            AND country_code = 'SWE'
            AND address_id > %s
        ''', (last_addr_id,)
    )

    cur.execute(
        r'''
        UPDATE addresses
        SET st_addr =
            regexp_replace(
                st_addr,
                '^\?kerg[^\s]*(\s+.*)$',
                'Åkergatan\1',
                'i'
            ),
            city = 'Borås'
        WHERE similarity(
                unaccent(lower(split_part(st_addr, ' ', 1))),
                unaccent('akergatan')
            ) > 0.5
            AND country_code = 'SWE'
            AND address_id > %s
        ''', (last_addr_id,)
    )

    conn.commit()
    print('\t☑ Updated addresses')

    
    # -------------------------- geocode addresses --------------------------
    cur.execute(
    '''
    SELECT  address_id, customer_id,
            st_addr, sub_addr, city, region,
            postal_code, country_code, score
    FROM addresses
    WHERE address_id > %s
        AND score is NULL
    ORDER BY address_id
    ''', (last_addr_id,))
    
    records = cur.fetchall()
    fields = [field.name for field in cur.description]

    # display
    record_cnt = len(records)
    if record_cnt == 1:
        plural_or_no = 'address'
    elif record_cnt > 1:
        plural_or_no = 'addresses'
    else:
        plural_or_no = None

    print(f'\nGeocoding {record_cnt} {plural_or_no}...') if plural_or_no else print('\n☐ No addresses to geocode --> skipping...')

    # track high watermark
    max_addr_id = last_addr_id

    # -------------------------- geocode addresses --------------------------
    for r in records:
        address = dict(zip(fields, r))
        max_addr_id = max(max_addr_id, address['address_id'])

        cand = geocode(address, address['address_id'])
        
        # skip if no match or low score
        if not cand or cand['score'] < score_threshold:
            continue

        # update addresses that pass threshold
        try:
            # get new inputs (normalize empty fields)
            attr = cand['attributes']

            # skip if addressline 1 is null
            if not attr['StAddr']:
                continue

            # update addresses
            cur.execute(
                '''
                UPDATE addresses
                    SET st_addr         = NULLIF(%s, ''),
                        sub_addr        = NULLIF(%s, ''),
                        city            = NULLIF(%s, ''),
                        region          = NULLIF(%s, ''),
                        postal_code     = NULLIF(%s, ''),
                        country_code    = NULLIF(%s, ''),
                        score           = %s   
                    WHERE address_id    = %s
                        AND score IS NULL
                ''', (
                    attr['StAddr'], attr['SubAddr'], attr['City'], 
                    attr['Region'], attr['Postal'], attr['CountryCode'],
                    cand['score'], address['address_id']
                )
            )
        except psycopg2.Error as e:
            conn.rollback()
            print(f'\n⚠ POSTGRES ERROR: {e}')
        except (requests.exceptions.RequestException, socket.gaierror) as e:
            print(f'\n⚠ GEOCODE NETWORK ERROR: {e}')
        except Exception as e:
            print(f'\n⚠ ERROR: {e}')
        else:
            conn.commit()

    # -------------------------- advance watermark --------------------------
    if max_addr_id > last_addr_id:
        cur.execute(
            '''
            UPDATE etl_watermark
                SET last_id = %s,
                    updated_at = now()
                WHERE target = 'addr_geocode'
            ''', (max_addr_id,)
        )
        conn.commit()

print ('\n☑ Geocoding done')