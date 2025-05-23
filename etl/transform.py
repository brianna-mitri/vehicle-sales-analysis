# -------------------------------------------
#   - uses ArcGIS REST API geocoding service
#   - updates new addresses with a score > 80
# -------------------------------------------

#################################################
# ------------------- SETUP ------------------- #
#################################################
# imports
import os, math, time, requests, psycopg2, socket
from dotenv import load_dotenv


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
    #'Subregion',
    'Postal',
    #'Country',
    #'CntryName',
    'CountryCode',
    'Status'
    #'Addr_type', 'Match_addr', 'Status'  #quality/diagnostics
])

# threshold to update addresses
score_threshold = 80 


#################################################
# ------------ GEOCODING FUNCTION ------------- #
#################################################
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
# ------------- UPDATE ADDRESSES -------------- #
#################################################
# connect to db
with psycopg2.connect(dsn) as conn, conn.cursor() as cur:
    # -------------------------- get new addresses --------------------------  
    # get last address id 
    cur.execute(
        '''
        SElECT last_id FROM etl_watermark
        WHERE target = 'addr_geocode'
        '''
    )
    last_id = cur.fetchone()[0]

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
        ''', (last_id,)
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
        ''', (last_id,)
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
    ''', (last_id,))
    
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
    max_seen = last_id

    # -------------------------- geocode addresses --------------------------
    for r in records:
        address = dict(zip(fields, r))
        max_seen = max(max_seen, address['address_id'])

        cand = geocode(address, max_seen)
        
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
    if max_seen > last_id:
        cur.execute(
            '''
            UPDATE etl_watermark
                SET last_id = %s,
                    updated_at = now()
                WHERE target = 'addr_geocode'
            ''', (max_seen,)
        )
        conn.commit()

print ('\n☑ Geocoding done')