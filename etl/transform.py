# -------------------------------------------
#   - uses ArcGIS REST API geocoding service
#   - updates new addresses with a score > 80
# -------------------------------------------

#################################################
# ------------------- SETUP ------------------- #
#################################################
# imports
import os, math, time, requests, pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from psycopg2.extras import execute_values
from bootstrap_dp import target_dsn

# set up api credentials
load_dotenv('../.env')
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
def geocode(record):
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
    print(query)

    # geocoding
    try:
        # make api request
        address_data = requests.get(url, params=query, timeout=10).json()
        print(address_data)
        
        candidate = address_data.get('candidates', [None])[0]
        time.sleep(0.1)
        return candidate

        # # update row
        # if candidate:
        #     attr = candidate['attributes']
        #     record.update({ 
        #         'st_addr':      attr['StAddr'],
        #         'sub_addr':     attr['SubAddr'], 
        #         'city':         attr['City'], 
        #         'region':       attr['Region'], 
        #         'postal_code':  attr['Postal'], 
        #         'country_code': attr['CountryCode'],
        #         'score':        attr['Score']
        #     })

    except Exception as e:
        print(f'Geocode fail: {e}')
        return None
    #return record