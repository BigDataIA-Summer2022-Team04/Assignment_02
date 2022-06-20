import os
from google.cloud import bigquery
import json
import logging
from dotenv import load_dotenv
from fastapi import FastAPI
import pandas as pd

#Enable logging
load_dotenv()
LOGLEVEL = os.environ.get('LOGLEVEL', 'INFO').upper()
logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level='DEBUG',
    datefmt='%Y-%m-%d %H:%M:%S')


#################################################
# Author: Abhijit Kunjiraman
# Creation Date: 16-Jun-22
# Last Modified Date:17-Jun-22
# Change Logs:
# SL No         Date            Changes
# 1             17-Jun-22       Second Version
# 
#################################################
# Exit Codes:
# 101 - Cannot connect to Big Query Server
# 102 - Invalid User input quan
# 104 - Invalid SQL query
#################################################


"""
This funcation takes aircraft engine type and returns the type in string and registration details.
--------
input is type: int
------
returns
json
    1.  Records of flight registration details based on engine type
"""





def typengine(type:int):
    logging.info(f"Script Starts")
    try:
        client = bigquery.Client()
        logging.info(f"Connection established to Big Query Server")
    except Exception as e:
        logging.error(f"Check the path of the JSON file and contents")
        logging.error(f"Cannot connect to Big Query Server")
        return 101
    try:
        enginetype = client.query(f"""select distinct TYPE_ENGINE from `plane-detection-352701.SPY_PLANE.FAA_REGISTRATION-2022-06-13T00_09_43` order by 1""")
    except Exception as e:
        logging.error(f"Bad SQL Query, verify SQL for fetching surveillance planes mfg years")
        return 104

    resulttype = enginetype.result()
    listypes=[]
    for row in resulttype:
        listypes.append(row[0])
    if type in listypes:
        try:
            df = client.query(f"""SELECT N_NUMBER, SERIAL_NUMBER, a.NAME, a.MFR_MDL_CODE, ENG_MFR_MDL, TYPE_AIRCRAFT, TYPE_ENGINE, b.NAME FROM `plane-detection-352701.SPY_PLANE.FAA_REGISTRATION-2022-06-13T00_09_43` a
            join `plane-detection-352701.SPY_PLANE.TYPE_ENGINE`  b on cast(a.TYPE_ENGINE as string)= cast(b.ID as string)
            where a.TYPE_ENGINE={type}""").to_dataframe()
        except Exception as e:
            logging.error(f"Bad SQL Query, verify SQL for fetching surveillance data")
            return 104
    else:
        print('Please enter a valid engine type from this list ', listypes)
        return 102
    
    logging.info(f"Parsing dataframe into Json object")
    json_records = df.to_json(orient='records')
    parsed_records = json.loads(json_records)
    json.dumps(json_records, indent=4)
    logging.info(f"Returning Json objects")
    return  parsed_records


def exit_script(error_code: int = 0):
    logging.info(f"Script Ends")
    exit(error_code)

def main(type:int):
    data = typengine(type)
    if data in (101,102,104):
        exit_script(data)
    logging.debug("############## Printing Records          ##############")
    logging.debug(json.dumps(data[:4], indent=4, sort_keys=True))
    exit_script()

if __name__ == "__main__":
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.getenv('BQ_KEY_JSON')
    main()
