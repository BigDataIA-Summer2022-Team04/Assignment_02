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
# 103 - Invalid User input years/ no rows returned because out of range of years.
# 104 - Invalid SQL query
#################################################
"""Takes quan and flights as input to give the records of plane models and types of planes along with number of times data has been collected of the planes taking a flight.
----------
quan : int
    indicates less than or more than
    0 indicates less than and 1 indicates more than
flights : int
    this returns the records with plane models which have taken more or less than the specified flights.
Returns
-------
json
    1.  Records of flight registration details
    2.  if entered year doesn't return any value or data related to that year does not exists the merely prints the years of which data is available from
        which the user can choose and enter appropriately."""






def noofflights(quan: int,flights: int):
    logging.info(f"Script Starts")
    if quan in (0,1):
        logging.info(f"Value of quan is valid, {quan} is valid")
        if quan == 1:
            try:
                client = bigquery.Client()
                logging.info(f"Connection established to Big Query Server")
            except Exception as e:
                logging.error(f"Check the path of the JSON file and contents")
                logging.error(f"Cannot connect to Big Query Server")
                return 101
            try:
                logging.info(f"Querying data from big query")
                df = client.query(f"""SELECT adshex, flights, type FROM `plane-detection-352701.SPY_PLANE.PLANE_FEATURES` 
                where flights > {flights} order by flights desc""").to_dataframe()
            except Exception as e:
                logging.error(f"Bad SQL Query, verify SQL for fetching data")
                return 104
            if df.empty:
                logging.error(f"No rows returned from big query")
                return 103
            logging.info(f"Parsing dataframe into Json object")
            json_records = df.to_json(orient='records')
            parsed_records = json.loads(json_records)
            json.dumps(json_records, indent=4)
            logging.info(f"Returning Json objects")
            return  parsed_records
        

        else:
            try:
                client = bigquery.Client()
                logging.info(f"Connection established to Big Query Server")
            except Exception as e:
                logging.error(f"Check the path of the JSON file and contents")
                logging.error(f"Cannot connect to Big Query Server")
                return 101

            try:
                logging.info(f"Querying data from big query")
                df = client.query(f"""SELECT adshex, flights, type FROM `plane-detection-352701.SPY_PLANE.PLANE_FEATURES` 
                where flights < {flights} order by flights desc""").to_dataframe()
            except Exception as e:
                logging.error(f"Bad SQL Query, verify SQL for fetching surveillance data")
                return 104

            if df.empty:
                logging.error(f"No rows returned from big query")
                return 103
            logging.info(f"Parsing dataframe into Json object")
            json_records = df.to_json(orient='records')
            parsed_records = json.loads(json_records)
            json.dumps(json_records, indent=4)
            logging.info(f"Returning Json objects")
            return  parsed_records
    else:
        print('please enter 0 or 1')
        return 102


def exit_script(error_code: int = 0):
    logging.info(f"Script Ends")
    exit(error_code)

def main(quan: int,flights: int):
    data = noofflights(quan, flights)
    if data in (101,102,103,104):
        exit_script(data)
    logging.debug("############## Printing Records          ##############")
    logging.debug(json.dumps(data[:4], indent=4, sort_keys=True))
    exit_script()

if __name__ == "__main__":
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.getenv('BQ_KEY_JSON')
    main()