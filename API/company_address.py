import os
import json
import logging
from dotenv import load_dotenv
from google.cloud import bigquery
#################################################
# Author: Jui Chavan
# Creation Date: 17-Jun-22
# Last Modified Date:
# Change Logs:
# SL No         Date            Changes
# 1             17-Jun-22       First Version
# 
#################################################
# Exit Codes:
# 101 - Cannot connect to Big Query Server
# 102 - Invalid User input
# 103 - No rows returned from big query
# 104 - Invalid SQL query
#################################################

#Enable logging
load_dotenv()
LOGLEVEL = os.environ.get('LOGLEVEL', 'INFO').upper()
logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=LOGLEVEL,
    datefmt='%Y-%m-%d %H:%M:%S')


# Defined functions
def find_address(N_NUMBER : str):
    """Gets and returns the records of comapny name with full address

    Parameters
    ----------
    state_short : str
        The short two-letter state and territory abbreviations
    Returns
    -------
    json
        1. Based on N_NUMBER get company deatails with full address
        2. Records of flight number, company name, street, street2, city, state, zip code, region, country
    """
    # if validate_state(state_short):
    logging.info(f"User passed state, {N_NUMBER} is valid")
    try:
        client = bigquery.Client()
        logging.info(f"Connection established to Big Query Server")
    except Exception as e:
        logging.error(f"Check the path of the JSON file and contents")
        logging.error(f"Cannot connect to Big Query Server")
        return 101
    formated_query = f"""SELECT 
spy.N_NUMBER,

IFNULL(spy.NAME, " ") || ', ' || IFNULL(spy.STREET, " ") || ', ' || IFNULL(spy.STREET2, " ") || ', ' || IFNULL(spy.ZIP_CODE, " ") || ', ' || IFNULL(reg.NAME, " ") || ', ' || IFNULL(spy.COUNTRY, " ") as FULL_ADDRESS
-- CONCAT(spy.NAME,", ", spy.STREET,", ", spy.STREET2,", ",spy.ZIP_CODE,", ",reg.NAME,", ",spy.COUNTRY) AS ADDRESS 
FROM `plane-detection-352701.SPY_PLANE.FAA` as spy
JOIN `plane-detection-352701.SPY_PLANE.REGION` as reg
ON REGION = reg.ID

JOIN `plane-detection-352701.SPY_PLANE.STATE_NAMES` as s
ON STATE = s.SHORT_NAME
WHERE N_NUMBER = '{N_NUMBER}'  
-- select * from `plane-detection-352701.SPY_PLANE.FAA` WHERE N_NUMBER = '1843F'
    """
    logging.info(f"Fetching data from big query")
    try:
        df = client.query(formated_query).to_dataframe()
    except Exception as e:
        logging.error(f"Bad SQL Query, Please verify SQL")
        return 104
    if df.empty:
        logging.error(f"No rows returned from big query")
        return 103
    # logging.info(f"Aggregating data from dataframe")
    # df2 = df.groupby(['TYPE_AIRCRAFT'])['TYPE_AIRCRAFT'].count().reset_index(name='count').sort_values(['count'], ascending=False) 
    # json_stats = df2.to_json(orient='records')
    logging.debug(df.head(5))
    json_records = df.to_json(orient='records')
    logging.info(f"Parsing dataframe into Json object")
    # parsed_stats = json.loads(json_stats)
    parsed_records = json.loads(json_records)
    # json.dumps(parsed_stats, indent=4)
    json.dumps(json_records, indent=4)
    logging.info(f"Returning Json objects")
    return parsed_records
    #else:
     #   logging.error(f"User passed state, {state_short} is invalid")
      #  logging.info(f"Please enter a valid US state short name")
       # return 102



def exit_script(error_code: int = 0):
    logging.info(f"Script Ends")
    exit(error_code)

def main(N_NUMBER: str = '1843F'):
    logging.info(f"Script Starts")
    data = find_address(N_NUMBER)
    if data in(101,102,103,104):
        exit_script(data)
    logging.debug(f"Length of returned json object : {len(data)}")
    # logging.debug("############## Printing Aggregation data ##############")
    # logging.debug(json.dumps(data[0], indent=4, sort_keys=True))
    logging.debug("############## Printing Records          ##############")
    logging.debug(json.dumps(data[0], indent=4, sort_keys=True))
    exit_script()

# Script Starts
if __name__ == "__main__":
    # load_dotenv()
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.getenv('BQ_KEY_JSON')
    main()