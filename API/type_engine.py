import os
import json
import logging
from dotenv import load_dotenv
from google.cloud import bigquery
from validate_state import validate_state


#################################################
# Author: Piyush
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
def type_engine(state_short: str):
    """Gets and returns the aggregate statistics's of flights and records

    Parameters
    ----------
    state_short : str
        The short two-letter state and territory abbreviations
    Returns
    -------
    json
        1. Aggregate of count of planes based on type of engine
        2. Records of flight number, serial number, state name, engine type
    """
    if validate_state(state_short):
        logging.info(f"User passed state, {state_short} is valid")
        try:
            client = bigquery.Client()
            logging.info(f"Connection established to Big Query Server")
        except Exception as e:
            logging.error(f"Check the path of the JSON file and contents")
            logging.error(f"Cannot connect to Big Query Server")
            return 101
        formated_query = f"""WITH faa AS (
        SELECT N_NUMBER, SERIAL_NUMBER, STATE, YEAR_MFR, TYPE_REGISTRANT, TYPE_AIRCRAFT, TYPE_ENGINE, STATUS_CODE
        FROM `plane-detection-352701.SPY_PLANE.FAA` faa
        WHERE COUNTRY = 'US' 
        AND STATE = '{state_short}'
        ),
        reg AS (
        SELECT ID, NAME FROM `plane-detection-352701.SPY_PLANE.TYPE_REGISTRANT`
        ),
        type AS (
        SELECT ID, NAME FROM `plane-detection-352701.SPY_PLANE.TYPE_AIRCRAFT` 
        ),
        engine AS (
        SELECT ID, NAME FROM `plane-detection-352701.SPY_PLANE.TYPE_ENGINE` 
        ),
        registration AS (
        SELECT ID, NAME FROM `plane-detection-352701.SPY_PLANE.STATUS_CODE` 
        )


        SELECT 
        t.N_NUMBER,
        t.SERIAL_NUMBER,
        t.STATE,
        e.NAME as TYPE_ENGINE
        FROM faa AS t
        JOIN reg AS r ON t.TYPE_REGISTRANT = r.ID
        JOIN type AS ty ON CAST(t.TYPE_AIRCRAFT as STRING) = ty.ID
        JOIN engine AS e ON t.TYPE_ENGINE = e.ID
        JOIN registration AS re ON CAST(t.STATUS_CODE as STRING) = re.ID
        -- LIMIT 2
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
        logging.info(f"Aggregating data from dataframe")
        df2 = df.groupby(['TYPE_ENGINE'])['TYPE_ENGINE'].count().reset_index(name='count').sort_values(['count'], ascending=False) 
        json_stats = df2.to_json(orient='records')
        json_records = df.to_json(orient='records')
        logging.info(f"Parsing dataframe into Json object")
        parsed_stats = json.loads(json_stats)
        parsed_records = json.loads(json_records)
        json.dumps(parsed_stats, indent=4)
        json.dumps(json_records, indent=4)
        logging.info(f"Returning Json objects")
        return  parsed_stats, parsed_records
    else:
        logging.error(f"User passed state, {state_short} is invalid")
        logging.info(f"Please enter a valid US state short name")
        return 102


def exit_script(error_code: int = 0):
    logging.info(f"Script Ends")
    exit(error_code)


def main(state_code: str = 'MA'):
    logging.info(f"Script Starts")
    data = type_engine(state_code)
    if data in(101,102,103,104):
        exit_script(data)  
    logging.debug(f"Length of returned json object : {len(data)}")
    logging.debug("############## Printing Aggregation data ##############")
    logging.debug(json.dumps(data[0], indent=4, sort_keys=True))
    logging.debug("############## Printing Records          ##############")
    logging.debug(json.dumps(data[1][:5], indent=4, sort_keys=True))
    exit_script()

# Script Starts
if __name__ == "__main__":
    # load_dotenv()
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.getenv('BQ_KEY_JSON')
    main()