from fastapi import FastAPI
import uvicorn
import os
from google.cloud import bigquery
import json
import logging
from dotenv import load_dotenv
import pandas as pd
from validate_state import validate_state

#################################

load_dotenv()
LOGLEVEL = os.environ.get('LOGLEVEL', 'INFO').upper()
logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=LOGLEVEL,
    datefmt='%Y-%m-%d %H:%M:%S')
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.getenv('BQ_KEY_JSON')

#################################

app = FastAPI()



#################################
# Piyush
@app.get("/aircraft")
def type_aircraft(state_short: str):
    """Gets and returns the aggregate statistics's of flights and records

    Parameters
    ----------
    state_short : str
        The short two-letter state and territory abbreviations
    Returns
    -------
    json
        1. Aggregate of count of planes based on type of aircraft
        2. Records of flight number, serial number, state name, aircraft type
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
        ty.NAME as TYPE_AIRCRAFT
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
        df2 = df.groupby(['TYPE_AIRCRAFT'])['TYPE_AIRCRAFT'].count().reset_index(name='count').sort_values(['count'], ascending=False) 
        json_stats = df2.to_json(orient='records')
        json_records = df.to_json(orient='records')
        logging.info(f"Parsing dataframe into Json object")
        parsed_stats = json.loads(json_stats)
        parsed_records = json.loads(json_records)
        json.dumps(parsed_stats, indent=4)
        json.dumps(json_records, indent=4)
        logging.info(f"Returning Json objects")
        # return  parsed_stats, parsed_records
        return  parsed_stats
    else:
        logging.error(f"User passed state, {state_short} is invalid")
        logging.info(f"Please enter a valid US state short name")
        return 102



#################################


#################################
# Abhi



#################################

#################################
# Jui





#################################


# @app.get("/")
# async def root():
#     return {"message": "Hello World"}

if __name__ == '__main__':
    uvicorn.run(app, host="127.0.0.1", port=5000, log_level="info")

