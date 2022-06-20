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
# 102 - Invalid User input n
# 103 - Invalid User input years
# 104 - Invalid SQL query
#################################################


def queries( n : int, year : int):

    """Takes n and mfgyear as input to give registration details of planes manufactured in the entered year, 
    The value of n specifies whether the data required is for surveillance or non surveillance planes 
    n=0 means data for surveillance planes and n=1 indicates data for non surveillance planes.
    ----------
    year : int
        the manufactured year
    Returns
    -------
    json
        1.  Records of flight registration details
        2.  if entered year doesn't return any value or data related to that year does not exists the merely prints the years of which data is available from
            which the user can choose and enter appropriately.
        """


    logging.info(f"Script Starts")
    if n in (0,1):
        logging.info(f"Value of n is valid, {n} is valid")
        if n==0:
            try:
                client = bigquery.Client()
                logging.info(f"Connection established to Big Query Server")
            except Exception as e:
                logging.error(f"Check the path of the JSON file and contents")
                logging.error(f"Cannot connect to Big Query Server")
                return 101
            try:
                logging.info(f"Querying data from big query for surveillance years")
                queryyears = client.query(f"""SELECT distinct extract(year from a.year_mfr) FROM `plane-detection-352701.SPY_PLANE.FAA_REGISTRATION-2022-06-13T00_09_43` a
                left join `plane-detection-352701.SPY_PLANE.TRAIN` b on a.MODE_S_CODE_HEX=b.ADSHEX
                where b.class = 'surveil' and extract(year from a.year_mfr) is not Null order by 1""")
            except Exception as e:
                logging.error(f"Bad SQL Query, verify SQL for fetching surveillance planes mfg years")
                return 104
            resultyear = queryyears.result()
            years=[]
            for mfgyear in resultyear:
                years.append(mfgyear[0])
            if year in years:
                try:
                    df = client.query(f"""SELECT * FROM `plane-detection-352701.SPY_PLANE.FAA_REGISTRATION-2022-06-13T00_09_43` a
                    left join `plane-detection-352701.SPY_PLANE.TRAIN` b on a.MODE_S_CODE_HEX=b.ADSHEX
                    where b.class = 'surveil' and extract(year from a.year_mfr)={year}""").to_dataframe()
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
                print('No non surrveilance planes were manufactured in the year provided please select from the following years ', years)
                return 103

        else:
            try:
                client = bigquery.Client()
                logging.info(f"Connection established to Big Query Server")
            except Exception as e:
                logging.error(f"Check the path of the JSON file and contents")
                logging.error(f"Cannot connect to Big Query Server")
                return 101
            try:
                logging.info(f"Querying data from big query for surveillance years")
                queryyears = client.query(f"""SELECT distinct extract(year from a.year_mfr) FROM `plane-detection-352701.SPY_PLANE.FAA_REGISTRATION-2022-06-13T00_09_43` a
                left join `plane-detection-352701.SPY_PLANE.TRAIN` b on a.MODE_S_CODE_HEX=b.ADSHEX
                where b.class != 'surveil' and extract(year from a.year_mfr) is not Null order by 1""")
            except Exception as e:
                logging.error(f"Bad SQL Query, verify SQL for fetching years for non surveillance planes mfg years")
                return 104
            resultyear = queryyears.result()
            years=[]
            for mfgyear in resultyear:
                years.append(mfgyear[0])
            if year in years:
                try:
                    client = bigquery.Client()
                    logging.info(f"Connection established to Big Query Server")
                except Exception as e:
                    logging.error(f"Check the path of the JSON file and contents")
                    return 101
                df = pd.DataFrame()

                try:
                    df = client.query(f"""SELECT * FROM `plane-detection-352701.SPY_PLANE.FAA_REGISTRATION-2022-06-13T00_09_43` a
                    left join `plane-detection-352701.SPY_PLANE.TRAIN` b on a.MODE_S_CODE_HEX=b.ADSHEX
                    where b.class != 'surveil' and extract(year from a.year_mfr)={year} limit 4""").to_dataframe()
                except Exception as e:
                    logging.error(f"Bad SQL Query, verify SQL for fetching non surveillance planes data")

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
                print('No non surrveilance planes were manufactured in the year provided please select from the following years ', years)
                return 103
    else:
        print('please enter 0 for surveillance plane details or 1 for non-surveillance')
        return 102

def exit_script(error_code: int = 0):
    logging.info(f"Script Ends")
    exit(error_code)

def main( n : int, year : int):
    data = queries(n, year)
    if data in (101,102,103,104,105,106):
        exit_script(data)
    logging.debug("############## Printing Records          ##############")
    logging.debug(json.dumps(data[:4], indent=4, sort_keys=True))
    exit_script()

if __name__ == "__main__":
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.getenv('BQ_KEY_JSON')
    main()