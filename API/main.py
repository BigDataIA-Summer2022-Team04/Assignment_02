import os
import logging
import pandas as pd
import models
from database import engine, SessionLocal
from dotenv import load_dotenv
from routers import plot, data, users, authentication
from google.cloud import bigquery
from fastapi import FastAPI
from custom_functions import logfunc
from fastapi.staticfiles import StaticFiles
import json

#################################################
# Author: Jui, Abhijit, Piyush
# Creation Date: 23-Jun-22
# Last Modified Date:
# Change Logs:
# SL No         Date            Changes
# 1             23-Jun-22       First Version
# 
#################################################
# Exit Codes:
# 101 - 
# 102 - 
# 103 - 
# 104 - 
#################################################


#################################

load_dotenv()
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.getenv('BQ_KEY_JSON')
LOGLEVEL = os.environ.get('LOGLEVEL', 'INFO').upper()
logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=LOGLEVEL,
    datefmt='%Y-%m-%d %H:%M:%S')

#################################

app = FastAPI(title="Main App")

models.Base.metadata.create_all(bind=engine)

#################################
# Abhi
@app.get("/planes_info_from_mfg_year")
def queries( n : int, year : int):
    endpoint='/planes_info_from_mfg_year'

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
                logfunc(endpoint,101)
                return 101
            try:
                logging.info(f"Querying data from big query for surveillance years")
                queryyears = client.query(f"""SELECT distinct extract(year from a.year_mfr) FROM `plane-detection-352701.SPY_PLANE.FAA_REGISTRATION-2022-06-13T00_09_43` a
                left join `plane-detection-352701.SPY_PLANE.TRAIN` b on a.MODE_S_CODE_HEX=b.ADSHEX
                where b.class = 'surveil' and extract(year from a.year_mfr) is not Null order by 1""")
            except Exception as e:
                logging.error(f"Bad SQL Query, verify SQL for fetching surveillance planes mfg years")
                logfunc(endpoint,104)
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
                    logfunc(endpoint,104)
                    return 104

                if df.empty:
                    logging.error(f"No rows returned from big query")
                    logfunc(endpoint,103)
                    return 103

                logging.info(f"Parsing dataframe into Json object")
                json_records = df.to_json(orient='records')
                parsed_records = json.loads(json_records)
                json.dumps(json_records, indent=4)
                logging.info(f"Returning Json objects")
                logfunc(endpoint, 200)
                return  parsed_records
            else:
                print('No non surrveilance planes were manufactured in the year provided please select from the following years ', years)
                logfunc(endpoint,103)
                return 103

        else:
            try:
                client = bigquery.Client()
                logging.info(f"Connection established to Big Query Server")
            except Exception as e:
                logging.error(f"Check the path of the JSON file and contents")
                logging.error(f"Cannot connect to Big Query Server")
                logfunc(endpoint,101)
                return 101
            try:
                logging.info(f"Querying data from big query for surveillance years")
                queryyears = client.query(f"""SELECT distinct extract(year from a.year_mfr) FROM `plane-detection-352701.SPY_PLANE.FAA_REGISTRATION-2022-06-13T00_09_43` a
                left join `plane-detection-352701.SPY_PLANE.TRAIN` b on a.MODE_S_CODE_HEX=b.ADSHEX
                where b.class != 'surveil' and extract(year from a.year_mfr) is not Null order by 1""")
            except Exception as e:
                logging.error(f"Bad SQL Query, verify SQL for fetching years for non surveillance planes mfg years")
                logfunc(endpoint,104)
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
                    logfunc(endpoint,101)
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
                    logfunc(endpoint,103)
                    return 103

                logging.info(f"Parsing dataframe into Json object")
                json_records = df.to_json(orient='records')
                parsed_records = json.loads(json_records)
                json.dumps(json_records, indent=4)
                logging.info(f"Returning Json objects")
                logfunc(endpoint, 200)
                return  parsed_records
            else:
                print('No non surrveilance planes were manufactured in the year provided please select from the following years ', years)
                logfunc(endpoint,103)
                return 103
    else:
        print('please enter 0 for surveillance plane details or 1 for non-surveillance')
        logfunc(endpoint,102)
        return 102


@app.get("/number_of_flights")
def noofflights(quan: int,flights: int):
    endpoint="/number_of_flights"
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
                logfunc(endpoint,101)
                return 101
            try:
                logging.info(f"Querying data from big query")
                df = client.query(f"""SELECT adshex, flights, type FROM `plane-detection-352701.SPY_PLANE.PLANE_FEATURES` 
                where flights > {flights} order by flights desc""").to_dataframe()
            except Exception as e:
                logging.error(f"Bad SQL Query, verify SQL for fetching data")
                logfunc(endpoint,104)
                return 104
            if df.empty:
                logging.error(f"No rows returned from big query")
                logfunc(endpoint,103)
                return 103
            logging.info(f"Parsing dataframe into Json object")
            json_records = df.to_json(orient='records')
            parsed_records = json.loads(json_records)
            json.dumps(json_records, indent=4)
            logging.info(f"Returning Json objects")
            logfunc(endpoint, 200)
            return  parsed_records
        

        else:
            try:
                client = bigquery.Client()
                logging.info(f"Connection established to Big Query Server")
            except Exception as e:
                logging.error(f"Check the path of the JSON file and contents")
                logging.error(f"Cannot connect to Big Query Server")
                logfunc(endpoint,101)
                return 101

            try:
                logging.info(f"Querying data from big query")
                df = client.query(f"""SELECT adshex, flights, type FROM `plane-detection-352701.SPY_PLANE.PLANE_FEATURES` 
                where flights < {flights} order by flights desc""").to_dataframe()
            except Exception as e:
                logging.error(f"Bad SQL Query, verify SQL for fetching surveillance data")
                logfunc(endpoint,104)
                return 104

            if df.empty:
                logging.error(f"No rows returned from big query")
                logfunc(endpoint,103)
                return 103
            logging.info(f"Parsing dataframe into Json object")
            json_records = df.to_json(orient='records')
            parsed_records = json.loads(json_records)
            json.dumps(json_records, indent=4)
            logging.info(f"Returning Json objects")
            logfunc(endpoint, 200)
            return  parsed_records
    else:
        print('please enter 0 or 1')
        logfunc(endpoint,102)
        return 102


@app.get("/regdet_by_type_aircraft")
def typeaircraft(type:int):
    endpoint="/regdet_by_type_aircraft"
    logging.info(f"Script Starts")
    try:
        client = bigquery.Client()
        logging.info(f"Connection established to Big Query Server")
    except Exception as e:
        logging.error(f"Check the path of the JSON file and contents")
        logging.error(f"Cannot connect to Big Query Server")
        logfunc(endpoint,101)
        return 101
    try:
        aircrafttypes = client.query(f"""select distinct TYPE_AIRCRAFT from `plane-detection-352701.SPY_PLANE.FAA_REGISTRATION-2022-06-13T00_09_43` order by 1""")
    except Exception as e:
        logging.error(f"Bad SQL Query, verify SQL for fetching surveillance planes mfg years")
        logfunc(endpoint,104)
        return 104

    resulttype = aircrafttypes.result()
    listypes=[]
    for row in resulttype:
        listypes.append(row[0])
    if type in listypes:
        try:
            df = client.query(f"""SELECT N_NUMBER, SERIAL_NUMBER, a.NAME, a.MFR_MDL_CODE, ENG_MFR_MDL, TYPE_AIRCRAFT, TYPE_ENGINE, b.NAME FROM `plane-detection-352701.SPY_PLANE.FAA_REGISTRATION-2022-06-13T00_09_43` a
            join `plane-detection-352701.SPY_PLANE.TYPE_AIRCRAFT` b on cast(a.TYPE_AIRCRAFT as string)= b.ID
            where a.TYPE_AIRCRAFT={type}""").to_dataframe()
        except Exception as e:
            logging.error(f"Bad SQL Query, verify SQL for fetching surveillance data")
            logfunc(endpoint,104)
            return 104
    else:
        print('Please enter a valid aircraft type from this list ', listypes)
        logfunc(endpoint,102)
        return 102
    
    logging.info(f"Parsing dataframe into Json object")
    json_records = df.to_json(orient='records')
    parsed_records = json.loads(json_records)
    json.dumps(json_records, indent=4)
    logging.info(f"Returning Json objects")
    logfunc(endpoint, 200)
    return  parsed_records


@app.get("regdet_through_enginetype")
def typengine(type:int):
    endpoint="regdet_through_enginetype"
    logging.info(f"Script Starts")
    try:
        client = bigquery.Client()
        logging.info(f"Connection established to Big Query Server")
    except Exception as e:
        logging.error(f"Check the path of the JSON file and contents")
        logging.error(f"Cannot connect to Big Query Server")
        logfunc(endpoint,101)
        return 101
    try:
        enginetype = client.query(f"""select distinct TYPE_ENGINE from `plane-detection-352701.SPY_PLANE.FAA_REGISTRATION-2022-06-13T00_09_43` order by 1""")
    except Exception as e:
        logging.error(f"Bad SQL Query, verify SQL for fetching surveillance planes mfg years")
        logfunc(endpoint,104)
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
            logfunc(endpoint,104)
            return 104
    else:
        print('Please enter a valid engine type from this list ', listypes)
        logfunc(endpoint,102)
        return 102
    
    logging.info(f"Parsing dataframe into Json object")
    json_records = df.to_json(orient='records')
    parsed_records = json.loads(json_records)
    json.dumps(json_records, indent=4)
    logging.info(f"Returning Json objects")
    logfunc(endpoint, 200)
    return  parsed_records

#################################


#################################
# Jui
@app.get("/get_popular_engine_count")
def popular_engine():
    endpoint=('/get_popular_engine_count') 
    """Gets and returns the aggregate STATISTICS of flights and records
    Parameters
    ----------
    Returns
    -------
    json
        1. Aggregate of count of planes based on type of engine
        2. Records of flight_number, engine number, count of flights per engine
    """
    # if validate_state(state_short):
    # logging.info(f"User passed state, {N_NUMBER} is valid")
    try:
        client = bigquery.Client()
        logging.info(f"Connection established to Big Query Server")
    except Exception as e:
        logging.error(f"Check the path of the JSON file and contents")
        logging.error(f"Cannot connect to Big Query Server")
        logfunc(endpoint, 101)
        return 101
    formated_query = f"""
    WITH TOP_ENGINES AS (SELECT TYPE_ENGINE, COUNT(N_NUMBER) AS COUNT_ENGINE_TYPE FROM `plane-detection-352701.SPY_PLANE.FAA` 
GROUP BY 1),
ENGINE_NAME AS (SELECT ID, NAME FROM `plane-detection-352701.SPY_PLANE.TYPE_ENGINE`)
SELECT ENGINE_NAME.ID,ENGINE_NAME.NAME, TOP_ENGINES.COUNT_ENGINE_TYPE
FROM ENGINE_NAME
JOIN TOP_ENGINES ON TOP_ENGINES.TYPE_ENGINE = ENGINE_NAME.ID
ORDER BY COUNT_ENGINE_TYPE DESC
    """
    logging.info(f"Fetching data from big query")
    try:
        df = client.query(formated_query).to_dataframe()
    except Exception as e:
        logging.error(f"Bad SQL Query, Please verify SQL")
        logfunc(endpoint, 104)
        return 104
    if df.empty:
        logging.error(f"No rows returned from big query")
        logfunc(endpoint, 103)
        return 103
    # logging.info(f"Aggregating data from dataframe")
    # df2 = df.groupby(['TYPE_AIRCRAFT'])['TYPE_AIRCRAFT'].count().reset_index(name='count').sort_values(['count'], ascending=False) 
    # json_stats = df2.to_json(orient='records')
    logging.debug(df.head(10))
    # for row in df.iterrows():
    #     print(row)
        #print(df['COUNT_ENGINE_TYPE'][x])
    json_records = df.to_json(orient='records')
    logging.info(f"Parsing dataframe into Json object")
    # parsed_stats = json.loads(json_stats)
    parsed_records = json.loads(json_records)
    # json.dumps(parsed_stats, indent=4)
    json.dumps(json_records, indent=4)
    logging.info(f"Returning Json objects")
    logfunc(endpoint, 200)
    return parsed_records
    #else:
     #   logging.error(f"User passed state, {state_short} is invalid")
      #  logging.info(f"Please enter a valid US state short name")
       # return 102


@app.get("/get_company_address")
def find_address(N_NUMBER : str):
    endpoint=('/get_company_address')
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
        logfunc(endpoint, 101)
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
        logfunc(endpoint, 104)
        return 104
    if df.empty:
        logging.error(f"No rows returned from big query")
        logfunc(endpoint, 103)
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
    logfunc(endpoint, 200)
    return parsed_records
    #else:
     #   logging.error(f"User passed state, {state_short} is invalid")
      #  logging.info(f"Please enter a valid US state short name")
       # return 102


@app.get("/flight_details_between_years")
def find_by_dates(start_date, end_date):
    endpoint=('/flight_details_between_years') 
    """Gets and returns the records of aircrafts
    Parameters
    ----------
    start_date : str
        the start date
    end_date: str
        The end date 
    Returns
    -------
    json
        List of planes from particular start_date to end_date
        
    """
    # if validate_state(state_short):
    logging.info(f"User passed dates are, {start_date} and {end_date} is valid")
    try:
        client = bigquery.Client()
        logging.info(f"Connection established to Big Query Server")
    except Exception as e:
        logging.error(f"Check the path of the JSON file and contents")
        logging.error(f"Cannot connect to Big Query Server")
        logfunc(endpoint, 101)
        return 101
    formated_query = f"""
    SELECT N_NUMBER, YEAR_MFR FROM `plane-detection-352701.SPY_PLANE.FAA` 
    WHERE YEAR_MFR BETWEEN '{start_date}' AND '{end_date}'
    """
    logging.info(f"Fetching data from big query")
    try:
        df = client.query(formated_query).to_dataframe()
    except Exception as e:
        logging.error(f"Bad SQL Query, Please verify SQL")
        logfunc(endpoint, 104)
        return 104
    if df.empty:
        logging.error(f"No rows returned from big query")
        logfunc(endpoint, 103)
        return 103
    # logging.info(f"Aggregating data from dataframe")
    # df2 = df.groupby(['TYPE_AIRCRAFT'])['TYPE_AIRCRAFT'].count().reset_index(name='count').sort_values(['count'], ascending=False) 
    # json_stats = df2.to_json(orient='records')
    logging.debug(df.head(5))
    df['YEAR_MFR'] = df['YEAR_MFR'].apply(str)
    #df['YEAR_MFR'] = pd.to_datetime(df['YEAR_MFR'], format='%Y-%m-%d', errors='coerce')
    json_records = df.to_json(orient='records')
    logging.info(f"Parsing dataframe into Json object")
    # parsed_stats = json.loads(json_stats)
    parsed_records = json.loads(json_records)
    # json.dumps(parsed_stats, indent=4)
    json.dumps(json_records, indent=4)
    logging.info(f"Returning Json objects")
    logfunc(endpoint, 200)
    return parsed_records
    #else:
     #   logging.error(f"User passed state, {state_short} is invalid")
      #  logging.info(f"Please enter a valid US state short name")
       # return 102

#################################

#################################
# Piyush


app.include_router(plot.router)
app.include_router(data.router)
app.include_router(users.router)
app.include_router(authentication.router)
app.mount("/", StaticFiles(directory="ui", html=True), name="ui")
#################################



def exit_script(error_code: int):
    logging.info(f"Script Ends")
    exit(error_code)


def big_query_handshake(sample_query: str = r"SELECT YEAR_MFR FROM `plane-detection-352701.SPY_PLANE.FAA` LIMIT 5"):
    logging.info(f"Handshake | Connecting to Big-Query")
    if not os.path.isfile(os.getenv('BQ_KEY_JSON')):
        logging.error(f"User input file not found, Re-Verify the path {os.getenv('BQ_KEY_JSON')}")
        return 105
    try:
        client = bigquery.Client()
        logging.info(f"Handshake | Connection established to Big-Query")
    except Exception as e:
        logging.error(f"Handshake | Cannot establish Handshake with Big-Query. \nException: {e}")
        return 101
    logging.info(f"Handshake | Fetching data from Big-Query")
    try:
        df = client.query(sample_query).to_dataframe()
    except Exception as e:
        logging.error(f"Handshake | Bad SQL Query, Please Re-Verify SQL \nException: {e}")
        return 104
    if df.empty:
        logging.error(f"No rows returned from big query")
        return 103
    return 0


if __name__ == "__main__":
    handshake_return_code = big_query_handshake()
    if handshake_return_code in (101,102,103,104,105):
        logging.info(f"Handshake | Failed, Existing Script")
        exit_script(handshake_return_code)
    else:
        logging.info(f"Handshake | Success, Launching API Server")
        # uvicorn.run(app, host="127.0.0.1", port=9000)
