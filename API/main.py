from fastapi import FastAPI
import uvicorn
import os
from google.cloud import bigquery
import json
import logging
from dotenv import load_dotenv
import pandas as pd
from validate_state import validate_state
from logfunc import logfunc

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



#################################


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
                return  parsed_records
            else:
                print('No non surrveilance planes were manufactured in the year provided please select from the following years ', years)
                logfunc(endpoint,103)
                return 103
    else:
        print('please enter 0 for surveillance plane details or 1 for non-surveillance')
        logfunc(endpoint,102)
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


####################################################################################
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
            return  parsed_records
    else:
        print('please enter 0 or 1')
        logfunc(endpoint,102)
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



#################################################################################
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
    return  parsed_records


def exit_script(error_code: int = 0):
    logging.info(f"Script Ends")
    exit(error_code)

def main(type:int):
    data = typeaircraft(type)
    if data in (101,102,104):
        exit_script(data)
    logging.debug("############## Printing Records          ##############")
    logging.debug(json.dumps(data[:4], indent=4, sort_keys=True))
    exit_script()




#########################################################################################
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

#################################

#################################
# Jui





#################################


# @app.get("/")
# async def root():
#     return {"message": "Hello World"}

if __name__ == '__main__':
    uvicorn.run(app, host="127.0.0.1", port=5000, log_level="info")

