from fastapi import FastAPI,status,HTTPException

import fastapi
import uvicorn
import os
from google.cloud import bigquery
import json
import logging
from dotenv import load_dotenv
import pandas as pd
#from logfunc import logfunc

#Jui
#########################################################################################

#get_popular_engine_count
load_dotenv()
LOGLEVEL = os.environ.get('LOGLEVEL', 'INFO').upper()
logging.basicConfig(
format='%(asctime)s %(levelname)-8s %(message)s',
level=LOGLEVEL,
datefmt='%Y-%m-%d %H:%M:%S')
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.getenv('BQ_KEY_JSON')
app = FastAPI()

@app.get("/get_popular_engine_count",status_code=status.HTTP_200_OK)
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
        logging.error(f"Cannot connect to Big Query Server "+str(e))
        #logfunc(endpoint, 101)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,detail="Something went wrong")
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
        logging.error(f"Bad SQL Query, Please verify SQL" + str(e))
        #logfunc(endpoint, 104)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,detail="Something went wrong")
    if df.empty:
        logging.error(f"No rows returned from big query")
        #logfunc(endpoint, 103)
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,detail="No data found for given values")
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

    return parsed_records
    #else:
     #   logging.error(f"User passed state, {state_short} is invalid")
      #  logging.info(f"Please enter a valid US state short name")
       # return 102

# get_company_address
###########################################################################################

@app.get("/get_company_address",status_code=status.HTTP_200_OK)
# Defined functions
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
        #logfunc(endpoint, 101)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,detail="Something went wrong")
    formated_query = f"""SELECT spy.N_NUMBER, IFNULL(spy.NAME, " ") || ', ' || IFNULL(spy.STREET, " ") || ', ' || IFNULL(spy.STREET2, " ") || ', ' || IFNULL(spy.ZIP_CODE, " ") || ', ' || IFNULL(reg.NAME, " ") || ', ' || IFNULL(spy.COUNTRY, " ") as FULL_ADDRESS
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
        #logfunc(endpoint, 104)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,detail="Something went wrong")
    if df.empty:
        logging.error(f"No rows returned from big query")
        #logfunc(endpoint, 103)
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,detail="No data found for given values")

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


# flight_details_between_years
###########################################################################################

@app.get("/flight_details_between_years",status_code=status.HTTP_200_OK)
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
    if(start_date.isnumeric() and end_date.isnumeric()):
        if(len(start_date)==4 and len(end_date)==4):
            if(int(end_date)-int(start_date)>0):  
                pass
            else:
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,detail="Start year should be smaller than End Year")
        else:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,detail="Invalid input")
    else:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,detail="Invalid input")

     
    start_date=start_date+"-01-01"
    end_date=end_date+"-01-01"

    # if validate_state(state_short):
    logging.info(f"User passed dates are, {start_date} and {end_date} is valid")
    try:
        client = bigquery.Client()
        logging.info(f"Connection established to Big Query Server")
    except Exception as e:
        logging.error(f"Cannot connect to Big Query Server")
        #logfunc(endpoint, 101)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,detail="Something went wrong")
    formated_query = f"""
    SELECT N_NUMBER,NAME,CITY,STATE,COUNTRY,YEAR_MFR FROM `plane-detection-352701.SPY_PLANE.FAA` 
    WHERE YEAR_MFR BETWEEN '{start_date}' AND '{end_date}'
    """
    logging.info(f"Fetching data from big query")
    try:
        df = client.query(formated_query).to_dataframe()
    except Exception as e:
        logging.error(f"Bad SQL Query, Please verify SQL")
        #logfunc(endpoint, 104)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,detail="Something went wrong")
    if df.empty:
        logging.error(f"No rows returned from big query")
        #logfunc(endpoint, 103)
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,detail="No data found for given values")

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
    return parsed_records
    #else:
     #   logging.error(f"User passed state, {state_short} is invalid")
      #  logging.info(f"Please enter a valid US state short name")
       # return 102

def exit_script(error_code: int = 0):
    logging.info(f"Script Ends")
    exit(error_code)

def main(start_date: str = '1990-01-01', end_date:str = '1999-01-01'):
    logging.info(f"Script Starts")
    data = find_by_dates(start_date, end_date)
    if data in(101,102,103,104):
        exit_script(data)
    # logging.debug(f"Length of returned json object : {len(data)}")
    # logging.debug("############## Printing Aggregation data ##############")
    # logging.debug(json.dumps(data[0], indent=4, sort_keys=True))
    logging.debug("############## Printing Records          ##############")
    logging.debug(json.dumps(data[:100], indent=4, sort_keys=True))
    exit_script()

# Script Starts
if __name__ == "__main__":
    
    main()

