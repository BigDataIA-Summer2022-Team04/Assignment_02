
import logging
from typing import Union
from google.cloud import bigquery
from custom_functions import validate_state, logfunc
from fastapi import APIRouter, HTTPException, Response, status, Query




router = APIRouter(
    prefix="/data",
    tags=['Data']
)
# /Applications/Python 3.9/Python Launcher.app

@router.get('/registrant', status_code=status.HTTP_200_OK)
async def get_registrant(user_list: Union[list[str], None] = Query(default=None), if_records: bool = False):
    if not user_list:
        logfunc("/data/registrant", 400)
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,detail="User Input is NULL" )
    filtered_state = []
    for states in user_list:
        if validate_state(states):
            filtered_state.append(states.upper())
    state_code = "', '".join(filtered_state)
    if not filtered_state:
        logfunc("/data/registrant", 400)
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,detail="User Input is Invalid, Verify the Inputs")
    else:
        logging.info(f"User passed state, {filtered_state} is valid")
        try:
            client = bigquery.Client()
            logging.info(f"Connection established to Big Query Server")
        except Exception as e:
            logging.error(f"Check the path of the JSON file and contents")
            logging.error(f"Cannot connect to Big Query Server")
            logfunc("/data/registrant", 500)
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)
            # return 101
        formated_query = f"""WITH faa AS (
        SELECT N_NUMBER, SERIAL_NUMBER, STATE, YEAR_MFR, TYPE_REGISTRANT, TYPE_AIRCRAFT, TYPE_ENGINE, STATUS_CODE
        FROM `plane-detection-352701.SPY_PLANE.FAA` faa
        WHERE COUNTRY = 'US' 
        -- AND STATE = '{state_code}'
        AND STATE IN ('{state_code}')
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
        r.NAME as TYPE_REGISTRANT
        FROM faa AS t
        JOIN reg AS r ON t.TYPE_REGISTRANT = r.ID
        JOIN type AS ty ON CAST(t.TYPE_AIRCRAFT as STRING) = ty.ID
        JOIN engine AS e ON t.TYPE_ENGINE = e.ID
        JOIN registration AS re ON CAST(t.STATUS_CODE as STRING) = re.ID
        """
        logging.info(f"Fetching data from big query")
        try:
            df = client.query(formated_query).to_dataframe()
        except Exception as e:
            logging.error(f"Bad SQL Query, Please verify SQL")
            logfunc("/data/registrant", 500)
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)
        if df.empty:
            logging.error(f"No rows returned from big query")
            logfunc("/data/registrant", 204)
            raise HTTPException(status_code=status.HTTP_204_NO_CONTENT)
            # return 103
        if if_records:
            logfunc("/data/registrant", 200)
            return Response(df.to_json(orient="records"), media_type="application/json")
        logging.info(f"Aggregating data from dataframe")
        df2 = df.groupby(['TYPE_REGISTRANT'])['TYPE_REGISTRANT'].count().reset_index(name='count').sort_values(['count'], ascending=False) 
        logging.info(f"Returning dataframe as JSON")
        logfunc("/data/registrant", 200)
        return Response(df2.to_json(orient="records"), media_type="application/json")


@router.get('/aircraft', status_code=status.HTTP_200_OK)
async def get_aircraft(user_list: Union[list[str], None] = Query(default=None), if_records: bool = False):
    if not user_list:
        logfunc("/data/aircraft", 400)
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,detail="User Input is NULL" )
    filtered_state = []
    for states in user_list:
        if validate_state(states):
            filtered_state.append(states.upper())
    state_code = "', '".join(filtered_state)
    if not filtered_state:
        logfunc("/data/aircraft", 400)
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,detail="User Input is Invalid, Verify the Inputs")
    else:
        logging.info(f"User passed state, {state_code} is valid")
        try:
            client = bigquery.Client()
            logging.info(f"Connection established to Big Query Server")
        except Exception as e:
            logging.error(f"Check the path of the JSON file and contents")
            logging.error(f"Cannot connect to Big Query Server")
            logfunc("/data/aircraft", 500)
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)
            # return 101
        formated_query = f"""WITH faa AS (
        SELECT N_NUMBER, SERIAL_NUMBER, STATE, YEAR_MFR, TYPE_REGISTRANT, TYPE_AIRCRAFT, TYPE_ENGINE, STATUS_CODE
        FROM `plane-detection-352701.SPY_PLANE.FAA` faa
        WHERE COUNTRY = 'US' 
        -- AND STATE = '{state_code}'
        AND STATE IN ('{state_code}')
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
        """
        logging.info(f"Fetching data from big query")
        try:
            df = client.query(formated_query).to_dataframe()
        except Exception as e:
            logging.error(f"Bad SQL Query, Please verify SQL")
            logfunc("/data/aircraft", 500)
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)
            # return 104
        if df.empty:
            logging.error(f"No rows returned from big query")
            logfunc("/data/aircraft", 204)
            raise HTTPException(status_code=status.HTTP_204_NO_CONTENT)
            # return 103
        if if_records:
            logfunc("/data/aircraft", 200)
            return Response(df.to_json(orient="records"), media_type="application/json")
        logging.info(f"Aggregating data from dataframe")
        df2 = df.groupby(['TYPE_AIRCRAFT'])['TYPE_AIRCRAFT'].count().reset_index(name='count').sort_values(['count'], ascending=False) 
        logging.info(f"Returning dataframe as JSON")
        logfunc("/data/aircraft", 200)
        return Response(df2.to_json(orient="records"), media_type="application/json")


@router.get('/engine', status_code=status.HTTP_200_OK)
async def get_engine(user_list: Union[list[str], None] = Query(default=None), if_records: bool = False):
    if not user_list:
        logfunc("/data/aircraft", 400)
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,detail="User Input is NULL" )
    filtered_state = []
    for states in user_list:
        if validate_state(states):
            filtered_state.append(states.upper())
    state_code = "', '".join(filtered_state)
    if not filtered_state:
        logfunc("/data/aircraft", 400)
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,detail="User Input is Invalid, Verify the Inputs")
    else:
        logging.info(f"User passed state, {state_code} is valid")
        try:
            client = bigquery.Client()
            logging.info(f"Connection established to Big Query Server")
        except Exception as e:
            logging.error(f"Check the path of the JSON file and contents")
            logging.error(f"Cannot connect to Big Query Server")
            logfunc("/data/aircraft", 500)
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)
            # return 101
        formated_query = f"""WITH faa AS (
        SELECT N_NUMBER, SERIAL_NUMBER, STATE, YEAR_MFR, TYPE_REGISTRANT, TYPE_AIRCRAFT, TYPE_ENGINE, STATUS_CODE
        FROM `plane-detection-352701.SPY_PLANE.FAA` faa
        WHERE COUNTRY = 'US' 
        -- AND STATE = '{state_code}'
        AND STATE IN ('{state_code}')
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
        """
        logging.info(f"Fetching data from big query")
        try:
            df = client.query(formated_query).to_dataframe()
        except Exception as e:
            logging.error(f"Bad SQL Query, Please verify SQL")
            logfunc("/data/aircraft", 500)
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)
            # return 104
        if df.empty:
            logging.error(f"No rows returned from big query")
            logfunc("/data/aircraft", 204)
            raise HTTPException(status_code=status.HTTP_204_NO_CONTENT)
            # return 103
        if if_records:
            logfunc("/data/aircraft", 200)
            return Response(df.to_json(orient="records"), media_type="application/json")
        logging.info(f"Aggregating data from dataframe")
        df2 = df.groupby(['TYPE_ENGINE'])['TYPE_ENGINE'].count().reset_index(name='count').sort_values(['count'], ascending=False) 
        logging.info(f"Returning dataframe as JSON")
        logfunc("/data/aircraft", 200)
        return Response(df2.to_json(orient="records"), media_type="application/json")

