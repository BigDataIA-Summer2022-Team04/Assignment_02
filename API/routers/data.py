
from fastapi import APIRouter, HTTPException, Response, status, Query
from validate_state import validate_state
from repository import user_functions as uf
import json
import logging
from google.cloud import bigquery
from typing import Union



router = APIRouter(
    prefix="/data",
    tags=['Data']
)


@router.get('/registrant', status_code=status.HTTP_200_OK)
# def get_registrant(state_code: str, records: bool = False):
async def get_registrant(user_list: Union[list[str], None] = Query(default=None), if_records: bool = False):
    if not user_list:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,detail="User Input is NULL" )
    filtered_state = []
    for states in user_list:
        if validate_state(states):
            filtered_state.append(states.upper())
    state_code = "', '".join(filtered_state)
    if not filtered_state:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,detail="User Input is Invalid, Verify the Inputs")
    else:
        logging.info(f"User passed state, {filtered_state} is valid")
        try:
            client = bigquery.Client()
            logging.info(f"Connection established to Big Query Server")
        except Exception as e:
            logging.error(f"Check the path of the JSON file and contents")
            logging.error(f"Cannot connect to Big Query Server")
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
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)
        if df.empty:
            logging.error(f"No rows returned from big query")
            raise HTTPException(status_code=status.HTTP_204_NO_CONTENT)
            # return 103
        if if_records:
            return Response(df.to_json(orient="records"), media_type="application/json")
        logging.info(f"Aggregating data from dataframe")
        df2 = df.groupby(['TYPE_REGISTRANT'])['TYPE_REGISTRANT'].count().reset_index(name='count').sort_values(['count'], ascending=False) 
        logging.info(f"Returning dataframe as JSON")
        return Response(df2.to_json(orient="records"), media_type="application/json")


@router.get('/aircraft', status_code=status.HTTP_200_OK)
# def get_aircraft(state_code: str, records: bool = False):
#     if not validate_state(state_code):
async def get_aircraft(user_list: Union[list[str], None] = Query(default=None), if_records: bool = False):
    if not user_list:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,detail="User Input is NULL" )
    filtered_state = []
    for states in user_list:
        if validate_state(states):
            filtered_state.append(states.upper())
    state_code = "', '".join(filtered_state)
    if not filtered_state:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,detail="User Input is Invalid, Verify the Inputs")
    else:
        logging.info(f"User passed state, {state_code} is valid")
        try:
            client = bigquery.Client()
            logging.info(f"Connection established to Big Query Server")
        except Exception as e:
            logging.error(f"Check the path of the JSON file and contents")
            logging.error(f"Cannot connect to Big Query Server")
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
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)
            # return 104
        if df.empty:
            logging.error(f"No rows returned from big query")
            raise HTTPException(status_code=status.HTTP_204_NO_CONTENT)
            # return 103
        if if_records:
            return Response(df.to_json(orient="records"), media_type="application/json")
        logging.info(f"Aggregating data from dataframe")
        df2 = df.groupby(['TYPE_AIRCRAFT'])['TYPE_AIRCRAFT'].count().reset_index(name='count').sort_values(['count'], ascending=False) 
        logging.info(f"Returning dataframe as JSON")
        return Response(df2.to_json(orient="records"), media_type="application/json")


@router.get('/engine', status_code=status.HTTP_200_OK)
# def get_engine(state_code: str, records: bool = False):
#     if not validate_state(state_code):
async def get_engine(user_list: Union[list[str], None] = Query(default=None), if_records: bool = False):
    if not user_list:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,detail="User Input is NULL" )
    filtered_state = []
    for states in user_list:
        if validate_state(states):
            filtered_state.append(states.upper())
    state_code = "', '".join(filtered_state)
    if not filtered_state:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,detail="User Input is Invalid, Verify the Inputs")
    else:
        logging.info(f"User passed state, {state_code} is valid")
        try:
            client = bigquery.Client()
            logging.info(f"Connection established to Big Query Server")
        except Exception as e:
            logging.error(f"Check the path of the JSON file and contents")
            logging.error(f"Cannot connect to Big Query Server")
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
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)
            # return 104
        if df.empty:
            logging.error(f"No rows returned from big query")
            raise HTTPException(status_code=status.HTTP_204_NO_CONTENT)
            # return 103
        if if_records:
            return Response(df.to_json(orient="records"), media_type="application/json")
        logging.info(f"Aggregating data from dataframe")
        df2 = df.groupby(['TYPE_ENGINE'])['TYPE_ENGINE'].count().reset_index(name='count').sort_values(['count'], ascending=False) 
        logging.info(f"Returning dataframe as JSON")
        return Response(df2.to_json(orient="records"), media_type="application/json")

