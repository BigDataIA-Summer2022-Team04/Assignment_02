
from fastapi import APIRouter, HTTPException, Response, status, Query
from validate_state import validate_state
from repository import user_functions as uf
import logging
import time
from google.cloud import bigquery
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from io import BytesIO
from starlette.responses import StreamingResponse
from typing import Union
import matplotlib.pyplot as plt
import geopandas

router = APIRouter(
    prefix="/plot",
    tags=['Plots']
)


@router.get('/histogram', status_code=status.HTTP_200_OK)
async def get_histogram(user_list: Union[list[str], None] = Query(default=['ALL'], description="State code of two char or One input of 'all'"), min_year: int = Query(default=1910, description="Input start year"), max_year: int = Query(default=2017, description="Input end year, Cannot be smaller or equal to start year"), buckets: int = Query(default=10, description="bin distribution, value more than 1")):
    filtered_state = []
    if not user_list:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,detail="User Input is NULL" )
    elif (len(user_list) == 1) and (user_list[0].upper() == "ALL"):
        filtered_state.append(user_list[0].upper())
    else:
        for states in user_list:
            if validate_state(states):
                filtered_state.append(states.upper())
        state_code = "', '".join(filtered_state)
    if (not filtered_state) or (max_year < min_year) or ( (max_year - min_year) < buckets) or (buckets <= 1) :
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,detail="User Input is Invalid, Verify the Inputs")
    else:
        logging.info(f"User passed path, States: {filtered_state}; Min_year: {min_year}; Max_year: {max_year} is valid")
        try:
            client = bigquery.Client()
            logging.info(f"Connection established to Big Query Server")
        except Exception as e:
            logging.error(f"Check the path of the JSON file and contents")
            logging.error(f"Cannot connect to Big Query Server")
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)
        if (len(user_list) == 1) and (user_list[0].upper() == "ALL"):
            formated_query = f"""
            SELECT YEAR_MFR 
            FROM `plane-detection-352701.SPY_PLANE.FAA`
            WHERE COUNTRY = 'US'
            """
        else:
            formated_query = f"""
            SELECT YEAR_MFR 
            FROM `plane-detection-352701.SPY_PLANE.FAA`
            WHERE COUNTRY = 'US'
            AND STATE IN ('{state_code}')
            """
        logging.info(f"Fetching data from big query")
        try:
            df = client.query(formated_query).to_dataframe()
        except Exception as e:
            logging.error(f"Exception: {e}")
            logging.error(f"Bad SQL Query, Please verify SQL")
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)
        if df.empty:
            logging.error(f"No rows returned from big query")
            raise HTTPException(status_code=status.HTTP_204_NO_CONTENT)
        logging.info(f"Aggregating data from dataframe")
        df = df.dropna()
        df['YEAR'] = pd.DatetimeIndex(df['YEAR_MFR']).year
        df = df[(df['YEAR'] >= min_year) & (df['YEAR'] <= max_year)]
        bins = np.linspace(min_year, max_year, buckets+1)
        df['bins'] = pd.cut(df['YEAR'], bins=bins, include_lowest=True)
        logging.info(f"Plotting histogram")
        plt.figure(figsize=(7, 8))
        values, bins, bars = plt.hist(df['YEAR'], bins=buckets, edgecolor='white')
        plt.title(f'Aircraft Registration between {min_year} and {max_year} for {", ".join(filtered_state)} states')
        plt.xlabel('Years')
        plt.ylabel('Count of Registration')
        plt.bar_label(bars, fontsize=10, color='black')
        plt.margins(x=0.01, y=0.1)
        # plt.show()
        ts = time.strftime("%Y%m%d-%H%M%S")
        buf = BytesIO()
        plt.savefig(buf, format="png")
        buf.seek(0)
        return StreamingResponse(buf, media_type="image/png")

@router.get('/map', status_code=status.HTTP_200_OK)
async def get_map(user_list: Union[list[str], None] = Query(default=['ALL'], description="State code of two char or One input of 'all'"), min_year: int = Query(default=1910, description="Input start year"), max_year: int = Query(default=2017, description="Input end year, Cannot be smaller or equal to start year")):
    filtered_state = []
    if not user_list:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,detail="User Input is NULL" )
    elif (len(user_list) == 1) and (user_list[0].upper() == "ALL"):
        filtered_state.append(user_list[0].upper())
    else:
        for states in user_list:
            if validate_state(states):
                filtered_state.append(states.upper())
        state_code = "', '".join(filtered_state)
    if (not filtered_state) or (max_year < min_year) :
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,detail="User Input is Invalid, Verify the Inputs")
    else:
        logging.info(f"User passed path, States: {filtered_state}; Min_year: {min_year}; Max_year: {max_year} is valid")
        try:
            client = bigquery.Client()
            logging.info(f"Connection established to Big Query Server")
        except Exception as e:
            logging.error(f"Check the path of the JSON file and contents")
            logging.error(f"Cannot connect to Big Query Server")
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)
        if (len(user_list) == 1) and (user_list[0].upper() == "ALL"):
            formated_query = f"""
            SELECT
            STATE AS STUSPS,
            COUNT(*) AS COUNT
            FROM `plane-detection-352701.SPY_PLANE.FAA` 
            WHERE COUNTRY = 'US'
            GROUP BY
            STATE
            """
        else:
            formated_query = f"""
            SELECT
            STATE AS STUSPS,
            COUNT(*) AS COUNT
            FROM `plane-detection-352701.SPY_PLANE.FAA` 
            WHERE COUNTRY = 'US' AND STATE IN ('{state_code}')
            GROUP BY
            STATE
            """
        logging.info(f"Fetching data from big query")
        try:
            df = client.query(formated_query).to_dataframe()
        except Exception as e:
            logging.error(f"Exception: {e}")
            logging.error(f"Bad SQL Query, Please verify SQL")
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)
        if df.empty:
            logging.error(f"No rows returned from big query")
            raise HTTPException(status_code=status.HTTP_204_NO_CONTENT)
        logging.info(f"Aggregating data from dataframe")
        df = df.dropna()

        states = geopandas.read_file('/Users/piyush/Downloads/geopandas-tutorial-master/data/usa-states-census-2014.shp')
        states = states.to_crs("EPSG:3395")
        filter = states.merge(df, on='STUSPS')
        us_boundary_map = states.boundary.plot(figsize=(18, 12), color="Gray")
        filter.plot(ax=us_boundary_map,  cmap = 'Spectral')

        # fig = plt.figure(1, figsize=(25,15))
        # ax = fig.add_subplot()
        # filter.apply(lambda x: ax.annotate(text=x.NAME + "\n" + str(x.COUNT), xy=x.geometry.centroid.coords[0], ha='center', fontsize=8),axis=1)
        # states.boundary.plot(ax=ax, color='Black', linewidth=.7)
        # states.plot(ax = ax, figsize = (10,10), cmap = 'Spectral', legend = True)

        # states.plot(column = 'COUNT', figsize = (25,15), cmap = 'Spectral', legend = True)

        # ax.axes.xaxis.set_visible(False)
        # ax.axes.yaxis.set_visible(False)
        plt.title('Flight Registration State Wise')
        buf = BytesIO()
        plt.savefig(buf, format="png")
        buf.seek(0)
        return StreamingResponse(buf, media_type="image/png")