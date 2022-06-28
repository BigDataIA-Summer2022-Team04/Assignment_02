
import time
import logging
import geopandas
import numpy as np
import pandas as pd
from io import BytesIO
from typing import Union
import matplotlib.pyplot as plt
import matplotlib.pyplot as plt
from google.cloud import bigquery
from routers.oaut2 import get_current_user
from custom_functions import validate_state, logfunc
from starlette.responses import StreamingResponse
from fastapi import APIRouter, HTTPException, Response, status, Query, Request, Depends
import schemas

router = APIRouter(
    prefix="/plot",
    tags=['Plots']
)


@router.get('/histogram', status_code=status.HTTP_200_OK)
async def get_histogram(states: Union[list[str], None] = Query(default=['ALL'], description="State code of two char or One input of 'all'"),
                        start_year: int = Query(default=1910, description="Input start year"),
                        end_year: int = Query(default=2017, description="Input end year, Cannot be smaller or equal to start year"),
                        buckets: int = Query(default=10, description="bin distribution, value more than 1"),
                        get_current_user: schemas.ServiceAccount = Depends(get_current_user)):
    '''
    Returns a histogram of count of yearly flight registration.

            Parameters:
                    states (list[str]): 2 char USA state list
                    start_year (int): Start year in YYYY, example 1990
                    end_year (int): End year in YYYY, example 2017
                    buckets (int): Grouping of distribution, (end_year - start_year) > buckets > 1

            Returns:
                    image_bytes (image/png): Binary of the image
    '''
    filtered_state = []
    if not states:
        logfunc(get_current_user.email, "/plot/histogram", 400)
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,detail="User Input is NULL" )
    elif (len(states) == 1) and (states[0].upper() == "ALL"):
        filtered_state.append(states[0].upper())
    else:
        for states in states:
            if validate_state(states):
                filtered_state.append(states.upper())
        state_code = "', '".join(filtered_state)
    if (not filtered_state) or (end_year < start_year) or ( (end_year - start_year) < buckets) or (buckets <= 1) :
        logfunc(get_current_user.email, "/plot/histogram", 400)
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,detail="User Input is Invalid, Verify the Inputs")
    else:
        logging.info(f"User passed path, States: {filtered_state}; Start_year: {start_year}; End_year: {end_year} is valid")
        try:
            client = bigquery.Client()
            logging.info(f"Connection established to Big Query Server")
        except Exception as e:
            logging.error(f"Check the path of the JSON file and contents")
            logging.error(f"Cannot connect to Big Query Server")
            logfunc(get_current_user.email, "/plot/histogram", 500)
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)
        if (len(states) == 1) and (states[0].upper() == "ALL"):
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
            logfunc(get_current_user.email, "/plot/histogram", 500)
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)
        if df.empty:
            logging.error(f"No rows returned from big query")
            logfunc(get_current_user.email, "/plot/histogram", 204)
            raise HTTPException(status_code=status.HTTP_204_NO_CONTENT)
        logging.info(f"Aggregating data from dataframe")
        df = df.dropna()
        df['YEAR'] = pd.DatetimeIndex(df['YEAR_MFR']).year
        df = df[(df['YEAR'] >= start_year) & (df['YEAR'] <= end_year)]
        bins = np.linspace(start_year, end_year, buckets+1)
        df['bins'] = pd.cut(df['YEAR'], bins=bins, include_lowest=True)
        logging.info(f"Plotting histogram")
        plt.figure(figsize=(7, 8))
        values, bins, bars = plt.hist(df['YEAR'], bins=buckets, edgecolor='white')
        plt.title(f'Aircraft Registration between {start_year} and {end_year} for {", ".join(filtered_state)} states')
        plt.xlabel('Years')
        plt.ylabel('Count of Registration')
        plt.bar_label(bars, fontsize=10, color='black')
        plt.margins(x=0.01, y=0.1)
        # plt.show()
        #ts = time.strftime("%Y%m%d-%H%M%S")
        buf = BytesIO()
        plt.savefig(buf, format="png")
        buf.seek(0)
        # logging.debug(f"raw_url {request.url._url}")
        logfunc(get_current_user.email, "/plot/histogram", 200)
        return StreamingResponse(buf, media_type="image/png")



@router.get('/map', status_code=status.HTTP_200_OK)
async def get_map(states: Union[list[str], None] = Query(default=['ALL'], 
                    description="State code of two char or One input of 'all'"), 
                    start_year: int = Query(default=1910, description="Input start year"), 
                    end_year: int = Query(default=2017, description="Input end year, Cannot be smaller or equal to start year"),
                    get_current_user: schemas.ServiceAccount = Depends(get_current_user)):
    '''
    Returns a USA map of count of flight registration state wise.

            Parameters:
                    states (list[str]): 2 char USA state list
                    start_year (int): Start year in YYYY, example 1990
                    end_year (int): End year in YYYY, example 2017

            Returns:
                    image_bytes (image/png): Binary of the image
    '''
    filtered_state = []
    if not states:
        logfunc(get_current_user.email, "/plot/map", 400)
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,detail="User Input is NULL" )
    elif (len(states) == 1) and (states[0].upper() == "ALL"):
        filtered_state.append(states[0].upper())
    else:
        for states in states:
            if validate_state(states):
                filtered_state.append(states.upper())
        state_code = "', '".join(filtered_state)
    if (not filtered_state) or (end_year < start_year) :
        logfunc(get_current_user.email, "/plot/map", 400)
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,detail="User Input is Invalid, Verify the Inputs")
    else:
        logging.info(f"User passed path, States: {filtered_state}; Start_year: {start_year}; End_year: {end_year} is valid")
        try:
            client = bigquery.Client()
            logging.info(f"Connection established to Big Query Server")
        except Exception as e:
            logging.error(f"Check the path of the JSON file and contents")
            logging.error(f"Cannot connect to Big Query Server")
            logfunc(get_current_user.email, "/plot/map", 500)
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)
        if (len(states) == 1) and (states[0].upper() == "ALL"):
            formated_query = f"""
            SELECT
            STATE AS STUSPS,
            COUNT(*) AS COUNT
            FROM `plane-detection-352701.SPY_PLANE.FAA` 
            WHERE COUNTRY = 'US' AND YEAR_MFR BETWEEN '{start_year}-01-01' AND '{end_year}-01-01'
            GROUP BY
            STATE
            """
        else:
            formated_query = f"""
            SELECT
            STATE AS STUSPS,
            COUNT(*) AS COUNT
            FROM `plane-detection-352701.SPY_PLANE.FAA` 
            WHERE COUNTRY = 'US' AND STATE IN ('{state_code}') AND YEAR_MFR BETWEEN '{start_year}-01-01' AND '{end_year}-01-01'
            GROUP BY
            STATE
            """
        logging.info(f"Fetching data from big query")
        try:
            df = client.query(formated_query).to_dataframe()
        except Exception as e:
            logging.error(f"Exception: {e}")
            logging.error(f"Bad SQL Query, Please verify SQL")
            logfunc(get_current_user.email, "/plot/map", 500)
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)
        if df.empty:
            logging.error(f"No rows returned from big query")
            logfunc(get_current_user.email, "/plot/map", 204)
            raise HTTPException(status_code=status.HTTP_204_NO_CONTENT)
        logging.info(f"Aggregating data from dataframe")
        df = df.dropna()
        states = geopandas.read_file('routers/data/usa-states-census-2014.shp')
        states = states.to_crs("EPSG:3395")
        filter = states.merge(df, on='STUSPS')
        fig, ax = plt.subplots(1, 1, figsize=(20,15))
        ax.axes.xaxis.set_visible(False)
        ax.axes.yaxis.set_visible(False)
        ax = fig.add_subplot()
        plt.title(f'Aircraft Registration Counts between {start_year} and {end_year} for {", ".join(filtered_state)} states', fontdict = {'fontsize' : 18})
        states.boundary.plot(ax=ax, color='Black', linewidth=.5)
        filter.plot(ax=ax, column = 'COUNT', cmap = 'Spectral', legend = True, legend_kwds={'shrink': 0.3})
        filter.apply(lambda x: ax.annotate(text=x.STUSPS + "\n" + str(x.COUNT), xy=x.geometry.centroid.coords[0], ha='center', fontsize=12),axis=1)
        ax.axes.xaxis.set_visible(False)
        ax.axes.yaxis.set_visible(False)
        buf = BytesIO()
        plt.savefig(buf, format="png")
        buf.seek(0)
        logfunc(get_current_user.email, "/plot/map", 200)
        return StreamingResponse(buf, media_type="image/png")


