import os
import uvicorn
import logging
import pandas as pd
from typing import Union
from dotenv import load_dotenv
from routers import plot, data
from google.cloud import bigquery
from fastapi import FastAPI, Query
from validate_state import validate_state
from fastapi.responses import HTMLResponse


#################################################
# Author: Piyush
# Creation Date: 22-Jun-22
# Last Modified Date:
# Change Logs:
# SL No         Date            Changes
# 1             22-Jun-22       First Version
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

app = FastAPI()

#################################
# Piyush

app.include_router(plot.router)
app.include_router(data.router)

@app.get("/", response_class=HTMLResponse)
async def read_items():
    html_content = """
    <html>
        <head>
            <title>DAMG7245 Assignment 2</title>
        </head>
        <body>
            <h1>DaaS API</h1>
            <p> Using FastAPI, python function converted as a API to interact with big-Query. <br>
                Refer localhost:<port_number>/doc for API documentation</p>
        </body>
    </html>
    """
    return HTMLResponse(content=html_content, status_code=200)

#################################


#################################
# Abhi
#################################

#################################
# Jui
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
    # sample_query = f"""SELECT YEAR_MFR FROM `plane-detection-352701.SPY_PLANE.FAA` LIMIT 5"""
    logging.info(f"Handshake | Fetching data from Big-Query")
    try:
        df = client.query(sample_query).to_dataframe()
    except Exception as e:
        # logging.error(f"Exception: {e}")
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
        uvicorn.run(app, host="127.0.0.1", port=9000)
