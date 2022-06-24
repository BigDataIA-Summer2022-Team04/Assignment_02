from fastapi import FastAPI
import uvicorn
import os
from google.cloud import bigquery
import json
import logging
from dotenv import load_dotenv


os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.getenv('BQ_KEY_JSON')
#os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r'C:\Users\abhij\Downloads\plane-detection-352701-90220d8b4de6.json'

def logfunc(endpoint:str, response_code: int):
    client = bigquery.Client()
    max=client.query(f"select max(logid)+1, string(current_timestamp()) as tstamp from `plane-detection-352701.SPY_PLANE.logs`").result()
    for i in max:
        var= i[0]
        tstamp=i[1]
    print(var)
    rows_to_insert =[{"logtime":tstamp, "endpoint": endpoint, "response_code": response_code,"logid":var}]
    errors = client.insert_rows_json('plane-detection-352701.SPY_PLANE.logs', rows_to_insert)  # Make an API request.
    if errors == []:
        print("New rows have been added.")
    else:
        print("Encountered errors while inserting rows: {}".format(errors))
