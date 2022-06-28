from google.cloud import bigquery
from dotenv import load_dotenv
import os
import logging

#################################################
# Author: Abhijit, Piyush
# Creation Date: 17-Jun-22
# Last Modified Date:
# Change Logs:
# SL No         Date            Changes
# 1             17-Jun-22       First Version
# 2             24-Jun-22       Log Function
#################################################

def validate_state(name: str):
    """ Validates if the user input is a two-letter state and territory abbreviations

    Parameters
    ----------
    name : str
        The short two-letter state and territory abbreviations
    Returns
    -------
    Boolean
        1. True: If the name is found in the state list
        2. False: If the name is not found in the state list
    """
    all_states = ['TX', 'CA', 'FL', 'DE', 'WA', 'AK', 'IL', 'GA', 'OH', 'AZ', 'MI', 'OR', 'NY', 'UT', 'CO', 'MN', 'NC', \
        'PA', 'WI', 'OK', 'TN', 'KS', 'MO', 'VA', 'IN', 'AL', 'NV', 'MT', 'LA', 'ID', 'IA', 'AR', 'MA', 'NJ', 'NM', 'SC', \
        'MS', 'MD', 'NE', 'KY', 'ND', 'CT', 'SD', 'WY', 'NH', 'ME', 'WV', 'HI', 'VT', 'RI', 'DC', 'AP', 'AE', 'AA']
    return name.lower() in (item.lower() for item in all_states)


def logfunc(username: str, endpoint:str, response_code: int):
    # load_dotenv()
    # os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.getenv('BQ_KEY_JSON')
    logging.info(f"Writing logs to bigQuery")
    client = bigquery.Client()
    query_string = f"""
    INSERT INTO `plane-detection-352701.SPY_PLANE.logs` VALUES (
    CAST(CURRENT_TIMESTAMP() AS STRING ), '{username}', '{endpoint}', {response_code}, (SELECT MAX(logid)+1 AS ID from `plane-detection-352701.SPY_PLANE.logs`))
    """
    # logging.info(f"query_string : {query_string}")
    try:
        df = client.query(query_string)
        print(df)
    except Exception as e:
        logging.error(f"Exception: {e}")
        logging.error(f"Error Writing logs to BigQuery")
        return
    logging.info(f"Writing logs to bigQuery Completed")

    # max=client.query(f"select max(logid)+1, string(current_timestamp()) as tstamp from `plane-detection-352701.SPY_PLANE.logs`").result()
    # for i in max:
    #     var= i[0]
    #     tstamp=i[1]
    # print(var)
    # rows_to_insert =[{"logtime":tstamp, "username": username, "endpoint": endpoint, "response_code": response_code,"logid":var}]
    # errors = client.insert_rows_json('plane-detection-352701.SPY_PLANE.logs', rows_to_insert)  # Make an API request.
    # if errors == []:
    #     print("New rows have been added.")
    # else:
    #     print("Encountered errors while inserting rows: {}".format(errors))


# logfunc('hello', 101)