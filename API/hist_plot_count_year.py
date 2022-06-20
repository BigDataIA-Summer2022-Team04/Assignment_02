import os
import json
import time
import logging
import argparse
import pandas as pd
import numpy as np
import datetime
import matplotlib.pyplot as plt
from dotenv import load_dotenv
from google.cloud import bigquery

#################################################
# Author: Piyush
# Creation Date: 18-Jun-22
# Last Modified Date:
# Change Logs:
# SL No         Date            Changes
# 1             17-Jun-22       First Version
#
#################################################
# Exit Codes:
# 101 - Cannot connect to Big Query Server
# 102 - Invalid User input
# 103 - No rows returned from big query
# 104 - Invalid SQL query
#################################################

# Enable logging
load_dotenv()
LOGLEVEL = os.environ.get('LOGLEVEL', 'INFO').upper()
logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=LOGLEVEL,
    datefmt='%Y-%m-%d %H:%M:%S')


# Defined functions
def hist_plot(save_path):
    """Gets the path where user wants to save the image

    Parameters
    ----------
    save_path : str
        absolute path to save the image
    Returns
    -------
    none
    """
    if os.path.isdir(save_path):
        logging.info(f"User passed path, {save_path} is valid")
        try:
            client = bigquery.Client()
            logging.info(f"Connection established to Big Query Server")
        except Exception as e:
            logging.error(f"Check the path of the JSON file and contents")
            logging.error(f"Cannot connect to Big Query Server")
            return 101
        formated_query = f"""SELECT YEAR_MFR FROM `plane-detection-352701.SPY_PLANE.FAA`"""
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
        df['YEAR'] = pd.DatetimeIndex(df['YEAR_MFR']).year
        min_value = df['YEAR'].min().astype(int)
        max_value = df['YEAR'].max().astype(int)
        bins = np.linspace(min_value, max_value, 10)
        labels = ['pre1920', 'pre1933', 'pre1945', 'pre1957',
                  'pre1969', 'pre1981', 'pre1993', 'pre2005', 'post2017']
        df['bins'] = pd.cut(df['YEAR'], bins=bins, labels=labels, include_lowest=True)
        logging.info(f"Plotting histogram")
        plt.figure(figsize=(7, 8))
        values, bins, bars = plt.hist(df['YEAR'], bins=9, edgecolor='white')
        plt.title(f'Aircraft Registration between {min_value} and {max_value}')
        plt.xlabel('Years')
        plt.ylabel('Count of Registration')
        plt.bar_label(bars, fontsize=10, color='black')
        plt.margins(x=0.01, y=0.1)
        # plt.show()
        ts = time.strftime("%Y%m%d-%H%M%S")
        file_path = os.path.join(save_path, f"faa_hist_{ts}.png")
        plt.savefig(file_path)
        logging.info(f"Image saved at {file_path}")
        return
    else:
        logging.error(f"User passed path, {save_path} is invalid")
        logging.info(f"Please enter a valid path without ~")
        return 102


def exit_script(error_code: int = 0):
    logging.info(f"Script Ends")
    exit(error_code)


def main(save_path):
    logging.info(f"Script Starts")
    plot = hist_plot(save_path)
    exit_script(plot)


# Script Starts
if __name__ == "__main__":
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.getenv('BQ_KEY_JSON')

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--save_path",
        default="/Users/piyush/Sandbox/DAMG7245_Assignment_01/data/processed",
        help="the location where the plot would be saved."
    )
    args = parser.parse_args()

    main(args.save_path)
    
