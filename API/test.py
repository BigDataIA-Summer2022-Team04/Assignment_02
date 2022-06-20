import pytest
import os
from dotenv import load_dotenv
from validate_state import validate_state
from status_code import status_code
from type_engine import type_engine
from type_registrant import type_registrant
from type_aircraft import type_aircraft
from hist_plot_count_year import hist_plot
from mfgplanesinfo import queries
from n_of_flights import noofflights
from typeaircraft import typeaircraft
from typeengine import typengine

#################################################
# Author: Piyush, Abhijit, Jui
# Creation Date: 17-Jun-22
# Last Modified Date:
# Change Logs:
# SL No         Date            Changes
# 1             17-Jun-22       First Version
# 2             20-Jun-22       Merged all test cases into one
#################################################

load_dotenv()

#################################################
# Piyush Test cases START
#################################################

## Invalid State Name
def test_state_uppercase():
    assert validate_state('MA') == True

def test_state_lowercase():
    assert validate_state('ma') == True

def test_invalid_state():
    assert validate_state('none') == False


## Incorrect Big Query JSON KEY
def test_incorrect_json_file_status_code():
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.getenv('BQ_KEY_JSON_INVALID')
    assert status_code('MA') == 101

def test_incorrect_json_file_type_aircraft():
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.getenv('BQ_KEY_JSON_INVALID')
    assert type_aircraft('MA') == 101

def test_incorrect_json_file_type_engine():
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.getenv('BQ_KEY_JSON_INVALID')
    assert type_engine('MA') == 101

def test_incorrect_json_file_type_registrant():
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.getenv('BQ_KEY_JSON_INVALID')
    assert type_registrant('MA') == 101


## Incorrect Sate Abbreviations
def test_incorrect_state_name_status_code():
    assert status_code('NO_State') == 102

def test_incorrect_state_name_type_aircraft():
    assert type_aircraft('NO_State') == 102

def test_incorrect_state_name_type_engine():
    assert type_engine('NO_State') == 102

def test_incorrect_state_name_type_registrant():
    assert type_registrant('NO_State') == 102


## Data Found from Big Query
def test_incorrect_state_name_status_code():
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.getenv('BQ_KEY_JSON')
    returns = status_code('MA')
    assert len(returns) == 2

def test_incorrect_state_name_type_aircraft():
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.getenv('BQ_KEY_JSON')
    returns = type_aircraft('MA')
    assert len(returns) == 2

def test_incorrect_state_name_type_engine():
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.getenv('BQ_KEY_JSON')
    returns = type_engine('MA')
    assert len(returns) == 2

def test_incorrect_state_name_type_registrant():
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.getenv('BQ_KEY_JSON')
    returns = type_registrant('MA')
    assert len(returns) == 2

# File path test case
def test_incorrect_file_path_hist_plot():
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.getenv('BQ_KEY_JSON')
    assert hist_plot('~/invalid/path') == 102

def test_incorrect_json_file_hist_plot():
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.getenv('BQ_KEY_JSON_INVALID')
    assert hist_plot('/Users/piyush/Downloads') == 101

#################################################
# Piyush Test cases ENDS
#################################################

#################################################
# Abhijit Test cases START
#################################################

## Incorrect Big Query JSON KEY
def test_incorrect_json_file_mfgplanesinfo():
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.getenv('BQ_KEY_JSON_INVALID')
    assert queries(0,2000) == 101

def test_incorrect_json_file_n_of_flights():
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.getenv('BQ_KEY_JSON_INVALID')
    assert noofflights(0,900) == 101

def test_incorrect_json_file_type_engine():
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.getenv('BQ_KEY_JSON_INVALID')
    assert typengine(1) == 101

def test_incorrect_json_file_type_registrant():
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.getenv('BQ_KEY_JSON_INVALID')
    assert typeaircraft('MA') == 101


## Data Found from Big Query
def test_incorrect_type_aircraft():
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.getenv('BQ_KEY_JSON')
    assert typeaircraft(100) == 102

def test_incorrecttype_engine():
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.getenv('BQ_KEY_JSON')
    assert typengine(90) == 102

def test_incorrect_mfgplanesinfo_year():
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.getenv('BQ_KEY_JSON')
    assert queries(1,1900)== 103

#################################################
# Abhijit Test cases ENDS
#################################################




#################################################
# Jui Test cases START
#################################################


#################################################
# Jui Test cases ENDS
#################################################