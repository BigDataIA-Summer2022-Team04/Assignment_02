import os
import json
import pytest
import requests
from dotenv import load_dotenv


load_dotenv()
USERNAME = os.getenv('USERNAME')
PASSWORD = os.getenv('PASSWORD')

def get_access_token():
    payload={'username': {USERNAME}, 'password': PASSWORD}
    response = requests.request("POST", "https://api.anandpiyush.com/login", data=payload)
    json_data = json.loads(response.text)
    return json_data["access_token"]


ACCESS_TOKEN = get_access_token()
header = {}
header['Authorization'] = f"Bearer {ACCESS_TOKEN}"

# https://api.anandpiyush.com/login
def test_login(url =  "https://api.anandpiyush.com/login"):
    payload={'username': {USERNAME}, 'password': PASSWORD}
    response = requests.request("POST", url, data=payload)
    assert response.status_code == 200

def test_incorrect_login(url =  "https://api.anandpiyush.com/login"):
    payload={'username': 'fake_user@email.com', 'password': 'WrongPassword'}
    response = requests.request("POST", url, data=payload)
    assert response.status_code == 404


# /data/registrant
def test_data_registrant_invalid_token(url =  "https://api.anandpiyush.com/data/registrant?user_list=MA&if_records=false"):
    response = requests.request("GET", url, headers={'Authorization' : 'InvalidToken'})
    assert response.status_code == 401

def test_data_registrant_correct(url =  "https://api.anandpiyush.com/data/registrant?user_list=MA&if_records=false"):
    response = requests.request("GET", url, headers=header)
    assert response.status_code == 200

def test_data_registrant_wrong_state(url =  "https://api.anandpiyush.com/data/registrant?user_list=MAA&if_records=false"):
    response = requests.request("GET", url, headers=header)
    assert response.status_code == 400


# /data/registrant
def test_data_aircraft_invalid_token(url =  "https://api.anandpiyush.com/data/registrant?user_list=MA&if_records=false"):
    response = requests.request("GET", url, headers={'Authorization' : 'InvalidToken'})
    assert response.status_code == 401

def test_data_aircraft_correct(url =  "https://api.anandpiyush.com/data/registrant?user_list=MA&if_records=false"):
    response = requests.request("GET", url, headers=header)
    assert response.status_code == 200

def test_data_aircraft_wrong_state(url =  "https://api.anandpiyush.com/data/registrant?user_list=NuLL&if_records=false"):
    response = requests.request("GET", url, headers=header)
    assert response.status_code == 400


# /data/engine
def test_data_engine_invalid_token(url =  "https://api.anandpiyush.com/data/engine?user_list=MA&if_records=false"):
    response = requests.request("GET", url, headers={'Authorization' : 'InvalidToken'})
    assert response.status_code == 401

def test_data_engine_correct(url =  "https://api.anandpiyush.com/data/engine?user_list=MA&if_records=false"):
    response = requests.request("GET", url, headers=header)
    assert response.status_code == 200

def test_data_engine_wrong_state(url =  "https://api.anandpiyush.com/data/engine?user_list=Empty&if_records=false"):
    response = requests.request("GET", url, headers=header)
    assert response.status_code == 400


# /plot/map
def test_data_map_invalid_token(url =  "https://api.anandpiyush.com/plot/map?states=ALL&start_year=1910&end_year=2017"):
    response = requests.request("GET", url, headers={'Authorization' : 'InvalidToken'})
    assert response.status_code == 401

def test_data_map_correct(url =  "https://api.anandpiyush.com/plot/map?states=ALL&start_year=1910&end_year=2017"):
    response = requests.request("GET", url, headers=header)
    assert response.status_code == 200

def test_data_map_wrong_state(url =  "https://api.anandpiyush.com/plot/map?states=ALL&start_year=1950&end_year=1940"):
    response = requests.request("GET", url, headers=header)
    assert response.status_code == 400


# /plot/histogram
def test_data_histogram_invalid_token(url =  "https://api.anandpiyush.com/plot/histogram?states=ALL&start_year=1910&end_year=2017&buckets=10"):
    response = requests.request("GET", url, headers={'Authorization' : 'InvalidToken'})
    assert response.status_code == 401

def test_data_histogram_correct(url =  "https://api.anandpiyush.com/plot/histogram?states=ALL&start_year=1910&end_year=2017&buckets=10"):
    response = requests.request("GET", url, headers=header)
    assert response.status_code == 200

def test_data_histogram_wrong_state(url =  "https://api.anandpiyush.com/plot/histogram?states=ALL&start_year=2017&end_year=1910&buckets=10"):
    response = requests.request("GET", url, headers=header)
    assert response.status_code == 400

def test_data_histogram_wrong_state(url =  "https://api.anandpiyush.com/plot/histogram?states=ALL&start_year=1910&end_year=2017&buckets=0"):
    response = requests.request("GET", url, headers=header)
    assert response.status_code == 400