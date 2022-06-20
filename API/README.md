# Data as a Service

# Deliverables
In this Module you must create 5 – 10 Python functions which have input parameters and output as an image from your dataset 

Requirements for each function:

1. Each python function should be in separate py file

2. Write the function Documentation: This should be written in plain English telling what the function will do with created date and author's name. Write it inside your py file
https://realpython.com/documenting-python-code/

3. Write the Function Unit Test: Unit test for your python functions use pytest for this. 
https://docs.pytest.org/en/7.1.x/

4. Error Handling: Your functions should have try and catch statements to handle errors. 

Submission: Create a folder in your GitHub repo and should have all the above files inside it 

---
# Accomplishments

## 01. Python Function Scripts

| Sl No 	| Function Name   	| Inputs                                       	| Outputs                                                	| Python Script                                     	|
|-------	|-----------------	|----------------------------------------------	|--------------------------------------------------------	|---------------------------------------------------	|
| 1     	| status_code     	| two-letter state and territory abbreviations 	| JSON <br> [0] - Data Summary  <br> [1] - Table Records 	| [status_code.py](/API/status_code.py)         	|
| 2     	| type_aircraft   	| two-letter state and territory abbreviations 	| JSON <br> [0] - Data Summary  <br> [1] - Table Records 	| [type_aircraft.py](/API/type_aircraft.py)     	|
| 3     	| type_engine     	| two-letter state and territory abbreviations 	| JSON <br> [0] - Data Summary  <br> [1] - Table Records 	| [type_engine.py](/API/type_engine.py)         	|
| 4     	| type_registrant 	| two-letter state and territory abbreviations 	| JSON <br> [0] - Data Summary  <br> [1] - Table Records 	| [type_registrant.py](/API/type_registrant.py) 	|
|       	|                 	| Unit test cases for above scripts            	|                                                        	| [test_01.py](/API/test_01.py)                 	|
| 5     	|                 	|                                              	|                                                        	|                                                   	|
| 6     	|                 	|                                              	|                                                        	|                                                   	|
| 7     	|                 	|                                              	|                                                        	|                                                   	|
| 8     	|                 	|                                              	|                                                        	|                                                   	|
| 9     	|                 	|                                              	|                                                        	|                                                   	|
| 10    	|                 	|                                              	|                                                        	|                                                   	|
| 11      	|                 	|                                              	|                                                        	|                                                   	|
| 12      	|                 	|                                              	|                                                        	|                                                   	|

## 02. Python Function Scripts

Example: [status_code.py](/API/status_code.py)
```python
def status_code(state_short: str):
    """Gets and returns the aggregate statistics's of flights and records

    Parameters
    ----------
    state_short : str
        The short two-letter state and territory abbreviations
    Returns
    -------
    json
        1. Aggregate of count of planes based on status code
        2. Records of flight number, serial number, state name, status
    """
```

## 03. Function Unit Test

Example: [test_01.py](/API/test_01.py)   
```python
## Incorrect Sate Abbreviations
def test_incorrect_state_name_status_code():
    assert status_code('NO_State') == 102
```

## 04. Error Handeling

Example: [status_code.py](/API/status_code.py)
```python
try:
    client = bigquery.Client()
    logging.info(f"Connection established to Big Query Server")
except Exception as e:
    logging.error(f"Check the path of the JSON file and contents")
    logging.error(f"Cannot connect to Big Query Server")
    return 101
```