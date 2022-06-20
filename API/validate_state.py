#################################################
# Author: Piyush
# Creation Date: 17-Jun-22
# Last Modified Date:
# Change Logs:
# SL No         Date            Changes
# 1             17-Jun-22       First Version
# 
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


