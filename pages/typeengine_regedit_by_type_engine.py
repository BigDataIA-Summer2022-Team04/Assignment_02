import streamlit as st
import pandas as pd
import pydeck as pdk
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import requests
import json
from PIL import Image
import seaborn as sns

st.set_page_config(page_title="Records of Flights with respect to engine type", page_icon="🌍",layout="wide", initial_sidebar_state="expanded")

st.markdown("# Records of Flights with respect to engine type")
st.sidebar.header("engine type")



st.write(
    """This page gives you the records of Planes with respect to the engine types """
)



image ='https://images.squarespace-cdn.com/content/v1/568c49565a56689becb3c612/1593715037605-QYL5KPD0M89I0JJUMIT3/CFM56-3+Aircraft+Engine+Transport?format=1000w'

st.image(image, caption='Aircraft engine')

image1='https://ychef.files.bbci.co.uk/976x549/p0bvnm3n.jpg'

st.image(image1, caption='Aircraft')


st.write(
    """A sample of the dataset: """
)
csv=pd.read_csv('https://damg7245assignment.s3.amazonaws.com/faa_registration.csv',nrows=10)
df = pd.DataFrame(csv)
df1=df[:10]
st.dataframe(df1, width=900, height=200)  # 👈 Draw the dataframe




url = "https://damg7245-assignment02-api-o227c7m3tq-ue.a.run.app/regdet_through_enginetype"




type = st.slider(
'Select the aircraft type from the slider',min_value=0, max_value=11)

st.write('You selected:', type)





def result(type):
    payload={'type': type}
    url = f"https://api.anandpiyush.com/regdet_through_enginetype?type={payload['type']}"
    header={}
    token='eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJrdW5qaXJhbWFuLmFAbm9ydGhlYXN0ZXJuLmVkdSIsImV4cCI6MTY1NjczNzQ5OH0.qZ6FY7yjZ7eE5O0tbR8cZeF5aHylqmgvGr5hBBpw6e0'
    header['Authorization'] = f"Bearer {token}"
    response = requests.request("GET", url, params=payload, headers=header)
    data=json.loads(response.text)
    df2=pd.DataFrame(data)
    return df2


buttonstat=st.button('Get Results', disabled=False)
if buttonstat:
    st.write('The following are the records')
    df2=result(type)
    st.dataframe(df2)
    st.markdown('***')
    st.markdown('Plot of count of different engines types used for given aircraft')
    chart=df2['TYPE_AIRCRAFT'].value_counts()
    st.bar_chart(chart)


