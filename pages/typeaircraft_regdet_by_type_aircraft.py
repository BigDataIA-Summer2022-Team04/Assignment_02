from matplotlib.axes import Axes
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

st.set_page_config(page_title="Records of Flights with respect to aircraft type", page_icon="🌍",layout="wide", initial_sidebar_state="expanded")

st.markdown("# Records of Flights with respect to aircraft type")
st.sidebar.header("aircraft type")



st.write(
    """This page gives you the records of Planes with respect to the aircraft types """
)



image ='https://cdni.rbth.com/rbthmedia/images/all/2017/05/05/parade_air-eng.jpg'

st.image(image, caption='Aircraft Types')

image1='https://img.buzzfeed.com/buzzfeed-static/static/2017-07/29/11/asset/buzzfeed-prod-fastlane-01/sub-buzz-2347-1501342449-1.png?resize=990:713?output-quality=auto&output-format=auto&downsize=640:*'

st.image(image1, caption='Surveillance Aircraft routes over the US')


st.write(
    """A sample of the dataset: """
)
csv=pd.read_csv('https://damg7245assignment.s3.amazonaws.com/faa_registration.csv',nrows=10)
df = pd.DataFrame(csv)
df1=df[:10]
st.dataframe(df1, width=900, height=200)  # 👈 Draw the dataframe




url = "https://api-o227c7m3tq-ue.a.run.app/regdet_by_type_aircraft"




type = st.slider(
'Select the aircraft type from the slider',min_value=1, max_value=9)

st.write('You selected:', type)





def result(type):
    payload={'type': type}
    url = f"https://api.anandpiyush.com/regdet_by_type_aircraft?type={payload['type']}"
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
    chart=df2['TYPE_ENGINE'].value_counts()
    st.bar_chart(chart)


