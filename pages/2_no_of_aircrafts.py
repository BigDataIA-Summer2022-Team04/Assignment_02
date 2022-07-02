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

st.set_page_config(page_title="Records of aircrafts with respect to no of times it took flight", page_icon="🌍",layout="wide", initial_sidebar_state="expanded")

st.markdown("# Spy Plane Registration Details based on No of flights")
st.sidebar.header("No of flights")



st.write(
    """This page gives you the records of Planes with respect to the number of times it took flight """
)



image ='https://www.popsci.com/uploads/2022/03/11/A380-flight-test-platform-for-the-ZEROe-demonstrator-.jpg?auto=webp&width=1440&height=958.32'

st.image(image, caption='Aircraft')

image1='https://img.buzzfeed.com/buzzfeed-static/static/2017-07/29/11/asset/buzzfeed-prod-fastlane-01/sub-buzz-2347-1501342449-1.png?resize=990:713?output-quality=auto&output-format=auto&downsize=640:*'

st.image(image1, caption='Surveillance Aircraft routes over the US')


st.write(
    """A sample of the dataset: """
)
csv=pd.read_csv('https://damg7245assignment.s3.amazonaws.com/planes_features.csv', nrows=5)
df = pd.DataFrame(csv)
df1=df[:5]
st.dataframe(df1, width=900, height=200)  # 👈 Draw the dataframe



url = "https://api-o227c7m3tq-ue.a.run.app/number_of_flights"

quan = st.selectbox(
     'Do you want to get results for aircrafts which took less than or more than number of flights, 0 indicates less than and 1 indicates more than:',
     (0, 1))


if quan==0: 
    a='less than' 
else:
    a='more than'


st.write('You selected:', a)


flights = st.slider(
'Select the number of flights from the slider',min_value=2, max_value=1000)

st.write('You selected:', flights)





def result(quan,flights):
    payload={'quan': quan, 'flights' : flights }
    url = f"https://api.anandpiyush.com/number_of_flights?quantity={payload['quan']}&flights={payload['flights']}"
    header={}
    token='eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJrdW5qaXJhbWFuLmFAbm9ydGhlYXN0ZXJuLmVkdSIsImV4cCI6MTY1NjczNDU4MX0.3Nqbl7xr-QLxLh8DIw0JuaQXyUTHh82CYYGuMb7pNxc'
    header['Authorization'] = f"Bearer {token}"
    response = requests.request("GET", url, params=payload,  headers=header)
    data=json.loads(response.text)
    df2=pd.DataFrame(data)
    return df2


buttonstat=st.button('Get Results', disabled=False)
if buttonstat:
    st.write('The following are the records')
    df2=result(quan,flights)
    st.dataframe(df2)
    st.write('Plot of flight code vs number of flights (top 20):')
    fig2=plt.figure()
    sns.barplot(x='type', y='flights', data=df2.head(20))
    plt.xticks(rotation=90)
    st.pyplot(fig2)
    st.write('Plot of flight type vs number of flights (top 20):')
    fig=plt.figure()
    sns.barplot(x='adshex', y='flights', data=df2.head(20))
    plt.xticks(rotation=90)
    st.pyplot(fig)

