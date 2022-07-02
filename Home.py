import streamlit as st
import pandas as pd
import pydeck as pdk
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import requests
import json
from PIL import Image

st.set_page_config(page_title="Registration Details with respect to mfg year", page_icon="🌍",layout="wide", initial_sidebar_state="expanded", menu_items={
         'Get Help': 'https://www.extremelycoolapp.com/help',
         'Report a bug': "https://www.extremelycoolapp.com/bug",
         'About': "# This is a header. This is an *extremely* cool app!"
     })

st.markdown("# Spy Plane Registration Details based on Manufacture year")
st.sidebar.header("MFG Records")

st.write(
    """This page gives you the records of Planes with respect to it's manufacture year and if it's being used for surveillance or not """
)



image ='https://prd-sc101-cdn.rtx.com/-/media/ca/newsroom/images/2020/mission/u2--news-release.jpg?rev=b2c3dc181bf248c4911d930a20787ffc'

st.image(image, caption='Surveillance Aircraft')

image1='https://img.buzzfeed.com/buzzfeed-static/static/2017-07/29/11/asset/buzzfeed-prod-fastlane-01/sub-buzz-2347-1501342449-1.png?resize=990:713?output-quality=auto&output-format=auto&downsize=640:*'

st.image(image1, caption='Surveillance Aircraft routes over the US')


st.write(
    """A sample of the dataset: """
)
csv=pd.read_csv('https://damg7245assignment.s3.amazonaws.com/faa_registration.csv',nrows=10)
df = pd.DataFrame(csv)
df1=df[:5]
st.dataframe(df1, width=900, height=200)  # 👈 Draw the dataframe


ax = df['YEAR MFR'].value_counts()
st.write(
    """Here is a plot which shows the counts of planes Manufactured in years """
)
st.bar_chart(ax)




url = "https://api-o227c7m3tq-ue.a.run.app/planes_info_from_mfg_year"

n = st.selectbox(
     'Do you want to get results for surveillance aircrafts or all other aircrafts, 0 indicates surveillance and 1 indicates others',
     (0, 1))


if n==0: 
    a='surveil' 
else:
    a='other'


st.write('You selected:', a)

if n==0:
    year = st.selectbox(
     'Select from the dropdown list the year of manufacture of the aircraft',
     (2001,2003,2004,2005,2008,2009,2010,2011,2012, 2013))
else:
    year = st.select_slider(
     'Select from the slider list the year of manufacture of the aircraft',options=(1969,1970,1971,1972,1973,1974,1975,1976,1977,1978,1979,1980,1981,1982,1983,1984,1985,1986,1988,1989,1990,991,1992,1993,1994,1995,1996,1997,1998,1999,2000,2001,2002,2003,2004,2005,
    2006,2007,2008,2009,2010,2011,2012,2013,2014,2015))
st.write('You selected:', year)





def result(n,year):
    payload={'n': n, 'year' : year }
    url = f"https://api.anandpiyush.com/planes_info_from_manufacture_year?surveil={payload['n']}&year={payload['year']}"
    header = {}
    token='eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJrdW5qaXJhbWFuLmFAbm9ydGhlYXN0ZXJuLmVkdSIsImV4cCI6MTY1NjczMjUxN30.8PWZXrPtXADvTh1VBbjE10emO3j3hGR9LXcFlAqrgHs'
    header['Authorization'] = f"Bearer {token}"
    response = requests.request("GET", url, params=payload,  headers=header)
    data=json.loads(response.text)
    df2=pd.DataFrame(data)
    return df2


buttonstat=st.button('Get Results', disabled=False)
if buttonstat:
    st.write('The following are the records')
    df2=result(n,year)
    st.dataframe(df2)
    st.markdown("***")
    
    chart=df2['MFR_MDL_CODE'].value_counts()
    chart1=df2['ENG_MFR_MDL'].value_counts()
    st.markdown('Bar chart displaying counts of all manufacture models:')
    
    st.bar_chart(chart)

    st.markdown("***")

    st.markdown('Bar chart displaying counts of all engine manufacture models:')
    
    st.bar_chart(chart1)



