import dash
from  dash import dcc, html
from flask import Flask
import dash_bootstrap_components as dbc
import os
from dotenv import load_dotenv
from google.cloud import bigquery
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

# app init
server = Flask(__name__)
app = dash.Dash(__name__, server = server, external_stylesheets=[dbc.themes.UNITED, dbc.icons.BOOTSTRAP])

# read files
load_dotenv()
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/Users/piyush/Desktop/plane-detection-352701-90220d8b4de6.json'
client = bigquery.Client()
# sample_query1 = r"""
# with jui as (
#   SELECT
#   DATE(logtime) AS date,
#   COUNT(*) AS JUI
# FROM `plane-detection-352701.SPY_PLANE.logs`
# where Username = 'jui'
# GROUP BY DATE(logtime)
# ), abhi as (
#   SELECT
#   DATE(logtime) AS date,
#   COUNT(*) AS ABHI
# FROM `plane-detection-352701.SPY_PLANE.logs`
# where Username = 'abhijit'
# GROUP BY DATE(logtime)
# ), piyush as (
#   SELECT
#   DATE(logtime) AS date,
#   COUNT(*) AS PIYUSH
# FROM `plane-detection-352701.SPY_PLANE.logs`
# where Username = 'piyush'
# GROUP BY DATE(logtime)
# )

# SELECT j.date, JUI, ABHI, PIYUSH
# FROM jui j
# FULL OUTER JOIN abhi a ON j.date = a.date
# FULL OUTER JOIN piyush p ON j.date = p.date
# ORDER BY 1 ASC
# """
# df1 = client.query(sample_query1).to_dataframe()
sample_query2 = r"""SELECT * FROM `plane-detection-352701.SPY_PLANE.logs`"""
df2 = client.query(sample_query2).to_dataframe()
df2['logtime'] = pd.to_datetime(df2['logtime'])
df2['logtime'] = pd.to_datetime(df2['logtime'], format='%d/%m/%y %H:%M')
df2['logdate'] = pd.to_datetime(df2['logtime']).dt.strftime('%Y-%m-%d')
df1 = pd.crosstab(df2['logdate'],df2['Username']).reset_index()
# app component
Header_component = html.H1("API Analysis Dashboard", style = {'color':'darkcyan', 'text-align':'center', 'font-size': '72px'})

# app component
countfig = go.FigureWidget()

# countfig.add_scatter(name = "Jui", x= df1["date"], y = df1["JUI"], fill = "tonexty", line_shape = 'spline')
# countfig.add_scatter(name = "Abhijit", x= df1["date"], y = df1["ABHI"], fill = "tonexty", line_shape = 'spline')
# countfig.add_scatter(name = "Piyush", x= df1["date"], y = df1["PIYUSH"], fill = "tonexty", line_shape = 'spline')
countfig.add_scatter(name = "Jui", x= df1["logdate"], y = df1["chavan.ju@northeastern.edu"], fill = "tonexty", line_shape = 'spline')
countfig.add_scatter(name = "Abhijit", x= df1["logdate"], y = df1["kunjiraman.a@northeastern.edu"], fill = "tonexty", line_shape = 'spline')
countfig.add_scatter(name = "Piyush", x= df1["logdate"], y = df1["anand.pi@northeastern.edu"], fill = "tonexty", line_shape = 'spline')
countfig.add_scatter(name = "Prof Sri", x= df1["logdate"], y = df1["s.krishnamurthy@northeastern.edu"], fill = "tonexty", line_shape = 'spline')
countfig.add_scatter(name = "Parth", x= df1["logdate"], y = df1["shah.parth3@northeastern.edu"], fill = "tonexty", line_shape = 'spline')
countfig.add_scatter(name = "Zifeng", x= df1["logdate"], y = df1["jiang.zif@northeastern.edu"], fill = "tonexty", line_shape = 'spline')
countfig.add_scatter(name = "Adina", x= df1["logdate"], y = df1["adina.n@northeastern.edu"], fill = "tonexty", line_shape = 'spline')

countfig.update_layout(title = "User Time Line")

countfig_cum = go.FigureWidget()

# countfig_cum.add_scatter(name = "Jui", x= df1["date"], y = df1["JUI"].cumsum(), fill = "tonexty", line_shape = 'spline')
# countfig_cum.add_scatter(name = "Abhijit", x= df1["date"], y = df1["ABHI"].cumsum(), fill = "tonexty", line_shape = 'spline')
# countfig_cum.add_scatter(name = "Piyush", x= df1["date"], y = df1["PIYUSH"].cumsum(), fill = "tonexty", line_shape = 'spline')
countfig_cum.add_scatter(name = "Jui", x= df1["logdate"], y = df1["chavan.ju@northeastern.edu"].cumsum(), fill = "tonexty", line_shape = 'spline')
countfig_cum.add_scatter(name = "Abhijit", x= df1["logdate"], y = df1["kunjiraman.a@northeastern.edu"].cumsum(), fill = "tonexty", line_shape = 'spline')
countfig_cum.add_scatter(name = "Piyush", x= df1["logdate"], y = df1["anand.pi@northeastern.edu"].cumsum(), fill = "tonexty", line_shape = 'spline')
countfig_cum.add_scatter(name = "Prof Sri", x= df1["logdate"], y = df1["s.krishnamurthy@northeastern.edu"].cumsum(), fill = "tonexty", line_shape = 'spline')
countfig_cum.add_scatter(name = "Parth", x= df1["logdate"], y = df1["shah.parth3@northeastern.edu"].cumsum(), fill = "tonexty", line_shape = 'spline')
countfig_cum.add_scatter(name = "Zifeng", x= df1["logdate"], y = df1["jiang.zif@northeastern.edu"].cumsum(), fill = "tonexty", line_shape = 'spline')
countfig_cum.add_scatter(name = "Adina", x= df1["logdate"], y = df1["adina.n@northeastern.edu"].cumsum(), fill = "tonexty", line_shape = 'spline')

countfig_cum.update_layout(title = "Cumulative API Traffic")

indicator200 = go.FigureWidget(
    go.Indicator(
        mode = "gauge+number",
        value = df2['response_code'].value_counts()[200],
        title = {'text':'Successful Request'},
        gauge={"bar":{"color":"green"}}
    )
)
indicator400 = go.FigureWidget(
    go.Indicator(
        mode = "gauge+number",
        value = df2['response_code'].value_counts()[400],
        title = {'text':'Bad Request'},
        gauge={"bar":{"color":"yellow"}}
    )
)
indicator500 = go.FigureWidget(
    go.Indicator(
        mode = "gauge+number",
        value = df2['response_code'].value_counts()[500],
        title = {'text':'Internal Server Error'},
        gauge={"bar":{"color":"red"}}
    )
)

indicatordata = go.FigureWidget(
    px.pie(
        labels = ["Data", "Plot"],
        values = [df2[~df2['Endpoint'].str.contains("/plot/")].shape[0], df2[df2['Endpoint'].str.contains("/plot/")].shape[0]],
        hole = 0.4
    )
)
indicatordata.update_layout(title = "Data / Plot Request Distribution")

# app layout
app.layout = html.Div(
    [
        dbc.Row(
            Header_component
        ),
        dbc.Row(
            [dbc.Col(
                [dcc.Graph(figure=countfig)]
            ),dbc.Col(
                [dcc.Graph(figure=countfig_cum)]
            )]
        ),
        dbc.Row(
            [dbc.Col(
                [dcc.Graph(figure=indicator400)]
            ), dbc.Col(
                [dcc.Graph(figure=indicator200)]
            ), dbc.Col(
                [dcc.Graph(figure=indicator500)]
            )]
        ),
        dbc.Row(
            [dbc.Col(
                [dcc.Graph(figure=indicatordata)]
            )]
        )
        
    ]
)

# app run
app.run_server(debug=True, host='0.0.0.0')