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
from datetime import datetime

# app init
server = Flask(__name__)
app = dash.Dash(__name__, server = server, external_stylesheets=[dbc.themes.UNITED, dbc.icons.BOOTSTRAP])
app.title = 'Reports'
app._favicon = ("report.png")

load_dotenv()
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.getenv('BQ_KEY_JSON')
client = bigquery.Client()
sample_query2 = r"""SELECT * FROM `plane-detection-352701.SPY_PLANE.logs`"""
df2 = client.query(sample_query2).to_dataframe()
df2['logtime'] = pd.to_datetime(df2['logtime'])
df2['logtime'] = pd.to_datetime(df2['logtime'], format='%d/%m/%y %H:%M')
df2['logdate'] = pd.to_datetime(df2['logtime']).dt.strftime('%Y-%m-%d')
df1 = pd.crosstab(df2['logdate'],df2['Username']).reset_index()

# app component
now = datetime.now()
ts = now.strftime("%b %d %Y, %H:%M")

Header_component = html.H1("API Analysis Dashboard", style = {'color':'darkcyan', 'text-align':'center', 'font-size': '72px'})
Time_component = html.H5(f"Updated on {ts}", style = {'color':'black', 'text-align':'right'})
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
        labels = ["Data", "Plot", "Prediction"],
        values = [df2[~df2['Endpoint'].str.contains("/plot/")].shape[0], df2[df2['Endpoint'].str.contains("/plot/")].shape[0], df2[df2['Endpoint'].str.contains("/predict/")].shape[0]],
        hover_name = ["Data", "Plot", "Prediction"],
        hole = 0.4
    )
)
indicatordata.update_layout(title = "Request Type Distribution")

# app layout
app.layout = html.Div(
    [
        dbc.Row(
            Header_component,
            # Time_component
        ),
        dbc.Row(
            # Header_component,
            Time_component
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
# if __name__ == '__main__':
app.run_server(debug=True)
# app.run_server(debug=True, host='0.0.0.0', port = 8051)