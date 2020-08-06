# In[ ]


#!/usr/bin/env python
# coding: utf-8

# %%
#############################################################
# Libraries
#############################################################
#pip install ipython
#pip install --upgrade covid19dh
#pip install numpy --upgrade
#pip install requests
#pip3 install tabulate
#pip install xlrd
#pip3 install requests-html
#pip install html5lib
#pip install teradataml
#pip install pytrends

import datetime as dt
from io import BytesIO
import json
from urllib.request import urlopen
from zipfile import ZipFile

from bs4 import BeautifulSoup
from covid19dh import covid19
import pandas as pd
import pytrends
from pytrends.request import TrendReq
import pytz
import requests as rq
from requests_html import HTMLSession
#import tdconnect
from teradataml.context.context import create_context, remove_context
from teradataml.dataframe.copy_to import copy_to_sql
# from teradataml.dataframe.fastload import fastload
#import tweepy as tw

import params
from common import print_complete

con = create_context(
    host=params.MyHost, username=params.MyUser, password=params.Password,
    temp_database_name=params.SchemaName, logmech=params.LogMech
)

# %%
#############################################################
# 1) Apple Mobility
#############################################################
#import datetime

#url = 'https://covid19-static.cdn-apple.com/covid19-mobility-data/2010HotfixDev23/v3/en-us/applemobilitytrends-2020-06-18.csv'
#df = pd.read_csv(url)


#df['MergedColumn'] = df[df.columns[0:]].apply(
#    lambda x: '|'.join(x.dropna().astype(str)),
#    axis=1
#)


#df.drop(df.columns.difference(['MergedColumn']), 1, inplace=True)
#df['current_dttm'] = datetime.datetime.today()

#fastload(df = df, table_name = "STG_Apple_Mobility", schema_name=params.SchemaName, if_exists = 'replace')

#from datetime import datetime
#datetime.utcnow()
#dateTimeObj = pytz.utc.localize(datetime.utcnow()).astimezone(pytz.timezone('US/Pacific'))
#timestampStr = dateTimeObj.strftime("%d-%b-%Y (%H:%M:%S.%f)")
#print("Apple Mobility Finished!  " + timestampStr)

# %%
#############################################################
# 2) Covid Cases
#############################################################
url = (
    'https://raw.githubusercontent.com/'
    'nytimes/covid-19-data/master/us-counties.csv'
)
df = pd.read_csv(url)
df['current_dttm'] = dt.datetime.today()
df = df.rename(columns={'date': 'date_key'})

copy_to_sql(
    df=df, table_name="STG_covid19_stats", schema_name=params.SchemaName,
    primary_index=['date_key'], if_exists = 'replace'
)

print_complete("Covid Cases")

# %%
#############################################################
# 3) Covid Projections
#############################################################
from pathlib import Path
url = (
    'https://ihmecovid19storage.blob.core.windows.net/latest/ihme-covid19.zip'
)
res = rq.get(url)

# unzip the content
f = ZipFile(BytesIO(res.content))

for i in (i for i in f.namelist() if '.csv' in i):
    myzip = f.extract(i)
    df = pd.read_csv(myzip)
    p = Path(i)
    df['Path_Update_Dt'] = p.parts[0]
    df['current_dttm'] = dt.datetime.today()

    if 'Reference_hospitalization_all_locs' in i:
        copy_to_sql(
            df=df, table_name='STG_Hospitalization_all_locs',
            schema_name=params.SchemaName, index=False, if_exists="replace"
        )
    elif 'Summary_stats_all_locs' in i:
        copy_to_sql(
            df=df, table_name='STG_Summary_stats_all_locs',
            schema_name=params.SchemaName, index=False, if_exists="replace"
        )

print_complete('Covid Projections')

# %%
#############################################################
# 4)  Google Trends
#############################################################

strtdt = dt.date(2020, 1, 1)
enddt = dt.date.today()
timeframe = strtdt.strftime("%Y-%m-%d")+' '+enddt.strftime("%Y-%m-%d")

pytrends = TrendReq(hl='en-US', tz=360)

a = 'covid + coronavirus'
cat = ''
kw_list = [a]
pytrends.build_payload(kw_list, timeframe=timeframe,geo='US')
iot = pytrends.interest_over_time()
iot = iot.rename(columns={a: 'Metric_value'})
iot['Trend_Name'] = 'Covid Search'
iot['Metric_Name'] = 'Covid'
iot['current_dttm'] = dt.datetime.today()
iot['Keyword_List'] = a
iot['Cat_CD'] = cat
iot['Type'] = 'Interest over time'
copy_to_sql(
    df=iot, table_name="STG_Google_Search_IOT", schema_name=params.SchemaName,
    index=True, if_exists="replace"
)

a = ''
cat = '1085'
kw_list = [a]
pytrends.build_payload(kw_list, cat=cat, timeframe=timeframe,geo='US')
iot = pytrends.interest_over_time()
iot = iot.rename(columns={a: 'Metric_value'})
iot['Trend_Name'] = 'Movie Listings & Theater Showtimes'
iot['Metric_Name'] = 'Interest over time'
iot['current_dttm'] = dt.datetime.today()
iot['Keyword_List'] = a
iot['Cat_CD'] = cat
iot['Type'] ='Interest over time'
copy_to_sql(
    df=iot, table_name="STG_Google_Search_IOT", schema_name=params.SchemaName,
    index=True, if_exists="append"
)

a = ''
cat = '208'
kw_list = [a]
pytrends.build_payload(kw_list, cat=cat, timeframe=timeframe,geo='US')
iot = pytrends.interest_over_time()
iot['Trend_Name'] = 'Tourist Destinations'
iot = iot.rename(columns={a: 'Metric_value'})
iot['Metric_Name'] = 'Interest over time'
iot['current_dttm'] = dt.datetime.today()
iot['Keyword_List'] = a
iot['Cat_CD'] = cat
iot['Type'] ='Interest over time'
copy_to_sql(
    df=iot, table_name="STG_Google_Search_IOT", schema_name=params.SchemaName,
    index=True, if_exists="append"
)

print_complete('Google Trends')

# %%
#############################################################
# 5)  Google Mobility
#############################################################
url = 'https://www.gstatic.com/covid19/mobility/Global_Mobility_Report.csv'
df = pd.read_csv(url, dtype='unicode')

df['current_dttm'] = dt.datetime.today()
df = df.rename(columns={'date': 'date_key'})

copy_to_sql(
    df=df, table_name="STG_Google_Mobility", schema_name=params.SchemaName,
    primary_index=['date_key'], if_exists='replace'
)

print_complete('Google Mobility')

# %%
#############################################################
# 6)  COVID Datahub Level 3
#############################################################
df = covid19("USA", level=3, start=dt.date(2020,1,1), verbose=False)
df['current_dttm'] = dt.datetime.today()
df = df.rename(columns={'date': 'date_key'})

copy_to_sql(
    df=df, table_name="STG_COVID19_Datahub_LVL3", if_exists='replace',
    schema_name=params.SchemaName, primary_index=['date_key']
)

print_complete('COVID Datahub Level 3')

# %%
#############################################################
# 7)  COVID Datahub Level 2
#############################################################
df = covid19("USA", level=2, start=dt.date(2020,1,1), verbose=False)
df['current_dttm'] = dt.datetime.today()
df = df.rename(columns={'date': 'date_key'})

copy_to_sql(
    df=df, table_name="STG_COVID19_Datahub_LVL2", if_exists = 'replace',
    schema_name=params.SchemaName, primary_index=['date_key'])

print_complete('COVID Datahub Level 2')

# %%
#############################################################
# 8)  Labor Stats Data
#############################################################
#data = json.dumps({"seriesid": ['CUSR0000SA0','SUUR0000SA0'],"startyear":"2019", "endyear":"2020"})
headers = {'Content-type': 'application/json'}
data = json.dumps({
    "seriesid": ['CUSR0000SA0','LNS13000000','LNS14000000'],
    "startyear": "2018",
    "endyear": "2020"
})
p = rq.post(
    'https://api.bls.gov/publicAPI/v2/timeseries/data/',
    data=data, headers=headers
)
json_data = json.loads(p.text)

df0 = pd.DataFrame(json_data['Results']['series'][0]['data'])
df0 = df0.drop(['footnotes'], axis=1)
df0.loc[:,'footnotes'] = "Consumer Price Index"
df0.loc[:,'series_id'] = "CUSR0000SA0"
df0['current_dttm'] = dt.datetime.today()
df0.rename(
    columns={
        'year': 'Year_Key',
        'periodName':
        'Period_Month',
        'value': 'Metric_Val',
        'period': 'Period_Key'
    }, inplace=True
)
copy_to_sql(
    df=df0, table_name="STG_Labor_Stats_CUSR0000SA0",
    schema_name=params.SchemaName, index=False, if_exists="replace"
)

df1 = pd.DataFrame(json_data['Results']['series'][1]['data'])
df1 = df1.drop(['footnotes'], axis=1)
df1.loc[:,'footnotes'] = "Unemployment Level"
df1.loc[:,'series_id'] = "LNS13000000"
df1['current_dttm'] = dt.datetime.today()
df1.rename(
    columns={
        'year': 'Year_Key',
        'periodName': 'Period_Month',
        'value': 'Metric_Val',
        'period': 'Period_Key'
    }, inplace=True
)
copy_to_sql(
    df=df1, table_name="STG_Labor_Stats_LNS13000000",
    schema_name=params.SchemaName, index=False, if_exists="replace"
)

df2 = pd.DataFrame(json_data['Results']['series'][2]['data'])
df2 = df2.drop(['footnotes'], axis=1)
df2.loc[:,'footnotes'] = "Unemployment Rate"
df2.loc[:,'series_id'] = "LNS14000000"
df2['current_dttm'] = dt.datetime.today()
df2.rename(
    columns={
        'year': 'Year_Key',
        'periodName': 'Period_Month',
        'value': 'Metric_Val',
        'period': 'Period_Key'
    }, inplace=True
)
copy_to_sql(
    df=df2, table_name="STG_Labor_Stats_LNS14000000",
    schema_name=params.SchemaName, index=False, if_exists="replace"
)

print_complete('Labor Stats')

# %%
#############################################################
# 9) Fuel Production
#############################################################
url = 'https://www.eia.gov/dnav/pet/xls/PET_CONS_WPSUP_K_4.xls'
df = pd.read_excel(url, sheet_name = "Data 1", skiprows=[0,1])
df['current_dttm'] = dt.datetime.today()
df = df.rename(columns={'Date': 'date_key'})

copy_to_sql(
    df=df, table_name="STG_Fuel_Production", schema_name=params.SchemaName,
    if_exists='replace'
)

print_complete('Fuel Production')

# %%
#############################################################
# 10) TSA Travel
#############################################################
session = HTMLSession()
url = 'https://www.tsa.gov/coronavirus/passenger-throughput'
r = session.get(url)
soup = BeautifulSoup(r.html.html, 'html.parser')
stat_table = soup.find('table')

df = pd.read_html(str(stat_table), header=0)[0]

df.columns = (
    'Travel_Date',
    'TravelThroughPut',
    'TravelThroughPutLastYear'
)

df['current_dttm'] = dt.datetime.today()

copy_to_sql(
    df=df, table_name="STG_TSA_TRAVEL", schema_name=params.SchemaName,
    if_exists='replace'
)

print_complete('TSA Travel')

# %%
#############################################################
# 11) CENSUS Data
#############################################################
url = (
    'https://www.census.gov/econ/currentdata/export/csv?'
    'programCode=RESCONST&timeSlotType=12&startYear=2018&endYear=2020'
    '&categoryCode=APERMITS&dataTypeCode=TOTAL&geoLevelCode=US&adjusted=yes'
    '&errorData=no&internal=false'
)
hd = pd.read_csv(url, sep='~',  header=None, nrows=6, keep_default_na=False)
desc = ''
for (idx, row) in hd.iterrows():
	desc = desc + row.loc[0]+'\n'

df = pd.read_csv(url, skiprows=6, keep_default_na=False, dtype={'Value': str})
df['Metric_name'] = 'Housing Starts in 1000s'
df['Data_source_desc'] = desc
df['current_dttm'] = dt.datetime.today()

##df = df.rename(columns={'date': 'date_key'})

copy_to_sql(
    df=df, table_name="STG_US_CENSUS_SURVEY", schema_name=params.SchemaName,
    primary_index=['Period'], if_exists = 'replace'
)

print_complete('Census Data')

# %%
#############################################################
# 11) Consumer Sentiment Index
#############################################################
url = 'http://www.sca.isr.umich.edu/files/tbcics.csv'
res = rq.get(url, proxies=params.proxies)
df = pd.read_csv(
    BytesIO(res.content),
    skiprows=[0,1,2,3],
    dtype = {'Unnamed: 1':str}
)
df = df[['Unnamed: 0','Unnamed: 1','Unnamed: 4']]
df = df.dropna()
df.columns = ('Month','Year','Consumer_Sentiment_Index')

df['current_dttm'] = dt.datetime.today()

copy_to_sql(
    df=df, table_name="STG_Consumer_Sentiment_Index",
    schema_name=params.SchemaName, if_exists='replace'
)

print_complete('Consumer Sentiment Index')

# %%
#############################################################
# Printing the Load Summary Stats
#############################################################
remove_context()
con = create_context(
    host=params.MyHost, username=params.MyUser, password=params.Password,
    temp_database_name=params.SchemaName, logmech=params.LogMech
)

pda = pd.read_sql('DATABASE '+params.SchemaName,con)

query = '\n\nUNION\n\n'.join(
    "SELECT\n"
    "  'Python' as Process_Name,\n"
    "  'Staging' as Table_Type,\n"
    "  '{}' as TableName,\n".format(tbl) +
    "  count(*) as Records_Processed,\n"
    "  max(current_dttm) as Process_Dttm\n"
    "FROM {}\n".format(tbl) +
    "GROUP BY 1,2,3\n\n"
    for tbl in (
        'STG_Hospitalization_all_locs', 'STG_Summary_stats_all_locs',
        'STG_covid19_stats', 'STG_Google_Search_IOT', 'STG_Google_Mobility',
        'STG_Labor_Stats_LNS13000000', 'STG_Labor_Stats_LNS14000000',
        'STG_Labor_Stats_CUSR0000SA0', 'STG_COVID19_Datahub_LVL2',
        'STG_COVID19_Datahub_LVL3', 'STG_Fuel_Production', 'STG_TSA_TRAVEL',
        'STG_Consumer_Sentiment_Index'
    )
)

#Fetch the data from Teradata using Pandas Dataframe
pda = pd.read_sql(query,con)
copy_to_sql(
    df=pda, table_name="ETL_Indicator_Proj_Audit",
    schema_name=params.SchemaName, if_exists='append'
)
print(pda)

# %%
