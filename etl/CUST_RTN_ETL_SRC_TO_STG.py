#!/usr/bin/env python
# coding: utf-8

# In[ ]


#!/usr/bin/env python
# coding: utf-8


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

from IPython import get_ipython
from covid19dh import covid19
import pytz
#import tweepy as tw
import pandas as pd
import numpy as np
from teradataml.dataframe.dataframe import DataFrame, in_schema
from teradataml.dataframe.copy_to import copy_to_sql
from teradataml.context.context import create_context, remove_context, get_connection, get_context
from teradataml.options.display import display
from teradataml.dataframe.fastload import fastload
#import tdconnect
import getpass
import datetime
from datetime import date
from datetime import datetime, timedelta
from datetime import datetime
from teradataml.context.context import *
import ssl
ssl._create_default_https_context = ssl._create_unverified_context
import params
con = create_context(host=params.MyHost, username=params.MyUser, password=params.Password,temp_database_name=params.SchemaName,logmech=params.LogMech)

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

#############################################################
# 2) Covid Cases
#############################################################

from datetime import datetime
import datetime

url = 'https://raw.githubusercontent.com/nytimes/covid-19-data/master/us-counties.csv'
df = pd.read_csv(url)
df['current_dttm'] = datetime.datetime.today()
df = df.rename(columns={'date': 'date_key'})


copy_to_sql(df = df, table_name = "STG_covid19_stats", schema_name=params.SchemaName , primary_index = ['date_key'], if_exists = 'replace')

from datetime import datetime
datetime.utcnow()
dateTimeObj = pytz.utc.localize(datetime.utcnow()).astimezone(pytz.timezone('US/Pacific'))
timestampStr = dateTimeObj.strftime("%d-%b-%Y (%H:%M:%S.%f)")
print("Covid Cases Finished!  " + timestampStr)


#############################################################
# 3) Covid Projections
#############################################################
from urllib.request import urlopen
import requests
from pathlib import Path
url = 'https://ihmecovid19storage.blob.core.windows.net/latest/ihme-covid19.zip'
content = requests.get(url)

# unzip the content
from io import BytesIO
from zipfile import ZipFile
f = ZipFile(BytesIO(content.content))

from datetime import datetime
import datetime

cleaned_list = [i for i in f.namelist() if '.csv' in i]
for i in cleaned_list:
    url = 'https://ihmecovid19storage.blob.core.windows.net/latest/ihme-covid19.zip'
    z = urlopen(url)
    myzip = ZipFile(BytesIO(z.read())).extract(i)
    df = pd.read_csv(myzip)
    p = Path(i)
    df['Path_Update_Dt'] = p.parts[0]
    df['current_dttm'] = datetime.datetime.today()
    
    if 'Reference_hospitalization_all_locs' in i:
        copy_to_sql(df = df, table_name = 'STG_Hospitalization_all_locs', schema_name=params.SchemaName, index=False, if_exists="replace")
    if 'Summary_stats_all_locs' in i:
        copy_to_sql(df = df, table_name = 'STG_Summary_stats_all_locs', schema_name=params.SchemaName, index=False, if_exists="replace")

        
from datetime import datetime        
datetime.utcnow()
dateTimeObj = pytz.utc.localize(datetime.utcnow()).astimezone(pytz.timezone('US/Pacific'))
timestampStr = dateTimeObj.strftime("%d-%b-%Y (%H:%M:%S.%f)")
print("Covid Projections Finished!  " + timestampStr)


#############################################################
# 4)  Google Trends
#############################################################
import pytrends
from pytrends.request import TrendReq
import datetime

strtdt = datetime.date(2020, 1, 1)
enddt = date.today()
timeframe=strtdt.strftime("%Y-%m-%d")+' '+enddt.strftime("%Y-%m-%d") 

pytrends = TrendReq(hl='en-US', tz=360)


a='covid + coronavirus'
cat=''
kw_list = [a]
pytrends.build_payload(kw_list, timeframe=timeframe,geo='US') 
iot=pytrends.interest_over_time()
iot = iot.rename(columns={a: 'Metric_value'})
iot['Trend_Name']='Covid Search'
iot['Metric_Name']='Covid'
iot['current_dttm'] = datetime.datetime.today()
iot['Keyword_List'] =a
iot['Cat_CD']=cat
iot['Type'] ='Interest over time'
copy_to_sql(df = iot, table_name = "STG_Google_Search_IOT", schema_name=params.SchemaName, index=True, if_exists="replace") 

a=''
cat='1085'
kw_list = [a]
pytrends.build_payload(kw_list, cat=cat, timeframe=timeframe,geo='US')
iot=pytrends.interest_over_time()
iot = iot.rename(columns={a: 'Metric_value'})
iot['Trend_Name']='Movie Listings & Theater Showtimes'
iot['Metric_Name']='Interest over time'
iot['current_dttm'] = datetime.datetime.today()
iot['Keyword_List'] =a
iot['Cat_CD']=cat
iot['Type'] ='Interest over time'
copy_to_sql(df = iot, table_name = "STG_Google_Search_IOT", schema_name=params.SchemaName, index=True, if_exists="append") 

a=''
cat='208'
kw_list = [a]
pytrends.build_payload(kw_list, cat=cat, timeframe=timeframe,geo='US')
iot=pytrends.interest_over_time()
iot['Trend_Name']='Tourist Destinations'
iot = iot.rename(columns={a: 'Metric_value'})
iot['Metric_Name']='Interest over time'
iot['current_dttm'] = datetime.datetime.today()
iot['Keyword_List'] =a
iot['Cat_CD']=cat
iot['Type'] ='Interest over time'
copy_to_sql(df = iot, table_name = "STG_Google_Search_IOT", schema_name=params.SchemaName, index=True, if_exists="append") 


from datetime import datetime
datetime.utcnow()
dateTimeObj = pytz.utc.localize(datetime.utcnow()).astimezone(pytz.timezone('US/Pacific'))
timestampStr = dateTimeObj.strftime("%d-%b-%Y (%H:%M:%S.%f)")
print("Google Trends Finished!  " + timestampStr)


#############################################################
# 5)  Google Mobility
#############################################################
import datetime
url = 'https://www.gstatic.com/covid19/mobility/Global_Mobility_Report.csv'
df = pd.read_csv(url, dtype='unicode')

df['current_dttm'] = datetime.datetime.today()
df = df.rename(columns={'date': 'date_key'})

copy_to_sql(df = df, table_name = "STG_Google_Mobility", schema_name=params.SchemaName, primary_index = ['date_key'], if_exists = 'replace')

from datetime import datetime
datetime.utcnow()
dateTimeObj = pytz.utc.localize(datetime.utcnow()).astimezone(pytz.timezone('US/Pacific'))
timestampStr = dateTimeObj.strftime("%d-%b-%Y (%H:%M:%S.%f)")
print("Google Mobility Finished!  " + timestampStr)

#############################################################
# 6)  COVID Datahub Level 3
#############################################################
import datetime

df = covid19("USA", level = 3, start = date(2020,1,1), verbose = False)
df['current_dttm'] = datetime.datetime.today()
df = df.rename(columns={'date': 'date_key'})

copy_to_sql(df = df, table_name = "STG_COVID19_Datahub_LVL3", schema_name=params.SchemaName, primary_index = ['date_key'], if_exists = 'replace')

from datetime import datetime
datetime.utcnow()
dateTimeObj = pytz.utc.localize(datetime.utcnow()).astimezone(pytz.timezone('US/Pacific'))
timestampStr = dateTimeObj.strftime("%d-%b-%Y (%H:%M:%S.%f)")
print("COVID Datahub Level 3 Finished!  " + timestampStr)

#############################################################
# 7)  COVID Datahub Level 2
#############################################################
import datetime

df = covid19("USA", level = 2, start = date(2020,1,1), verbose = False)
df['current_dttm'] = datetime.datetime.today()
df = df.rename(columns={'date': 'date_key'})

copy_to_sql(df = df, table_name = "STG_COVID19_Datahub_LVL2", schema_name=params.SchemaName, primary_index = ['date_key'], if_exists = 'replace')

from datetime import datetime
datetime.utcnow()
dateTimeObj = pytz.utc.localize(datetime.utcnow()).astimezone(pytz.timezone('US/Pacific'))
timestampStr = dateTimeObj.strftime("%d-%b-%Y (%H:%M:%S.%f)")
print("COVID Datahub Level 2 Finished!  " + timestampStr)


#############################################################
# 8)  Labor Stats Data
#############################################################
import json
import requests
import datetime
#data = json.dumps({"seriesid": ['CUSR0000SA0','SUUR0000SA0'],"startyear":"2019", "endyear":"2020"})
headers = {'Content-type': 'application/json'}
data = json.dumps({"seriesid": ['CUSR0000SA0','LNS13000000','LNS14000000'],"startyear":"2018", "endyear":"2020"})
p = requests.post('https://api.bls.gov/publicAPI/v2/timeseries/data/', data=data, headers=headers)
json_data = json.loads(p.text)

df0 = pd.DataFrame(json_data['Results']['series'][0]['data'])
df0 = df0.drop(['footnotes'], axis=1)
df0.loc[:,'footnotes']="Consumer Price Index"
df0.loc[:,'series_id']="CUSR0000SA0"
df0['current_dttm'] = datetime.datetime.today()
df0.rename(columns={'year': 'Year_Key', 'periodName': 'Period_Month', 'value': 'Metric_Val', 'period': 'Period_Key'}, inplace=True)
copy_to_sql(df = df0, table_name = "STG_Labor_Stats_CUSR0000SA0", schema_name = params.SchemaName, index=False, if_exists="replace")

df1 = pd.DataFrame(json_data['Results']['series'][1]['data'])
df1 = df1.drop(['footnotes'], axis=1)
df1.loc[:,'footnotes']="Unemployment Level"
df1.loc[:,'series_id']="LNS13000000"
df1['current_dttm'] = datetime.datetime.today()
df1.rename(columns={'year': 'Year_Key', 'periodName': 'Period_Month', 'value': 'Metric_Val', 'period': 'Period_Key'}, inplace=True)
copy_to_sql(df = df1, table_name = "STG_Labor_Stats_LNS13000000", schema_name = params.SchemaName, index=False, if_exists="replace")

df2 = pd.DataFrame(json_data['Results']['series'][2]['data'])
df2 = df2.drop(['footnotes'], axis=1)
df2.loc[:,'footnotes']="Unemployment Rate"
df2.loc[:,'series_id']="LNS14000000"
df2['current_dttm'] = datetime.datetime.today()
df2.rename(columns={'year': 'Year_Key', 'periodName': 'Period_Month', 'value': 'Metric_Val', 'period': 'Period_Key'}, inplace=True)
copy_to_sql(df = df2, table_name = "STG_Labor_Stats_LNS14000000", schema_name = params.SchemaName, index=False, if_exists="replace")

from datetime import datetime
datetime.utcnow()
dateTimeObj = pytz.utc.localize(datetime.utcnow()).astimezone(pytz.timezone('US/Pacific'))
timestampStr = dateTimeObj.strftime("%d-%b-%Y (%H:%M:%S.%f)")
print("Labor Stats Finished!  " + timestampStr)


#############################################################
# 9) Fuel Production
#############################################################
import datetime

url = 'https://www.eia.gov/dnav/pet/xls/PET_CONS_WPSUP_K_4.xls'
df = pd.read_excel(url, sheet_name = "Data 1", skiprows=[0,1])
df['current_dttm'] = datetime.datetime.today()
df = df.rename(columns={'Date': 'date_key'})

copy_to_sql(df = df, table_name = "STG_Fuel_Production", schema_name = params.SchemaName, if_exists = 'replace')

from datetime import datetime
datetime.utcnow()
dateTimeObj = pytz.utc.localize(datetime.utcnow()).astimezone(pytz.timezone('US/Pacific'))
timestampStr = dateTimeObj.strftime("%d-%b-%Y (%H:%M:%S.%f)")
print("Fuel Production Finished!  " + timestampStr)

#############################################################
# 10) TSA Travel
#############################################################

import requests
from bs4 import BeautifulSoup
from requests_html import HTMLSession
import datetime

session = HTMLSession()
url = 'https://www.tsa.gov/coronavirus/passenger-throughput'
r = session.get(url)
soup=BeautifulSoup(r.html.html,'html.parser')
stat_table = soup.find('table')

df = pd.read_html(str(stat_table),header=0)[0]

df.columns = ('Travel_Date',
              'TravelThroughPut', 
              'TravelThroughPutLastYear')

df['current_dttm'] = datetime.datetime.today()
        
copy_to_sql(df = df, table_name = "STG_TSA_TRAVEL", schema_name = params.SchemaName, if_exists = 'replace')

from datetime import datetime
datetime.utcnow()
dateTimeObj = pytz.utc.localize(datetime.utcnow()).astimezone(pytz.timezone('US/Pacific'))
timestampStr = dateTimeObj.strftime("%d-%b-%Y (%H:%M:%S.%f)")
print("TSA Travel Finished!  " + timestampStr)


#############################################################
# 11) CENSUS Data
#############################################################

import datetime
url = 'https://www.census.gov/econ/currentdata/export/csv?programCode=RESCONST&timeSlotType=12&startYear=2018&endYear=2020&categoryCode=APERMITS&dataTypeCode=TOTAL&geoLevelCode=US&adjusted=yes&errorData=no&internal=false'
hd = pd.read_csv(url, sep='~',  header=None, nrows=6, keep_default_na=False)
desc=''
for (idx, row) in hd.iterrows():
	desc=desc+row.loc[0]+'\n' 

df = pd.read_csv(url, skiprows=6, keep_default_na=False, dtype={'Value': str})
df['Metric_name']='Housing Starts in 1000s'
df['Data_source_desc']=desc
df['current_dttm'] = datetime.datetime.today()

##df = df.rename(columns={'date': 'date_key'})

copy_to_sql(df = df, table_name = "STG_US_CENSUS_SURVEY", schema_name=params.SchemaName, primary_index = ['Period'], if_exists = 'replace')


from datetime import datetime
datetime.utcnow()
dateTimeObj = pytz.utc.localize(datetime.utcnow()).astimezone(pytz.timezone('US/Pacific'))
timestampStr = dateTimeObj.strftime("%d-%b-%Y (%H:%M:%S.%f)")
print("Census Data Finished!  " + timestampStr)


#############################################################
# 11) Consumer Sentiment Index
#############################################################

from urllib.parse import quote_plus
import datetime
url = 'http://www.sca.isr.umich.edu/files/tbcics.csv'
res = requests.get(url)
df = pd.read_csv(BytesIO(res.content),skiprows=[0,1,2,3],dtype = {'Unnamed: 1':str})
df = df[['Unnamed: 0','Unnamed: 1','Unnamed: 4']]
df = df.dropna()
df.columns = ('Month','Year','Consumer_Sentiment_Index')

df['current_dttm'] = datetime.datetime.today()

copy_to_sql(df = df, table_name = "STG_Consumer_Sentiment_Index", schema_name=params.SchemaName, if_exists = 'replace')
from datetime import datetime
datetime.utcnow()
dateTimeObj = pytz.utc.localize(datetime.utcnow()).astimezone(pytz.timezone('US/Pacific'))
timestampStr = dateTimeObj.strftime("%d-%b-%Y (%H:%M:%S.%f)")
print("Consumer Sentiment Index!  " + timestampStr)


#############################################################
# Printing the Load Summary Stats
#############################################################
remove_context()
con = create_context(host=params.MyHost, username=params.MyUser, password=params.Password,temp_database_name=params.SchemaName,logmech=params.LogMech)

pda = pd.read_sql('DATABASE '+params.SchemaName,con)

query = "SELECT 'Python' as Process_Name, 'Staging' as Table_Type, 'STG_Hospitalization_all_locs' as TableName, count(*) as Records_Processed, max(current_dttm) as Process_Dttm FROM STG_Hospitalization_all_locs GROUP BY 1,2,3 \
UNION \
SELECT 'Python' as Process_Name, 'Staging' as Table_Type, 'STG_Summary_stats_all_locs' as TableName, count(*) as Records_Processed, max(current_dttm) as Process_Dttm FROM STG_Summary_stats_all_locs GROUP BY 1,2,3 \
UNION \
SELECT 'Python' as Process_Name, 'Staging' as Table_Type, 'STG_covid19_stats' as TableName, count(*) as Records_Processed, max(current_dttm) as Process_Dttm FROM STG_covid19_stats GROUP BY 1,2,3 \
UNION \
SELECT 'Python' as Process_Name, 'Staging' as Table_Type, 'STG_Google_Search_IOT' as TableName, count(*) as Records_Processed, max(current_dttm) as Process_Dttm FROM STG_Google_Search_IOT GROUP BY 1,2,3 \
UNION \
SELECT 'Python' as Process_Name, 'Staging' as Table_Type, 'STG_Google_Mobility' as TableName, count(*) as Records_Processed, max(current_dttm) as Process_Dttm FROM STG_Google_Mobility GROUP BY 1,2,3 \
UNION \
SELECT 'Python' as Process_Name, 'Staging' as Table_Type, 'STG_Labor_Stats_LNS13000000' as TableName, count(*) as Records_Processed, max(current_dttm) as Process_Dttm FROM STG_Labor_Stats_LNS13000000 GROUP BY 1,2,3 \
UNION \
SELECT 'Python' as Process_Name, 'Staging' as Table_Type, 'STG_Labor_Stats_LNS14000000' as TableName, count(*) as Records_Processed, max(current_dttm) as Process_Dttm FROM STG_Labor_Stats_LNS14000000 GROUP BY 1,2,3 \
UNION \
SELECT 'Python' as Process_Name, 'Staging' as Table_Type, 'STG_Labor_Stats_CUSR0000SA0' as TableName, count(*) as Records_Processed, max(current_dttm) as Process_Dttm FROM STG_Labor_Stats_CUSR0000SA0 GROUP BY 1,2,3 \
UNION \
SELECT 'Python' as Process_Name, 'Staging' as Table_Type, 'STG_COVID19_Datahub_LVL2' as TableName, count(*) as Records_Processed, max(current_dttm) as Process_Dttm FROM STG_COVID19_Datahub_LVL2 GROUP BY 1,2,3 \
UNION \
SELECT 'Python' as Process_Name, 'Staging' as Table_Type, 'STG_COVID19_Datahub_LVL3' as TableName, count(*) as Records_Processed, max(current_dttm) as Process_Dttm FROM STG_COVID19_Datahub_LVL3 GROUP BY 1,2,3 \
UNION \
SELECT 'Python' as Process_Name, 'Staging' as Table_Type, 'STG_Fuel_Production' as TableName, count(*) as Records_Processed, max(current_dttm) as Process_Dttm FROM STG_Fuel_Production GROUP BY 1,2,3 \
UNION \
SELECT 'Python' as Process_Name, 'Staging' as Table_Type, 'STG_TSA_TRAVEL' as TableName, count(*) as Records_Processed, max(current_dttm) as Process_Dttm FROM STG_TSA_TRAVEL GROUP BY 1,2,3 \
UNION \
SELECT 'Python' as Process_Name, 'Staging' as Table_Type, 'STG_Consumer_Sentiment_Index' as TableName, count(*) as Records_Processed, max(current_dttm) as Process_Dttm FROM STG_Consumer_Sentiment_Index GROUP BY 1,2,3;"


#Fetch the data from Teradata using Pandas Dataframe
pda = pd.read_sql(query,con)
copy_to_sql(df = pda, table_name = "ETL_Indicator_Proj_Audit", schema_name=params.SchemaName, if_exists = 'append')
print(pda)