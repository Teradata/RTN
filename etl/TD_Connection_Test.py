#!/usr/bin/env python
# coding: utf-8

# In[ ]


#!/usr/bin/env python
# coding: utf-8

#############################################################
# Libraries
#############################################################
pip install ipython
pip install --upgrade covid19dh
pip install numpy --upgrade
pip install requests
pip3 install tabulate
pip install xlrd
pip3 install requests-html
pip install html5lib
pip install teradataml
pip install pytrends
from teradataml.dataframe.dataframe import DataFrame, in_schema
from teradataml.dataframe.copy_to import copy_to_sql
from teradataml.context.context import create_context, remove_context, get_connection, get_context
from teradataml.options.display import display
from teradataml.dataframe.fastload import fastload
from teradataml.context.context import *
import pandas as pd
import pytrends
from pytrends.request import TrendReq
import datetime
from datetime import date
from urllib.request import urlopen
import requests
from pathlib import Path
import getpass
from covid19dh import covid19
import json
from requests_html import HTMLSession
import ssl
ssl._create_default_https_context = ssl._create_unverified_context
import params
con = create_context(host=params.MyHost, username=params.MyUser, password=params.Password,temp_database_name=params.SchemaName,logmech=params.LogMech)

#############################################################
# Teradata Query Test
#############################################################
query = "Select * from DBC.DBCInfo"
pda = pd.read_sql(query,con)
print("Teradata Connection Successful!")


#############################################################
# Teradata Create Table Test
#############################################################
pda = pd.read_sql("create table zzz_test (x char(1));",con)
pda = pd.read_sql("drop table zzz_test;",con)
print("Table creation test successful!")


#############################################################
# Teradata Create View Test
#############################################################
pda = pd.read_sql("create view zzz_test_v as select * from DBC.DBCINFO;",con)
pda = pd.read_sql("drop view zzz_test_v;",con)
print("View creation test successful!")


#############################################################
# Teradata Create Procedure Test
#############################################################
pda = pd.read_sql("replace procedure zzz_test() begin end;",con)
pda = pd.read_sql("drop procedure zzz_test;",con)
print("Procedure creation test successful!")


#############################################################
# Teradata URL Access Test
#############################################################
url = 'https://raw.githubusercontent.com/nytimes/covid-19-data/master/us-counties.csv'
df = pd.read_csv(url)
print("Covid Cases Data connection successful!")

url = 'https://ihmecovid19storage.blob.core.windows.net/latest/ihme-covid19.zip'
content = requests.get(url)
print("Covid Projections Data connection successful!")

strtdt = datetime.date(2020, 1, 1)
enddt = date.today()
timeframe=strtdt.strftime("%Y-%m-%d")+' '+enddt.strftime("%Y-%m-%d") 
pytrends = TrendReq(hl='en-US', tz=360)
a='covid + coronavirus'
cat=''
kw_list = [a]
pytrends.build_payload(kw_list, timeframe=timeframe,geo='US') 
iot=pytrends.interest_over_time()
print("Google Trend Data connection successful!")

url = 'https://www.gstatic.com/covid19/mobility/Global_Mobility_Report.csv'
df = pd.read_csv(url, dtype='unicode')
print("Google Mobility Data connection successful!")

df = covid19("USA", level = 3, start = date(2020,1,1), verbose = False)
print("COVID Datahub Level connection successful!")

headers = {'Content-type': 'application/json'}
data = json.dumps({"seriesid": ['CUSR0000SA0','LNS13000000','LNS14000000'],"startyear":"2018", "endyear":"2020"})
p = requests.post('https://api.bls.gov/publicAPI/v2/timeseries/data/', data=data, headers=headers)
json_data = json.loads(p.text)
print("Labor Stats Data connection successful!")

url = 'https://www.eia.gov/dnav/pet/xls/PET_CONS_WPSUP_K_4.xls'
df = pd.read_excel(url, sheet_name = "Data 1", skiprows=[0,1])
print("Fuel Production Data connection successful!")

session = HTMLSession()
url = 'https://www.tsa.gov/coronavirus/passenger-throughput'
r = session.get(url)
print("TSA Travel Data connection successful!")

url = 'https://www.census.gov/econ/currentdata/export/csv?programCode=RESCONST&timeSlotType=12&startYear=2018&endYear=2020&categoryCode=APERMITS&dataTypeCode=TOTAL&geoLevelCode=US&adjusted=yes&errorData=no&internal=false'
hd = pd.read_csv(url, sep='~',  header=None, nrows=6, keep_default_na=False)
print("CENSUS Data connection successful!")

url = 'http://www.sca.isr.umich.edu/files/tbcics.csv'
df = pd.read_csv(url,skiprows=[0,1,2,3],dtype = {'Unnamed: 1':str})
print("Consumer Sentiment Index connection successful!")


print("URL data access test successful!")

