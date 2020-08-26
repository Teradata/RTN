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
import csv
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
from teradatasqlalchemy.types import *
import ssl
ssl._create_default_https_context = ssl._create_unverified_context
import params
con = create_context(host=params.MyHost, username=params.MyUser, password=params.Password,temp_database_name=params.SchemaName,logmech=params.LogMech)

#############################################################
# Manual Loads
#############################################################

pda = pd.read_sql('DATABASE '+params.SchemaName,con)

# DIM_GEO_LOCATION_T
url = 'https://raw.githubusercontent.com/golestm/RTN/master/data/DIM_GEO_LOCATION_T.txt'
df = pd.read_csv(url,sep="|", doublequote=True,  encoding='latin-1')
copy_to_sql(df = df, table_name = "TEMP_DIM_GEO_LOCATION_T", schema_name=params.SchemaName, if_exists = 'replace')
pd.read_sql('DATABASE '+params.SchemaName,con)
pd.read_sql('DELETE FROM DIM_GEO_LOCATION_T;',con)
pd.read_sql('INSERT INTO DIM_GEO_LOCATION_T SELECT * FROM TEMP_DIM_GEO_LOCATION_T;',con)
print("DIM_GEO_LOCATION_T Finished!")
            
# DIM_ZIP_COUNTY_MSA_MAP_RAW
url = 'https://raw.githubusercontent.com/golestm/RTN/master/data/DIM_ZIP_COUNTY_MSA_MAP_RAW.txt'
df = pd.read_csv(url,sep="|", doublequote=True,  encoding='latin-1', dtype = {'ZIPCODE':str,'CBSA_CODE':str})
copy_to_sql(df = df, table_name = "TEMP_DIM_ZIP_COUNTY_MSA_MAP_RAW", schema_name=params.SchemaName, if_exists = 'replace')
pd.read_sql('DELETE FROM DIM_ZIP_COUNTY_MSA_MAP_RAW;',con)
pd.read_sql('INSERT INTO DIM_ZIP_COUNTY_MSA_MAP_RAW SELECT CAST(ZIPCODE as VARCHAR(10)), CAST(COUNTY_FIPS as VARCHAR(10)), COUNTY_RES_RATIO, COUNTY_BUS_RATIO, COUNTY_OTH_RATIO, COUNTY_TOT_RATIO, TOP_ZIP_FLAG, SPLIT_COUNTY_CNT, TOP_COUNTY_FLAG, COUNTY_NAME, CBSA_CODE, CBSA_NAME, MSA_NAME, STATE_NAME FROM TEMP_DIM_ZIP_COUNTY_MSA_MAP_RAW;',con)
print("DIM_ZIP_COUNTY_MSA_MAP_RAW Finished!")

# DIM_People_location
url = 'https://raw.githubusercontent.com/golestm/RTN/master/data/DIM_People_location.txt'
df = pd.read_csv(url,sep="|", doublequote=True,  encoding='latin-1', dtype = {'Zipcode':str})
copy_to_sql(df = df, table_name = "TEMP_DIM_People_location", schema_name=params.SchemaName, if_exists = 'replace')
pd.read_sql('DELETE FROM DIM_People_location;',con)
pd.read_sql('INSERT INTO DIM_People_location SELECT CLient_nbr, Client_name, Site_Id, Site_Type, Address, City, State, CAST(Zipcode as VARCHAR(10)), Region, Country_cd, Nbr_at_location, People_type FROM TEMP_DIM_People_location;',con)
print("DIM_People_location Finished!")

# DIM_Site_addresses
url = 'https://raw.githubusercontent.com/golestm/RTN/master/data/DIM_Site_addresses.txt'
df = pd.read_csv(url,sep="|", doublequote=True,  encoding='latin-1', dtype = {'Zipcode':str})
copy_to_sql(df = df, table_name = "TEMP_DIM_Site_addresses", schema_name=params.SchemaName, if_exists = 'replace')
pd.read_sql('DELETE FROM DIM_Site_addresses;',con)
pd.read_sql('INSERT INTO DIM_Site_addresses SELECT * FROM TEMP_DIM_Site_addresses;',con)
print("DIM_Site_addresses Finished!")


# STG_CO_EST2019
url = 'https://raw.githubusercontent.com/golestm/RTN/master/data/STG_CO_EST2019.txt'
df = pd.read_csv(url,sep="|", doublequote=True,  encoding='latin-1')
copy_to_sql(df = df, table_name = "TEMP_STG_CO_EST2019", schema_name=params.SchemaName, if_exists = 'replace')
pd.read_sql('DELETE FROM STG_CO_EST2019;',con)
pd.read_sql('INSERT INTO STG_CO_EST2019 SELECT * FROM TEMP_STG_CO_EST2019;',con)
print("STG_CO_EST2019 Finished!")

# Transaltion_Table
url = 'https://raw.githubusercontent.com/golestm/RTN/master/data/Transaltion_Table.txt'
df = pd.read_csv(url,sep="|", doublequote=True,  encoding='latin-1')
copy_to_sql(df = df, table_name = "TEMP_Transaltion_Table", schema_name=params.SchemaName, if_exists = 'replace')
pd.read_sql('DELETE FROM Transaltion_Table;',con)
pd.read_sql('INSERT INTO Transaltion_Table SELECT * FROM TEMP_Transaltion_Table;',con)
print("Transaltion_Table Finished!")

# STG_BEA_PersonalConsumption_2_3_5
url = 'https://raw.githubusercontent.com/golestm/RTN/master/data/STG_BEA_PersonalConsumption_2_3_5.txt'
df = pd.read_csv(url,sep="|", doublequote=True,  encoding='latin-1')
copy_to_sql(df = df, table_name = "TEMP_STG_BEA_PersonalConsumption_2_3_5", schema_name=params.SchemaName, if_exists = 'replace')
pd.read_sql('DELETE FROM STG_BEA_PersonalConsumption_2_3_5;',con)
pd.read_sql('INSERT INTO STG_BEA_PersonalConsumption_2_3_5 SELECT * FROM TEMP_STG_BEA_PersonalConsumption_2_3_5;',con)
print("STG_BEA_PersonalConsumption_2_3_5 Finished!")

# STG_BEA_PersonalConsumption_2_4_5
url = 'https://raw.githubusercontent.com/golestm/RTN/master/data/STG_BEA_PersonalConsumption_2_4_5.txt'
df = pd.read_csv(url,sep="|", doublequote=True,  encoding='latin-1')
copy_to_sql(df = df, table_name = "TEMP_STG_BEA_PersonalConsumption_2_4_5", schema_name=params.SchemaName, if_exists = 'replace')
pd.read_sql('DELETE FROM STG_BEA_PersonalConsumption_2_4_5;',con)
pd.read_sql('INSERT INTO STG_BEA_PersonalConsumption_2_4_5 SELECT * FROM TEMP_STG_BEA_PersonalConsumption_2_4_5;',con)
print("STG_BEA_PersonalConsumption_2_4_5 Finished!")

# STG_COVID19_NATIONAL_ESTIMATES
url = 'https://raw.githubusercontent.com/golestm/RTN/master/data/STG_COVID19_NATIONAL_ESTIMATES.txt'
df = pd.read_csv(url,sep="|", doublequote=True,  encoding='latin-1')
copy_to_sql(df = df, table_name = "TEMP_STG_COVID19_NATIONAL_ESTIMATES", schema_name=params.SchemaName, if_exists = 'replace')
pd.read_sql('DELETE FROM STG_COVID19_NATIONAL_ESTIMATES;',con)
pd.read_sql('INSERT INTO STG_COVID19_NATIONAL_ESTIMATES SELECT * FROM TEMP_STG_COVID19_NATIONAL_ESTIMATES;',con)
print("STG_COVID19_NATIONAL_ESTIMATES Finished!")

# FACT_Covid_Model_Data (Will load if there is no records for the first time)
url = 'https://raw.githubusercontent.com/golestm/RTN/master/data/FACT_Covid_Model_Data.txt'
df = pd.read_csv(url,sep="|", doublequote=True,  encoding='latin-1')
copy_to_sql(df = df, table_name = "TEMP_FACT_Covid_Model_Data", schema_name=params.SchemaName, if_exists = 'replace')
pd.read_sql('INSERT INTO FACT_Covid_Model_Data SELECT * FROM TEMP_FACT_Covid_Model_Data WHERE NOT EXISTS (SELECT 1 FROM FACT_Covid_Model_Data);',con)
print("FACT_Covid_Model_Data Finished!")

# DIM_DASH_VIZ_METRIC_XREF(Will load if there is no records for the first time)
url = 'https://raw.githubusercontent.com/golestm/RTN/master/data/DIM_DASH_VIZ_METRIC_XREF.txt'
df = pd.read_csv(url,sep="|", doublequote=True,  encoding='latin-1', dtype = {'DASHBOARD_VERSION':str})
copy_to_sql(df = df, table_name = "TEMP_DIM_DASH_VIZ_METRIC_XREF", schema_name=params.SchemaName, if_exists = 'replace')
pd.read_sql('INSERT INTO DIM_DASH_VIZ_METRIC_XREF SELECT * FROM TEMP_DIM_DASH_VIZ_METRIC_XREF WHERE NOT EXISTS (SELECT 1 FROM DIM_DASH_VIZ_METRIC_XREF);',con)
print("DIM_DASH_VIZ_METRIC_XREF!")


from datetime import datetime
datetime.utcnow()
dateTimeObj = pytz.utc.localize(datetime.utcnow()).astimezone(pytz.timezone('US/Pacific'))
timestampStr = dateTimeObj.strftime("%d-%b-%Y (%H:%M:%S.%f)")
print("Initial Static Tables Reloaded!  " + timestampStr)

#############################################################
# Printing the Load Summary Stats
#############################################################

pda = pd.read_sql('DATABASE '+params.SchemaName,con)

query = "SELECT 'Python' as Process_Name, 'Static' as Table_Type, 'DIM_GEO_LOCATION_T' as TableName, count(*) as Records_Processed, max(current_timestamp(0)) as Process_Dttm FROM DIM_GEO_LOCATION_T GROUP BY 1,2,3 \
UNION \
SELECT 'Python' as Process_Name, 'Static' as Table_Type, 'DIM_ZIP_COUNTY_MSA_MAP_RAW' as TableName, count(*) as Records_Processed, max(current_timestamp(0)) as Process_Dttm FROM DIM_ZIP_COUNTY_MSA_MAP_RAW GROUP BY 1,2,3 \
UNION \
SELECT 'Python' as Process_Name, 'Static' as Table_Type, 'DIM_People_location' as TableName, count(*) as Records_Processed, max(current_timestamp(0)) as Process_Dttm FROM DIM_People_location GROUP BY 1,2,3 \
UNION \
SELECT 'Python' as Process_Name, 'Static' as Table_Type, 'DIM_Site_addresses' as TableName, count(*) as Records_Processed, max(current_timestamp(0)) as Process_Dttm FROM DIM_Site_addresses GROUP BY 1,2,3 \
UNION \
SELECT 'Python' as Process_Name, 'Static' as Table_Type, 'STG_CO_EST2019' as TableName, count(*) as Records_Processed, max(current_timestamp(0)) as Process_Dttm FROM STG_CO_EST2019 GROUP BY 1,2,3 \
UNION \
SELECT 'Python' as Process_Name, 'Static' as Table_Type, 'Transaltion_Table' as TableName, count(*) as Records_Processed, max(current_timestamp(0)) as Process_Dttm FROM Transaltion_Table GROUP BY 1,2,3 \
UNION \
SELECT 'Python' as Process_Name, 'Static' as Table_Type, 'STG_BEA_PersonalConsumption_2_3_5' as TableName, count(*) as Records_Processed, max(current_timestamp(0)) as Process_Dttm FROM STG_BEA_PersonalConsumption_2_3_5 GROUP BY 1,2,3 \
UNION \
SELECT 'Python' as Process_Name, 'Static' as Table_Type, 'STG_BEA_PersonalConsumption_2_4_5' as TableName, count(*) as Records_Processed, max(current_timestamp(0)) as Process_Dttm FROM STG_BEA_PersonalConsumption_2_4_5 GROUP BY 1,2,3 \
UNION \
SELECT 'Python' as Process_Name, 'Static' as Table_Type, 'STG_COVID19_NATIONAL_ESTIMATES' as TableName, count(*) as Records_Processed, max(current_timestamp(0)) as Process_Dttm FROM STG_COVID19_NATIONAL_ESTIMATES GROUP BY 1,2,3;"


#Fetch the data from Teradata using Pandas Dataframe
pda = pd.read_sql(query,con)
copy_to_sql(df = pda, table_name = "ETL_Indicator_Proj_Audit", schema_name=params.SchemaName, if_exists = 'append')
print(pda)

