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
import sqlalchemy as sqla
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
import params

eng = sqla.create_engine(
    f'teradatasql://{params.MyUser}:{params.Password}@{params.MyHost}/'
    f'?LOGMECH={params.LogMech}&TMODE=ANSI'
)
con = create_context(tdsqlengine=eng)

#############################################################
# Stage to Core Loads
#############################################################
try:
    # Core Load Calls to Teradata
    con.execute(
        "CALL {}.ETL_CUST_DATA_CORE (v_MsgTxt,v_RowCnt,v_ResultSet);"
        .format(params.SchemaName)
    )
    from datetime import datetime
    datetime.utcnow()
    dateTimeObj = pytz.utc.localize(datetime.utcnow()).astimezone(pytz.timezone('US/Pacific'))
    timestampStr = dateTimeObj.strftime("%d-%b-%Y (%H:%M:%S.%f)")
    print("ETL_CUST_DATA_CORE Finished!  " + timestampStr)

    con.execute(
        "CALL {}.ETL_LOOKUP_CORE (v_MsgTxt,v_RowCnt,v_ResultSet);"
        .format(params.SchemaName)
    )
    from datetime import datetime
    datetime.utcnow()
    dateTimeObj = pytz.utc.localize(datetime.utcnow()).astimezone(pytz.timezone('US/Pacific'))
    timestampStr = dateTimeObj.strftime("%d-%b-%Y (%H:%M:%S.%f)")
    print("ETL_LOOKUP_CORE Finished!  " + timestampStr)

    con.execute(
        "CALL {}.ETL_COVID_CASES_CORE (v_MsgTxt,v_RowCnt,v_ResultSet);"
        .format(params.SchemaName)
    )
    from datetime import datetime
    datetime.utcnow()
    dateTimeObj = pytz.utc.localize(datetime.utcnow()).astimezone(pytz.timezone('US/Pacific'))
    timestampStr = dateTimeObj.strftime("%d-%b-%Y (%H:%M:%S.%f)")
    print("ETL_COVID_CASES_CORE Finished!  " + timestampStr)

    con.execute(
        "CALL {}.ETL_LABOR_STATS_CORE (v_MsgTxt,v_RowCnt,v_ResultSet);"
        .format(params.SchemaName)
    )
    from datetime import datetime
    datetime.utcnow()
    dateTimeObj = pytz.utc.localize(datetime.utcnow()).astimezone(pytz.timezone('US/Pacific'))
    timestampStr = dateTimeObj.strftime("%d-%b-%Y (%H:%M:%S.%f)")
    print("ETL_LABOR_STATS_CORE Finished!  " + timestampStr)

    con.execute(
        "CALL {}.ETL_COVID_MODEL_CORE (v_MsgTxt,v_RowCnt,v_ResultSet);"
        .format(params.SchemaName)
    )
    from datetime import datetime
    datetime.utcnow()
    dateTimeObj = pytz.utc.localize(datetime.utcnow()).astimezone(pytz.timezone('US/Pacific'))
    timestampStr = dateTimeObj.strftime("%d-%b-%Y (%H:%M:%S.%f)")
    print("ETL_COVID_MODEL_CORE Finished!  " + timestampStr)

    con.execute(
        "CALL {}.ETL_GOOGLE_MOBILITY_CORE (v_MsgTxt,v_RowCnt,v_ResultSet);"
        .format(params.SchemaName)
    )
    from datetime import datetime
    datetime.utcnow()
    dateTimeObj = pytz.utc.localize(datetime.utcnow()).astimezone(pytz.timezone('US/Pacific'))
    timestampStr = dateTimeObj.strftime("%d-%b-%Y (%H:%M:%S.%f)")
    print("ETL_GOOGLE_MOBILITY_CORE Finished!  " + timestampStr)

    con.execute(
        "CALL {}.ETL_BEA_CORE (v_MsgTxt,v_RowCnt,v_ResultSet);"
        .format(params.SchemaName)
    )
    from datetime import datetime
    datetime.utcnow()
    dateTimeObj = pytz.utc.localize(datetime.utcnow()).astimezone(pytz.timezone('US/Pacific'))
    timestampStr = dateTimeObj.strftime("%d-%b-%Y (%H:%M:%S.%f)")
    print("ETL_BEA_CORE Finished!  " + timestampStr)

    con.execute(
        "CALL {}.ETL_GOOGLE_TREND_CORE (v_MsgTxt,v_RowCnt,v_ResultSet);"
        .format(params.SchemaName)
    )
    from datetime import datetime
    datetime.utcnow()
    dateTimeObj = pytz.utc.localize(datetime.utcnow()).astimezone(pytz.timezone('US/Pacific'))
    timestampStr = dateTimeObj.strftime("%d-%b-%Y (%H:%M:%S.%f)")
    print("ETL_GOOGLE_TREND_CORE Finished!  " + timestampStr)

    con.execute(
        "CALL {}.ETL_COVID19_DATAHUB_CORE (v_MsgTxt,v_RowCnt,v_ResultSet);"
        .format(params.SchemaName)
    )
    from datetime import datetime
    datetime.utcnow()
    dateTimeObj = pytz.utc.localize(datetime.utcnow()).astimezone(pytz.timezone('US/Pacific'))
    timestampStr = dateTimeObj.strftime("%d-%b-%Y (%H:%M:%S.%f)")
    print("ETL_COVID19_DATAHUB_CORE Finished!  " + timestampStr)

    con.execute(
        "CALL {}.ETL_FUEL_PROD_CORE (v_MsgTxt,v_RowCnt,v_ResultSet);"
        .format(params.SchemaName)
    )
    from datetime import datetime
    datetime.utcnow()
    dateTimeObj = pytz.utc.localize(datetime.utcnow()).astimezone(pytz.timezone('US/Pacific'))
    timestampStr = dateTimeObj.strftime("%d-%b-%Y (%H:%M:%S.%f)")
    print("ETL_FUEL_PROD_CORE Finished!  " + timestampStr)

    con.execute(
        "CALL {}.ETL_CONSUMER_SENTIMENT_CORE (v_MsgTxt,v_RowCnt,v_ResultSet);"
        .format(params.SchemaName)
    )
    from datetime import datetime
    datetime.utcnow()
    dateTimeObj = pytz.utc.localize(datetime.utcnow()).astimezone(pytz.timezone('US/Pacific'))
    timestampStr = dateTimeObj.strftime("%d-%b-%Y (%H:%M:%S.%f)")
    print("ETL_CONSUMER_SENTIMENT_CORE Finished!  " + timestampStr)

    con.execute(
        "CALL {}.ETL_TSA_TRAVEL_CORE (v_MsgTxt,v_RowCnt,v_ResultSet);"
        .format(params.SchemaName)
    )
    from datetime import datetime
    datetime.utcnow()
    dateTimeObj = pytz.utc.localize(datetime.utcnow()).astimezone(pytz.timezone('US/Pacific'))
    timestampStr = dateTimeObj.strftime("%d-%b-%Y (%H:%M:%S.%f)")
    print("ETL_TSA_TRAVEL_CORE Finished!  " + timestampStr)

    con.execute(
        "CALL {}.ETL_CENSUS_DATA_CORE (v_MsgTxt,v_RowCnt,v_ResultSet);"
        .format(params.SchemaName)
    )
    from datetime import datetime
    datetime.utcnow()
    dateTimeObj = pytz.utc.localize(datetime.utcnow()).astimezone(pytz.timezone('US/Pacific'))
    timestampStr = dateTimeObj.strftime("%d-%b-%Y (%H:%M:%S.%f)")
    print("ETL_CENSUS_DATA_CORE Finished!  " + timestampStr)

except BaseException as ex:
    print(str(ex))

finally:
    con.execute(
        "CALL {}.ETL_POST_LOAD_CORE (v_MsgTxt,v_RowCnt,v_ResultSet);"
        .format(params.SchemaName)
    )
    from datetime import datetime
    datetime.utcnow()
    dateTimeObj = pytz.utc.localize(datetime.utcnow()).astimezone(pytz.timezone('US/Pacific'))
    timestampStr = dateTimeObj.strftime("%d-%b-%Y (%H:%M:%S.%f)")
    print("ETL_POST_LOAD_CORE Finished!  " + timestampStr)


#############################################################
# Printing the Load Summary Stats
#############################################################

pda = pd.read_sql('DATABASE '+params.SchemaName,con)

query = "select Process_Name, Table_Type, TableName, Records_Processed, Process_Dttm \
from ETL_Indicator_Proj_Audit \
where table_type = 'Core' \
QUALIFY 1=ROW_NUMBER() OVER (PARTITION BY Process_Name ORDER BY Process_Dttm DESC);"


#Fetch the data from Teradata using Pandas Dataframe
pda = pd.read_sql(query,con)
print(pda)
