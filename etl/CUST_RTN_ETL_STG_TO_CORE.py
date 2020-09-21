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

import pandas as pd
import sqlalchemy as sqla
from teradataml.context.context import create_context

from common import print_complete
import params

def stgToCore():
    eng = sqla.create_engine(
        f'teradatasql://{params.MyUser}:{params.Password}@{params.MyHost}/'
        f'?LOGMECH={params.LogMech}&TMODE=ANSI'
    )
    con = create_context(tdsqlengine=eng)

    # %%
    #############################################################
    # Stage to Core Loads
    #############################################################
    try:
        # Core Load Calls to Teradata
        con.execute(
            "CALL {}.ETL_CUST_DATA_CORE (v_MsgTxt,v_RowCnt,v_ResultSet);"
            .format(params.SchemaName)
        )
        print_complete("ETL_CUST_DATA_CORE")

        con.execute(
            "CALL {}.ETL_LOOKUP_CORE (v_MsgTxt,v_RowCnt,v_ResultSet);"
            .format(params.SchemaName)
        )
        print_complete("ETL_LOOKUP_CORE")

        con.execute(
            "CALL {}.ETL_COVID_CASES_CORE (v_MsgTxt,v_RowCnt,v_ResultSet);"
            .format(params.SchemaName)
        )
        print_complete("ETL_COVID_CASES_CORE")

        con.execute(
            "CALL {}.ETL_LABOR_STATS_CORE (v_MsgTxt,v_RowCnt,v_ResultSet);"
            .format(params.SchemaName)
        )
        print_complete("ETL_LABOR_STATS_CORE")

        con.execute(
            "CALL {}.ETL_COVID_MODEL_CORE (v_MsgTxt,v_RowCnt,v_ResultSet);"
            .format(params.SchemaName)
        )
        print_complete("ETL_COVID_MODEL_CORE")

        con.execute(
            "CALL {}.ETL_GOOGLE_MOBILITY_CORE (v_MsgTxt,v_RowCnt,v_ResultSet);"
            .format(params.SchemaName)
        )
        print_complete("ETL_GOOGLE_MOBILITY_CORE")

        con.execute(
            "CALL {}.ETL_BEA_CORE (v_MsgTxt,v_RowCnt,v_ResultSet);"
            .format(params.SchemaName)
        )
        print_complete("ETL_BEA_CORE")

        con.execute(
            "CALL {}.ETL_GOOGLE_TREND_CORE (v_MsgTxt,v_RowCnt,v_ResultSet);"
            .format(params.SchemaName)
        )
        print_complete("ETL_GOOGLE_TREND_CORE")

        con.execute(
            "CALL {}.ETL_COVID19_DATAHUB_CORE (v_MsgTxt,v_RowCnt,v_ResultSet);"
            .format(params.SchemaName)
        )
        print_complete("ETL_COVID19_DATAHUB_CORE")

        con.execute(
            "CALL {}.ETL_FUEL_PROD_CORE (v_MsgTxt,v_RowCnt,v_ResultSet);"
            .format(params.SchemaName)
        )
        print_complete("ETL_FUEL_PROD_CORE")

        con.execute(
            "CALL {}.ETL_CONSUMER_SENTIMENT_CORE "
            "(v_MsgTxt,v_RowCnt,v_ResultSet);"
            .format(params.SchemaName)
        )
        print_complete("ETL_CONSUMER_SENTIMENT_CORE")

        con.execute(
            "CALL {}.ETL_TSA_TRAVEL_CORE (v_MsgTxt,v_RowCnt,v_ResultSet);"
            .format(params.SchemaName)
        )
        print_complete("ETL_TSA_TRAVEL_CORE")

        con.execute(
            "CALL {}.ETL_CENSUS_DATA_CORE (v_MsgTxt,v_RowCnt,v_ResultSet);"
            .format(params.SchemaName)
        )
        print_complete("ETL_CENSUS_DATA_CORE")

    except BaseException as ex:
        print(str(ex))

    finally:
        con.execute(
            "CALL {}.ETL_POST_LOAD_CORE (v_MsgTxt,v_RowCnt,v_ResultSet);"
            .format(params.SchemaName)
        )
        print_complete("ETL_POST_LOAD_CORE")

    # %%
    #############################################################
    # Printing the Load Summary Stats
    #############################################################

    pda = pd.read_sql('DATABASE '+params.SchemaName,con)

    query = (
        "select\n"
        "  Process_Name,\n"
        "  Table_Type,\n"
        "  TableName,\n"
        "  Records_Processed,\n"
        "  Process_Dttm\n"
        "from ETL_Indicator_Proj_Audit\n"
        "where table_type = 'Core'\n"
        "QUALIFY 1 = ROW_NUMBER() OVER (\n"
        "    PARTITION BY Process_Name\n"
        "    ORDER BY Process_Dttm DESC\n"
        ");"
    )

    # %%
    #Fetch the data from Teradata using Pandas Dataframe
    pda = pd.read_sql(query,con)
    print(pda)


if __name__ == "__main__":
    stgToCore()
