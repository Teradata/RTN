#!/usr/bin/env python
# coding: utf-8

# In[ ]


#!/usr/bin/env python
# coding: utf-8


#############################################################
# Credentials
#############################################################
MyHost ='???'
MyUser ='???'
TempDB ='???'
SchemaName = "???"
password = '???'
sender_email = '???'
# Needs to use company SMPT Server Info
receivers = ['???','???',...]

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

from IPython import get_ipython
from covid19dh import covid19
import pytz
import tweepy as tw
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
con = create_context(host=MyHost, username=MyUser, password=password, temp_database_name=TempDB, logmech='LDAP')


#############################################################
# Email Status
#############################################################

import smtplib
import tabulate
from tabulate import tabulate
import base64

from datetime import datetime
datetime.utcnow()
dateTimeObj = pytz.utc.localize(datetime.utcnow()).astimezone(pytz.timezone('US/Pacific'))
timestampStr = dateTimeObj.strftime("%d-%b-%Y (%H:%M:%S.%f)")


message = """From: RTN Dashboard Automated Process
To: Subscribed Recepients
Subject:  Daily RTN Dashboard Load Statistics

"""

WarningQuery = "SELECT 'Procedure '||Process_Name||' was last updated on '|| MAX(Process_Dttm(DATE)) \
FROM ETL_Indicator_Proj_Audit \
WHERE Process_Name <> 'Python' \
HAVING MAX(Process_Dttm(DATE)) < DATE \
GROUP BY Process_Name \
UNION \
SELECT 'Table '||TableName||' was last updated on '||MAX(Process_Dttm(DATE)) \
FROM ETL_Indicator_Proj_Audit \
HAVING MAX(Process_Dttm(DATE)) < DATE \
GROUP BY TableName;"

Warning = pd.read_sql('DATABASE '+SchemaName,con)
Warning = pd.read_sql(WarningQuery,con)

server = smtplib.SMTP('smtp.gmail.com', 587)
server.starttls()
server.login(sender_email, EmailPWD)
server.sendmail(sender_email, receivers, message + 'Daily Load Job Finished at '+timestampStr+ "\r\n" + "\r\n" + "\r\n" +"WARNING:"+"\r\n"+tabulate(Warning, showindex=False,tablefmt="psql"))

print("Confirmation Email Sent!  " + timestampStr)