Copyright (C) 2016-2020 by Teradata Corporation. All rights reserved.
# Covid19

Covid19 Return to Business demo assets

This application consists of the following components:
  * Database Objects Installation - (ddl folder)
  * Database Objects Uninstall - (ddl folder)
  * ETL Python Scripts - (etl folder)
  * Initial Manual Data Inserts (data folder)
  * Tableau Application Visualization (tableau folder)
  
# Prerequisites:
  * Python Version 3.7 or Greater
  * Teradata Connection through Python (teradataml)
  * Test the Teradata-Python connectivity and interfaces through the connection testing file TD_Connection_Test.py
  * Teradata DBS 16.20
  * Tableau Version 2020.1
  * Tableau Reader Version 2020.1  (download at tableau.com)
  * Stored Procedure Creation Granted to the user
  * SELECT WITH GRANT OPTION access to Sys_Calendar.CALENDAR.calendar_date to the user
  
# Configuation Steps:
  * Set Database Credential Information in the file params.py
      * MyHost ='???'
      * MyUser ='???''
      * SchemaName = '???'
      * LogMech = '???' -- TD2, LDAP and etc.

  * Set Default Database in the DDL Installation File ddl
      * Do a complete replacement of '???' to target database name
      
# Installation Steps:
  * Create a seperate database instance (15-20 Gigs of Perm Space) 
  * Initiate a Teradata session through SQL Assistant, Teradata Studio or BTEQ
  * Run CUST_RTN_Install.ddl Install through successful completion
  * Check the logs and database for additional Tables/Views/Stored Procedures
  * Run Python Script CUST_RTN_ETL_STATIC_DATA.py for initial static table load as well as first initial load
  * Check the logs and database for record counts
  * Run Python Script CUST_RTN_ETL_SRC_TO_STG.py for Source to Staging load
  * Check the logs and database for record counts
  * Run Python Script CUST_RTN_ETL_STG_TO_CORE.py for Source to Staging load
  * Check the logs and database for record counts
  * Schedule the Python Scripts to be run Daily or manually run for updates
  
# Tableau Dashboard Refresh:
  * Tableau requires refresh process after initial load for each of the 11 data sources
  * Right mouse on the data source and select Extract -> Refresh
  * You will be prompted to log into the database in order to refresh the data source
  # or
  * Download the dashboard file onto your desktop target Tableau Directory
  * Right click and select "Unpackage"
  * On the created Resiliency Dashboard vx.xx.twb file, right click and open with WordPad to see the XML definition
  * Modify by "Select" and "Replace" the following Connection Information:
	- 'tdprd2.td.teradata.com' to your Host/Sever Name
	- 'auth-ldap' to 'auth-teradata' if other than LDAP
	- 'ADLDEMO_COVID19' to your schema name
	- 'CH186039' to your User Name
  * Save and Open the Dashboard
  * Refresh all data sources and check all tabs

# Audit Process/Validation:
  * Post every run, an email is generated with all the daily load statitics and tables/processes requiring attention
  * Audit Table ETL_Indicator_Proj_Audit captures all the audit records counts and timestamp from Source systems into Teradata staging tables
  * Audit Table ETL_Indicator_Proj_Audit captures all the audit records counts and timestamp from Teradata Staging to Core
  * Error Table ETL_Proc_Error_Logs, captures all stage to core stored procedure errors
  
# Customization:
  * Refer to the WIKI Pages for Customer Specific Extensions
  
# To Uninstall:
  * Set Default Database in the DDL Installation File ddl/CUST_RTN_Uninstall.ddl
      * Do a complete replacement of '???' to target database name
  * Run CUST_RTN_Uninstall.ddl which removes all Tables/Views/Stored Procedure from your database
