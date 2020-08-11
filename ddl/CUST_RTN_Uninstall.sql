DATABASE ???;

DROP TABLE ???."DIM_DASH_VIZ_METRIC_XREF";
DROP TABLE ???."DIM_GEO_LOCATION_T";
DROP TABLE ???."DIM_People_location";
DROP TABLE ???."DIM_Site_addresses";
DROP TABLE ???."DIM_ZIPCODE_COUNTY_MSA_LKUP";
DROP TABLE ???."DIM_ZIP_COUNTY_MSA_MAP_RAW";
DROP TABLE ???."ETL_Indicator_Proj_Audit";
DROP TABLE ???."FACT_COVID19_DATAHUB";
DROP TABLE ???."FACT_COVID19_DATAHUB_STATE";
DROP TABLE ???."FACT_Covid_Model_Data";
DROP TABLE ???."FACT_COVID_MODEL_DATA_SUM";
DROP TABLE ???."FACT_INDICATOR_DASHBOARD_T2_P";
DROP TABLE ???."F_IND_DASH_GOOGLE_TRENDS";
DROP TABLE ???."F_IND_DASH_NYT_COVID19_GEO_7MAVG_WEEKLY_SNPSHT";
DROP TABLE ???."STG_BEA_PersonalConsumption_2_3_5";
DROP TABLE ???."STG_BEA_PersonalConsumption_2_4_5";
DROP TABLE ???."STG_Consumer_Sentiment_Index";
DROP TABLE ???."STG_COVID19_Datahub_LVL2";
DROP TABLE ???."STG_COVID19_Datahub_LVL3";
DROP TABLE ???."STG_COVID19_NATIONAL_ESTIMATES";
DROP TABLE ???."STG_covid19_stats";
DROP TABLE ???."STG_CO_EST2019";
DROP TABLE ???."STG_Fuel_Production";
DROP TABLE ???."STG_Google_Mobility";
DROP TABLE ???."STG_Google_Search_IOT";
DROP TABLE ???."STG_Hospitalization_all_locs";
DROP TABLE ???."STG_Labor_Stats_CUSR0000SA0";
DROP TABLE ???."STG_Labor_Stats_LNS13000000";
DROP TABLE ???."STG_Labor_Stats_LNS14000000";
DROP TABLE ???."STG_Summary_stats_all_locs";
DROP TABLE ???."STG_TSA_TRAVEL";
DROP TABLE ???."STG_US_CENSUS_SURVEY";
DROP TABLE ???."TEMP_DIM_DASH_VIZ_METRIC_XREF";          --Not created with install script - created during execution
DROP TABLE ???."TEMP_DIM_GEO_LOCATION_T";                --Not created with install script - created during execution
DROP TABLE ???."TEMP_DIM_People_location";               --Not created with install script - created during execution
DROP TABLE ???."TEMP_DIM_Site_addresses";                --Not created with install script - created during execution
DROP TABLE ???."TEMP_DIM_ZIP_COUNTY_MSA_MAP_RAW";        --Not created with install script - created during execution
DROP TABLE ???."TEMP_FACT_Covid_Model_Data";             --Not created with install script - created during execution
DROP TABLE ???."TEMP_STG_BEA_PersonalConsumption_2_3_5"; --Not created with install script - created during execution
DROP TABLE ???."TEMP_STG_BEA_PersonalConsumption_2_4_5"; --Not created with install script - created during execution
DROP TABLE ???."TEMP_STG_COVID19_NATIONAL_ESTIMATES";    --Not created with install script - created during execution
DROP TABLE ???."TEMP_STG_CO_EST2019";                    --Not created with install script - created during execution
DROP TABLE ???."TEMP_Transaltion_Table";                 --Not created with install script - created during execution
DROP TABLE ???."Transaltion_Table";
DROP VIEW ???."DIM_CALENDAR_V";
DROP VIEW ???."DIM_DASH_VIZ_METRIC_XREF_V";
DROP VIEW ???."DIM_GEO_LOCATION_V";
DROP VIEW ???."DIM_PEOPLE_LOCATION_V";
DROP VIEW ???."DIM_SITE_ADDRESSES_V";
DROP VIEW ???."DIM_ZIPCODE_COUNTY_MSA_LKUP_V";
DROP VIEW ???."FACT_COVID19_DATAHUB_STATE_V";
DROP VIEW ???."FACT_COVID19_DATAHUB_V";
DROP VIEW ???."FACT_COVID_MODEL_DATA_SUM_V";
DROP VIEW ???."FACT_INDICATOR_DASHBOARD_V";
DROP VIEW ???."F_IND_DASH_COVID_NAT_ESTIMATES_V";
DROP VIEW ???."F_IND_DASH_Covid_Projections_Curr_V";
DROP VIEW ???."F_IND_DASH_Covid_Projections_Prev_V";
DROP VIEW ???."F_IND_DASH_DataHub_V";
DROP VIEW ???."F_IND_DASH_GOOGLE_SEARCH_TRENDS_VIZ_V";
DROP VIEW ???."F_IND_DASH_GOOGLE_TRENDS_V";
DROP VIEW ???."F_IND_DASH_HEALTH_VIZ_V";
DROP VIEW ???."F_IND_DASH_MACROECONOMICS_GEO_MONTHLY_V";
DROP VIEW ???."F_IND_DASH_MACROECONOMICS_VIZ_V";
DROP VIEW ???."F_IND_DASH_MOBILITY_GEO_VIZ_V";
DROP VIEW ???."F_IND_DASH_MOBILITY_GEO_WEEKLY_V";
DROP VIEW ???."F_IND_DASH_NYT_COVID19_DATAHUB_TD_EMP_LOC_V";
DROP VIEW ???."F_IND_DASH_NYT_COVID19_GEO_7MAVG_WEEKLY_SNPSHT_V";
DROP VIEW ???."F_IND_DASH_Timeline_to_safety_V";
DROP VIEW ???."XREF_SOURCE_DATA_UPDATES_V";
DROP PROCEDURE ???."ETL_COVID_CASES_CORE";
DROP PROCEDURE ???."ETL_COVID_MODEL_CORE";

--Additional objects that need droppin
DROP TABLE ???."FACT_Covid_Model_Data_UNPIVOT";
DROP TABLE ???."ETL_Proc_Error_Logs";
DROP TABLE ???."F_IND_DASH_NYT_COVID19_COUNTY_7MAVG";
DROP TABLE ???."gm_step1";
DROP TABLE ???."gm_step2";
DROP VIEW  ???."F_IND_DASH_Covid_Summary_V";
DROP VIEW  ???."F_IND_DASHBOARD_MOBILITY_GEO_WEEKLY_V";

