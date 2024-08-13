# Data Transformation
# The transformation process includes:

1. Normalizing the JSON data.
2. Adding IP address-based local time zones.
3. Calculating timestamps in the local time zones of gamers.
4. Creating views to facilitate data analysis.

-- Create a view to normalize the JSON data
create or replace view LOGS as
select 
    RAW_LOG:agent::text as AGENT,
    RAW_LOG:datetime_iso8601::TIMESTAMP_NTZ as DATETIME,
    RAW_LOG:user_event::text as USER_EVENT,
    RAW_LOG:user_login::text as USER_LOGIN,
    RAW_LOG:ip_address::text as IP_ADDRESS,
    RAW_LOG as RAW_LOG
from game_logs;

-- Adjust session timezone
alter session set timezone = 'Asia/Shanghai';

-- Enhance the data with geographical information
create or replace view LOGS as
select 
    RAW_LOG:datetime_iso8601::TIMESTAMP_NTZ as DATETIME,
    RAW_LOG:user_event::text as USER_EVENT,
    RAW_LOG:user_login::text as USER_LOGIN,
    RAW_LOG:ip_address::text as IP_ADDRESS,
    RAW_LOG as RAW_LOG
from game_logs
where IP_ADDRESS is not null;

-- Calculate local time zone and day of the week
SELECT logs.ip_address,
       logs.user_login as GAMER_NAME,
       logs.user_event as GAME_EVENT_NAME,
       logs.datetime as GAME_EVENT_UTC,
       city,
       region,
       country,
       timezone as GAMER_LTZ_NAME,
       CONVERT_TIMEZONE('UTC',timezone,logs.datetime) AS game_event_ltz,
       dayname(game_event_ltz) as DOW_NAME,
       hour(game_event_ltz) as hour_of_day,
       TOD_NAME
FROM AGS_GAME_AUDIENCE.RAW.LOGS logs
JOIN IPINFO_GEOLOC.demo.location loc 
ON IPINFO_GEOLOC.public.TO_JOIN_KEY(logs.ip_address) = loc.join_key
AND IPINFO_GEOLOC.public.TO_INT(logs.ip_address) 
BETWEEN start_ip_int AND end_ip_int
JOIN AGS_GAME_AUDIENCE.RAW.TIME_OF_DAY_LU tod
ON hour(game_event_ltz)=tod.hour;

-- Create an enhanced table with all transformations applied
create table ags_game_audience.enhanced.logs_enhanced as (
select logs.ip_address,
       logs.user_login as GAMER_NAME,
       logs.user_event as GAME_EVENT_NAME,
       logs.datetime as GAME_EVENT_UTC,
       city,
       region,
       country,
       timezone as GAMER_LTZ_NAME,
       CONVERT_TIMEZONE('UTC',timezone,logs.datetime) AS game_event_ltz,
       dayname(game_event_ltz) as DOW_NAME,
       hour(game_event_ltz) as hour_of_day,
       TOD_NAME
from AGS_GAME_AUDIENCE.RAW.LOGS logs
JOIN IPINFO_GEOLOC.demo.location loc 
ON IPINFO_GEOLOC.public.TO_JOIN_KEY(logs.ip_address) = loc.join_key
AND IPINFO_GEOLOC.public.TO_INT(logs.ip_address) 
BETWEEN start_ip_int AND end_ip_int
JOIN AGS_GAME_AUDIENCE.RAW.TIME_OF_DAY_LU tod
ON hour(game_event_ltz)=tod.hour
);


Key SQL Queries
Extract Data: COPY INTO command for loading data from Snowflake stages.
Transform Data: SQL views and joins to normalize data, add time zones, and calculate local timestamps.
Load Data: Creating enhanced tables for final analysis.
Usage
Set up Snowflake Environment: Ensure you have access to Snowflake and the required stages and tables.
Run SQL Scripts: Execute the SQL commands in Snowflake to perform the ETL operations.
Analyze Results: Use the resulting tables and views for analysis and recommendations.
Future Enhancements
Automate Data Pipeline: Implement a scheduled ETL pipeline for continuous data processing.
Advanced Analysis: Incorporate machine learning models to predict gaming trends based on the data.
