create database covid_db;
use covid_db;

CREATE TABLE IF NOT EXISTS covid_db.covid_staging 
(
 Country 			        STRING,
 Total_Cases   		                DOUBLE,
 New_Cases    		                DOUBLE,
 Total_Deaths                       	DOUBLE,
 New_Deaths                         	DOUBLE,
 Total_Recovered                    	DOUBLE,
 Active_Cases                       	DOUBLE,
 Serious		               	DOUBLE,
 Tot_Cases                   		DOUBLE,
 Deaths                      		DOUBLE,
 Total_Tests                   		DOUBLE,
 Tests			                DOUBLE,
 CASES_per_Test                     	DOUBLE,
 Death_in_Closed_Cases     	        STRING,
 Rank_by_Testing_rate 		        DOUBLE,
 Rank_by_Death_rate    		        DOUBLE,
 Rank_by_Cases_rate    		        DOUBLE,
 Rank_by_Death_of_Closed_Cases   	DOUBLE
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
STORED as TEXTFILE
LOCATION '/user/cloudera/ds/COVID_HDFS_LZ'
tblproperties ("skip.header.line.count"="1");

CREATE EXTERNAL TABLE IF NOT EXISTS covid_db.covid_ds_partitioned 
(
 Country 			        STRING,
 Total_Cases   		                DOUBLE,
 New_Cases    		                DOUBLE,
 Total_Deaths                      	DOUBLE,
 New_Deaths                        	DOUBLE,
 Total_Recovered                   	DOUBLE,
 Active_Cases                       	DOUBLE,
 Serious		                DOUBLE,
 Tot_Cases                   		DOUBLE,
 Deaths                      		DOUBLE,
 Total_Tests                   		DOUBLE,
 Tests			                DOUBLE,
 CASES_per_Test                     	DOUBLE,
 Death_in_Closed_Cases     	        STRING,
 Rank_by_Testing_rate 		        DOUBLE,
 Rank_by_Death_rate    		        DOUBLE,
 Rank_by_Cases_rate    		        DOUBLE,
 Rank_by_Death_of_Closed_Cases   	DOUBLE
)
PARTITIONED BY (COUNTRY_NAME STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
STORED as TEXTFILE
LOCATION '/user/cloudera/ds/COVID_HDFS_PARTITIONED';

SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

SET hive.exec.max.dynamic.partitions=100000;
SET hive.exec.max.dynamic.partitions.pernode=100000;

FROM
covid_db.covid_staging
INSERT INTO TABLE covid_db.covid_ds_partitioned PARTITION(COUNTRY_NAME)
SELECT *,Country WHERE Country is not null and Country <> "" and Country <> "World";

CREATE EXTERNAL TABLE IF NOT EXISTS covid_db.covid_final_output
(
 DEATHS 	DOUBLE,
 TESTS 		DOUBLE,
 TOP_Death 	STRING,
 TOP_TEST 	STRING
)
PARTITIONED BY (COUNTRY_NAME STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
STORED as TEXTFILE
LOCATION '/user/cloudera/ds/COVID_FINAL_OUTPUT';

FROM
covid_db.covid_ds_partitioned
INSERT INTO TABLE covid_db.covid_final_output PARTITION(COUNTRY_NAME)
SELECT Country, CASE WHEN length(Deaths) > 0 THEN Deaths ELSE 0 END,
CASE WHEN length(Tests) > 0 THEN Tests ELSE 0 END,
rank() over(order by cast(regexp_replace(Deaths, ",", "") as double) desc), 
rank() over(order by cast (regexp_replace( Tests, ",", "") as double) desc) 
WHERE Country is not null and Country <> "" and Country <> "World";

INSERT OVERWRITE LOCAL DIRECTORY '/home/cloudera/covid_project/output/final_output' ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' select * from covid_db.covid_final_output;
