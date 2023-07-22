CREATE DATABASE IF NOT EXISTS flights_staging;
USE flights_staging;



CREATE EXTERNAL TABLE IF NOT EXISTS aircraft_all(
icao24 STRING, registration STRING, manufacturericao STRING, manufacturername STRING, model STRING,
typecode STRING, serialnumber STRING, linenumber STRING, icaoaircrafttype STRING, operator STRING,
operatorcallsign STRING, operatoricao STRING, operatoriata STRING, owner STRING, testreg STRING,
registered STRING, reguntil STRING, status STRING, built STRING, firstflightdate STRING,
seatconfiguration STRING, engines STRING, modes BOOLEAN, adsb BOOLEAN, acars BOOLEAN,
notes STRING, categoryDescription STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/fatir/flights-data-staging/Aircraft'
tblproperties ("skip.header.line.count"="1");

LOAD DATA LOCAL INPATH '/home/fatir/datalake-flight/aircraftDatabase.csv'
OVERWRITE INTO TABLE aircraft_all;



CREATE EXTERNAL TABLE IF NOT EXISTS aircraft_types(
AircraftDescription STRING, Description STRING, Designator STRING, EngineCount STRING,
EngineType STRING, ManufacturerCode STRING, ModelFullName STRING, WTC STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/fatir/flights-data-staging/Aircraft-Types'
tblproperties ("skip.header.line.count"="1");

LOAD DATA LOCAL INPATH '/home/fatir/datalake-flight/doc8643AircraftTypes.csv'
OVERWRITE INTO TABLE aircraft_types;



CREATE EXTERNAL TABLE IF NOT EXISTS airports(
name STRING, iata STRING, icao STRING, latitude DOUBLE, longitude DOUBLE,
country STRING, altitude DOUBLE, type STRING, municipality STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/fatir/flights-data-staging/Airport'
tblproperties ("skip.header.line.count"="1");

LOAD DATA LOCAL INPATH '/home/fatir/datalake-flight/Airport.csv'
OVERWRITE INTO TABLE airports;