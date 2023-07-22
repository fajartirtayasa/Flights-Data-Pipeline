CREATE DATABASE if not exists flights_staging;

USE flights_staging;

CREATE EXTERNAL TABLE IF NOT EXISTS fact_flight_dep(
        ICAO24 STRING,
        firstSeen BIGINT,
        estDepartureAirport STRING,
        lastSeen BIGINT,
        estArrivalAirport STRING,
        callsign STRING,
        estDepartureAirportHorizDistance BIGINT,
        estDepartureAirportVertDistance BIGINT,
        estArrivalAirportHorizDistance DOUBLE,
        estArrivalAirportVertDistance DOUBLE,
        departureAirportCandidatesCount BIGINT,
        arrivalAirportCandidatesCount BIGINT
    )
STORED AS PARQUET
LOCATION 'hdfs://localhost:9000/user/fatir/flights-data-staging/flight_table_dep';

CREATE EXTERNAL TABLE IF NOT EXISTS fact_flight_arr(
        ICAO24 STRING,
        firstSeen BIGINT,
        estDepartureAirport STRING,
        lastSeen BIGINT,
        estArrivalAirport STRING,
        callsign STRING,
        estDepartureAirportHorizDistance DOUBLE,
        estDepartureAirportVertDistance DOUBLE,
        estArrivalAirportHorizDistance BIGINT,
        estArrivalAirportVertDistance BIGINT,
        departureAirportCandidatesCount BIGINT,
        arrivalAirportCandidatesCount BIGINT
    )
STORED AS PARQUET
LOCATION 'hdfs://localhost:9000/user/fatir/flights-data-staging/flight_table_arr';
