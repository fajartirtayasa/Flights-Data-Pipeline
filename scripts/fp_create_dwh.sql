CREATE DATABASE if not exists flights_dwh;

USE flights_dwh;

CREATE EXTERNAL TABLE IF NOT EXISTS fact_flight(
        ICAO24 STRING,
        sk_departure_date BIGINT,
        sk_departure_time BIGINT,
        departure_airport STRING,
        sk_arrival_date BIGINT,
        sk_arrival_time BIGINT,
        arrival_airport STRING,
        callsign STRING,
        flight_duration BIGINT
    )
STORED AS PARQUET
LOCATION 'hdfs://localhost:9000/user/fatir/flights-dwh/fact_flight';