-- create logging directory for hive

sudo chown hadoop -R /var/log/hive
mkdir /var/log/hive/user/hadoop

-- discover direcories in s3 root location
aws s3 ls /

-- connect to interative hive client
hive -d INPUT=s3://airline-analysis-hive/input -d OUTPUT=s3://airline-analysis-hive//output/

-- ========================================================
-- Loading and Processing Data
-- ========================================================
-- create external table from CSV data stored in S3 bucket

CREATE EXTERNAL TABLE delay_flights (
  `index` int,
  `Year` int,
  `Month` int,
  `DayofMonth` int,
  `DayOfWeek` int,
  `DepTime` int,
  `CRSDepTime` int,
  `ArrTime` int,
  `CRSArrTime` int,
  `UniqueCarrier` string,
  `FlightNum` int,
  `TailNum` string,
  `ActualElapsedTime` int,
  `CRSElapsedTime` int,
  `AirTime` int,
  `ArrDelay` int,
  `DepDelay` int,
  `Origin` string,
  `Dest` string,
  `Distance` int,
  `TaxiIn` int,
  `TaxiOut` int,
  `Cancelled` int,
  `CancellationCode` string,
  `Diverted` int,
  `CarrierDelay` int,
  `WeatherDelay` int,
  `NASDelay` int,
  `SecurityDelay` int,
  `LateAircraftDelay` int
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '${INPUT}/'
TBLPROPERTIES ("skip.header.line.count"="1");

-- verify loaded
show tables;
set hive.cli.print.header=true;

select * from delay_flights limit 5;

-- ========================================================
-- Applying Queries
-- ========================================================

set hive.cli.print.header=true;
-- Year wise carrier delay from 2003-2010
SELECT `Year`, avg((`CarrierDelay` / `ArrDelay`)*100) AS `AvgCarrierDelay` FROM delay_flights GROUP BY `Year` ORDER BY `Year`;

-- Year wise NAS delay from 2003-2010
SELECT `Year`, avg((`NASDelay` / `ArrDelay`)*100) AS `AvgNASDelay` FROM delay_flights GROUP BY `Year` ORDER BY `Year`;

-- Year wise Weather delay from 2003-2010
SELECT `Year`, avg((`WeatherDelay` / `ArrDelay`)*100) AS `AvgWeatherDelay` FROM delay_flights GROUP BY `Year` ORDER BY `Year`;

-- Year wise late aircraft delay from 2003-2010
SELECT `Year`, avg((`LateAircraftDelay` / `ArrDelay`)*100) AS `AvgLateAircraftDelay` FROM delay_flights GROUP BY `Year` ORDER BY `Year`;

-- Year wise security delay from 2003-2010
SELECT `Year`, avg((`SecurityDelay` / `ArrDelay`)*100) AS `AvgSecurityDelay` FROM delay_flights GROUP BY `Year` ORDER BY `Year`;

