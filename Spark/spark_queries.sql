-- connect to spark-shell
spark-shell

-- ========================================================
-- Loading and Processing Data
-- ========================================================
-- load the CSV data stored in S3 bucket to a spark dataframe and create a temporary view
val df = spark.read.format("csv").option("header", "true").load("s3://airline-analysis-spark/input/DelayedFlights-updated.csv")
df.createOrReplaceTempView("delay_flights")

-- verufy loaded data
df.show()


-- ========================================================
-- Applying Queries
-- ========================================================

-- Year wise carrier delay from 2003-2010
spark.time(spark.sql("SELECT `Year`, avg((`CarrierDelay` / `ArrDelay`)*100) AS `AvgCarrierDelay` FROM delay_flights GROUP BY `Year` ORDER BY `Year`").show());

-- Year wise NAS delay from 2003-2010
spark.time(spark.sql("SELECT `Year`, avg((`NASDelay` / `ArrDelay`)*100) AS `AvgNASDelay` FROM delay_flights GROUP BY `Year` ORDER BY `Year`").show());

-- Year wise Weather delay from 2003-2010
spark.time(spark.sql("SELECT `Year`, avg((`WeatherDelay` / `ArrDelay`)*100) AS `AvgWeatherDelay` FROM delay_flights GROUP BY `Year` ORDER BY `Year`").show());

-- Year wise late aircraft delay from 2003-2010
spark.time(spark.sql("SELECT `Year`, avg((`LateAircraftDelay` / `ArrDelay`)*100) AS `AvgLateAircraftDelay` FROM delay_flights GROUP BY `Year` ORDER BY `Year`").show());

-- Year wise security delay from 2003-2010
spark.time(spark.sql("SELECT `Year`, avg((`SecurityDelay` / `ArrDelay`)*100) AS `AvgSecurityDelay` FROM delay_flights GROUP BY `Year` ORDER BY `Year`").show());

-- ========================================================
-- troubleshooting
-- ========================================================
ps -ef | grep spark-shell
kill -9 processID