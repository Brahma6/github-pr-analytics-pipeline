
-- Top 100 Taxi Drivers by Total Tips in the Last 3 Months
SELECT taxi_id,
       SUM(tips) AS total_tips
FROM `bigquery-public-data.chicago_taxi_trips.taxi_trips`
WHERE DATE(trip_start_timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 MONTH)
  AND tips > 0
GROUP BY taxi_id
ORDER BY total_tips DESC
LIMIT 100;


-- Alternative approach using a CTE to determine the date range as data availability till 2023
WITH date_bounds AS (
  SELECT MAX(DATE(trip_start_timestamp)) AS max_date
  FROM `bigquery-public-data.chicago_taxi_trips.taxi_trips`
)
SELECT t.taxi_id,
       SUM(t.tips) AS total_tips
FROM `bigquery-public-data.chicago_taxi_trips.taxi_trips` t
CROSS JOIN date_bounds d
WHERE DATE(t.trip_start_timestamp) >= DATE_SUB(d.max_date, INTERVAL 3 MONTH)
  AND t.tips > 0
GROUP BY t.taxi_id
ORDER BY total_tips DESC
LIMIT 100;

-- Note: Adjust the date range in the WHERE clause as necessary based on data availability.


