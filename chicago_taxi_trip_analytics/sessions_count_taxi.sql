-- This query calculates the number of distinct sessions for each taxi in the Chicago Taxi Trips dataset.   
-- A session is defined as a series of trips where the gap between the end of one trip and the start of the next is less than 8 hours.
-- If the gap is 8 hours or more, it is considered the start of a new session.


WITH trips_ordered AS (
  SELECT taxi_id,
         trip_start_timestamp,
         trip_end_timestamp,
         LAG(trip_end_timestamp) OVER (
           PARTITION BY taxi_id 
           ORDER BY trip_start_timestamp
         ) AS prev_end
  FROM `bigquery-public-data.chicago_taxi_trips.taxi_trips`
  WHERE trip_start_timestamp IS NOT NULL
    AND trip_end_timestamp IS NOT NULL
),
sessions AS (
  SELECT taxi_id,
         trip_start_timestamp,
         trip_end_timestamp,
         CASE 
           WHEN prev_end IS NULL 
             OR TIMESTAMP_DIFF(trip_start_timestamp, prev_end, SECOND) >= 28800 
           THEN 1 
           ELSE 0 
         END AS session_start
  FROM trips_ordered
)
SELECT taxi_id,
       COUNT(*) AS total_sessions
FROM sessions
WHERE session_start = 1
GROUP BY taxi_id
ORDER BY total_sessions DESC;
