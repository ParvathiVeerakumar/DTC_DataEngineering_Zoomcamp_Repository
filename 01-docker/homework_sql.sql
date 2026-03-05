SELECT
	*
FROM
	GREEN_TAXI_DATA_2025;

--Q3: For the trips in November 2025 (lpep_pickup_datetime between '2025-11-01' and '2025-12-01', exclusive of the upper bound), how many trips had a trip_distance of less than or equal to 1 mile?
SELECT
	CAST(LPEP_PICKUP_DATETIME AS DATE) AS "day"
FROM
	GREEN_TAXI_DATA_2025;

SELECT
	*
FROM
	GREEN_TAXI_DATA_2025
WHERE
	CAST(LPEP_PICKUP_DATETIME AS DATE) >= '2025-11-01'
	AND CAST(LPEP_PICKUP_DATETIME AS DATE) < '2025-12-01'
	AND TRIP_DISTANCE <= 1;

-- Q4: Which was the pick up day with the longest trip distance? Only consider trips with trip_distance less than 100 miles (to exclude data errors).
SELECT
	CAST(LPEP_PICKUP_DATETIME AS DATE) AS "Day",
	MAX(TRIP_DISTANCE) AS "Max_trip_distance"
FROM
	GREEN_TAXI_DATA_2025
WHERE
	TRIP_DISTANCE < 100
GROUP BY
	CAST(LPEP_PICKUP_DATETIME AS DATE)
ORDER BY
	MAX(TRIP_DISTANCE) DESC;
-- Q5: For the passengers picked up in the zone named "East Harlem North" in November 2025, which was the drop off zone that had the largest tip?
SELECT
	ZPU."Zone" AS "pickup_zone",
	SUM(GTD2025.TOTAL_AMOUNT)
FROM
	GREEN_TAXI_DATA_2025 GTD2025
	JOIN ZONES ZPU ON GTD2025."PULocationID" = ZPU."LocationID"
WHERE
	CAST(LPEP_PICKUP_DATETIME AS DATE) = '2025-11-18'
GROUP BY
	PICKUP_ZONE
ORDER BY
	SUM(TOTAL_AMOUNT) DESC;

-- Q6: For the passengers picked up in the zone named "East Harlem North" in November 2025, which was the drop off zone that had the largest tip? 
SELECT
	ZDO."Zone",
	MAX(GTD2025.TIP_AMOUNT)
FROM
	GREEN_TAXI_DATA_2025 GTD2025
	JOIN ZONES ZPU ON GTD2025."PULocationID" = ZPU."LocationID"
	JOIN ZONES ZDO ON GTD2025."DOLocationID" = ZDO."LocationID"
WHERE
	ZPU."Zone" = 'East Harlem North'
	AND CAST(GTD2025.LPEP_PICKUP_DATETIME AS DATE) >= '2025-11-01'
	AND CAST(GTD2025.LPEP_PICKUP_DATETIME AS DATE) < '2025-12-01'
GROUP BY
	ZDO."Zone"
ORDER BY
	MAX(GTD2025.TIP_AMOUNT) DESC;