SELECT * FROM public.zones
LIMIT 100;

SELECT * FROM public.yellow_taxi_data
LIMIT 10000;

-- ==============================
-- LEVEL 1: BASIC (JOIN + GROUP BY)
-- ==============================

-- 1. Liệt kê top 10 pickup zones có số chuyến đi nhiều nhất.
SELECT
	zones."Zone",
	COUNT(*) as trip_count
FROM yellow_taxi_data
INNER JOIN zones
	ON zones."LocationID" = yellow_taxi_data."PULocationID"
GROUP BY zones."Zone"
ORDER BY COUNT(*) DESC
LIMIT 10;

-- 2. Liệt kê top 10 dropoff zones có số chuyến đi nhiều nhất.
SELECT
	zones."Zone",
	COUNT(*) as trip_count
FROM yellow_taxi_data
INNER JOIN zones
	ON zones."LocationID" = yellow_taxi_data."DOLocationID"
GROUP BY zones."Zone"
ORDER BY COUNT(*) DESC
LIMIT 10;

-- 3. Tính tổng doanh thu (total_amount) theo từng borough (pickup).
SELECT
	zones."Borough",
	SUM(yellow_taxi_data.total_amount) AS total_amount
FROM yellow_taxi_data
INNER JOIN zones
	ON zones."LocationID" = yellow_taxi_data."PULocationID"
GROUP BY zones."Borough"
ORDER BY total_amount DESC
LIMIT 10;

-- 4. Tính giá tiền trung bình (total_amount) theo từng pickup zone.
SELECT
	zones."Zone",
	AVG(yellow_taxi_data.total_amount) AS average_total_amount
FROM yellow_taxi_data
INNER JOIN zones
	ON zones."LocationID" = yellow_taxi_data."PULocationID"
GROUP BY zones."Zone"
ORDER BY average_total_amount DESC
LIMIT 10;

-- 5. Tìm 5 zone có quãng đường trung bình (trip_distance) dài nhất.
SELECT
	zones."Zone",
	AVG(yellow_taxi_data.trip_distance) AS average_trip_distance
FROM yellow_taxi_data
INNER JOIN zones
	ON zones."LocationID" = yellow_taxi_data."PULocationID"
WHERE yellow_taxi_data.trip_distance > 0
GROUP BY zones."Zone"
ORDER BY average_trip_distance DESC
LIMIT 10;

-- ==============================
-- LEVEL 2: TIME ANALYSIS
-- ==============================

-- 6. Đếm số chuyến đi theo từng giờ trong ngày (dựa trên pickup time).
SELECT
	EXTRACT(HOUR FROM "tpep_pickup_datetime") AS pickup_hour,
	COUNT(*)
FROM yellow_taxi_data
GROUP BY pickup_hour;

-- 7. Tìm giờ cao điểm nhất (giờ có nhiều trip nhất).
WITH hourly AS (
	SELECT
		EXTRACT(HOUR FROM "tpep_pickup_datetime") AS pickup_hour,
		COUNT(*) AS trip_count
	FROM yellow_taxi_data
	GROUP BY pickup_hour 
)
SELECT *
FROM hourly
WHERE trip_count = (SELECT MAX(trip_count) FROM hourly);

-- 8. So sánh tổng số chuyến đi giữa ngày thường và cuối tuần.
SELECT (
		CASE 
			WHEN EXTRACT(DOW FROM "tpep_pickup_datetime") IN (0, 6) 
				THEN 'WEEKEND'
			ELSE
				'WEEKDAY'
		END
	) AS day_type,
	COUNT(*) AS trip_count
FROM yellow_taxi_data
GROUP BY day_type;

-- 9. Tính thời gian trung bình mỗi chuyến đi (dropoff - pickup).
SELECT AVG(
		EXTRACT(EPOCH FROM ("tpep_dropoff_datetime" - "tpep_pickup_datetime"))
	) AS avg_trip_duration_seconds,
	AVG(
		EXTRACT(EPOCH FROM ("tpep_dropoff_datetime" - "tpep_pickup_datetime")) / 60
	) AS avg_trip_duration_minutes,
	AVG(
		EXTRACT(EPOCH FROM ("tpep_dropoff_datetime" - "tpep_pickup_datetime")) / 3600
	) AS avg_trip_duration_hours
FROM yellow_taxi_data;

-- 10. Tính số chuyến đi theo từng ngày trong tháng.
SELECT
	DATE("tpep_pickup_datetime") AS date_time,
	COUNT(*) AS trip_count
FROM yellow_taxi_data
GROUP BY date_time
ORDER BY date_time;

-- ==============================
-- LEVEL 3: INTERMEDIATE (OD ANALYSIS)
-- ==============================

-- 11. Liệt kê top 20 tuyến đường (pickup → dropoff) có nhiều chuyến nhất.
SELECT
	CONCAT(pickup_loc."Borough", '/', pickup_loc."Zone") AS pickup,
	CONCAT(dropoff_loc."Borough", '/', dropoff_loc."Zone") AS dropoff,
	COUNT(*) AS total_trips
FROM
	yellow_taxi_data
JOIN zones pickup_loc
	ON pickup_loc."LocationID" = yellow_taxi_data."PULocationID"
JOIN zones dropoff_loc
	ON dropoff_loc."LocationID" = yellow_taxi_data."DOLocationID"
GROUP BY pickup, dropoff
ORDER BY total_trips DESC
LIMIT 20;
	

-- 12. Tính tổng doanh thu theo từng tuyến đường (pickup → dropoff).
WITH revenue_by_route AS (
	SELECT
		"PULocationID",
		"DOLocationID",
		SUM("total_amount") AS total_amount
	FROM
		yellow_taxi_data
		WHERE
			yellow_taxi_data."PULocationID" IS NOT NULL
			AND
			yellow_taxi_data."DOLocationID" IS NOT NULL
			AND
			yellow_taxi_data."PULocationID" <> yellow_taxi_data."DOLocationID"
	GROUP BY
		"PULocationID",
		"DOLocationID"
)
SELECT
	CONCAT(pickup_loc."Borough", '/', pickup_loc."Zone") AS pickup,
	CONCAT(dropoff_loc."Borough", '/', dropoff_loc."Zone") AS dropoff,
	total_amount
FROM
	revenue_by_route
JOIN zones pickup_loc
	ON pickup_loc."LocationID" = revenue_by_route."PULocationID"
JOIN zones dropoff_loc
	ON dropoff_loc."LocationID" = revenue_by_route."DOLocationID"
ORDER BY total_amount DESC
LIMIT 5;

-- 13. Tìm 5 tuyến có average revenue per trip cao nhất (tối thiểu 100 chuyến).
WITH average_by_route AS (
	SELECT
		"PULocationID",
		"DOLocationID",
		AVG(total_amount) AS avg_amount
	FROM
		yellow_taxi_data
	GROUP BY "PULocationID", "DOLocationID"
	HAVING COUNT(*) > 100
)
SELECT
	CONCAT(pickup_loc."Borough", '/', pickup_loc."Zone") AS pickup,
	CONCAT(dropoff_loc."Borough", '/', dropoff_loc."Zone") AS dropoff,
	avg_amount
FROM
	average_by_route
JOIN zones pickup_loc
	ON pickup_loc."LocationID" = average_by_route."PULocationID"
JOIN zones dropoff_loc
	ON dropoff_loc."LocationID" = average_by_route."DOLocationID"
ORDER BY avg_amount DESC
LIMIT 5;

-- 14. Tính tỷ lệ chuyến đi nội borough (pickup và dropoff cùng borough).
SELECT
	COUNT(*) * 100.00 / (SELECT COUNT(*) FROM yellow_taxi_data) AS intra_borough_percentage
FROM
	yellow_taxi_data
JOIN zones pickup_loc
	ON pickup_loc."LocationID" = yellow_taxi_data."PULocationID"
JOIN zones dropoff_loc
	ON dropoff_loc."LocationID" = yellow_taxi_data."DOLocationID"
WHERE
	pickup_loc."Borough" = dropoff_loc."Borough"

-- 15. Pickup borough nào có nhiều chuyến đi sang borough khác nhất?
select
	CONCAT(pu."Borough") AS pickup,
	COUNT(*)
FROM
	yellow_taxi_data ytd
JOIN zones pu ON pu."LocationID" = ytd."PULocationID"
JOIN zones dof ON dof."LocationID" = ytd."DOLocationID"
WHERE pu."Borough" <> dof."Borough"
GROUP BY pu."Borough"
ORDER BY COUNT(*) DESC
LIMIT 1;

-- ==============================
-- LEVEL 4: ADVANCED (WINDOW FUNCTION)
-- ==============================

-- 16. Tìm top 3 pickup zones có nhiều chuyến nhất trong mỗi borough.
SELECT *
FROM (
	SELECT
		pu."Zone" AS zones,
		pu."Borough" AS borough,
		COUNT(*) AS total_trips_in_borough,
		ROW_NUMBER() OVER(
			PARTITION BY pu."Borough"
			ORDER BY COUNT(*) DESC
		) AS rn
	FROM
		yellow_taxi_data ytd
	JOIN zones pu
		ON pu."LocationID" = ytd."PULocationID"
	GROUP BY pu."Borough", pu."Zone"
)
WHERE rn < 4;
	
-- 17. Tìm top 3 tuyến đường có doanh thu cao nhất trong ngày.
WITH revenue_by_route AS (
	SELECT
		DATE("tpep_pickup_datetime") AS date_time,
		"PULocationID",
		"DOLocationID",
		SUM("total_amount") AS total_amount
	FROM yellow_taxi_data
	GROUP BY date_time, "PULocationID", "DOLocationID"
	ORDER BY date_time ASC
)
SELECT *
FROM (
	SELECT
		date_time,
		CONCAT(pu."Zone", '/', pu."Borough") AS pickup_location,
		CONCAT(dof."Zone", '/', dof."Borough") AS dropoff_location,
		total_amount,
		DENSE_RANK() OVER(
			PARTITION BY date_time
			ORDER BY total_amount DESC
		) AS rn
	FROM revenue_by_route
	JOIN zones pu ON pu."LocationID" = revenue_by_route."PULocationID"
	JOIN zones dof ON dof."LocationID" = revenue_by_route."DOLocationID"
) t
WHERE t.rn < 4;

-- 18. Xếp hạng (rank) các zone theo tổng doanh thu.
WITH zone_revenue AS (
	SELECT
		"PULocationID",
		SUM("total_amount") AS total_amount
	FROM yellow_taxi_data
	WHERE "total_amount" > 0 AND "trip_distance" > 0
	GROUP BY "PULocationID"
)
SELECT *
FROM (
	SELECT
		pu_loc."Zone",
		zr."total_amount",
		DENSE_RANK() OVER(
			ORDER BY zr."total_amount" DESC
		) AS rn
	FROM
		zone_revenue zr
	JOIN zones pu_loc ON pu_loc."LocationID" = zr."PULocationID"
);

-- 19. Tính phần trăm đóng góp doanh thu của mỗi borough trên tổng doanh thu.

-- 20. Tìm zone có mức tăng trưởng số chuyến cao nhất giữa các tháng.


-- ==============================
-- LEVEL 5: DATA QUALITY & DE MINDSET
-- ==============================

-- 21. Đếm số record có pickup_time > dropoff_time.

-- 22. Đếm số chuyến đi có trip_distance = 0 nhưng total_amount > 0.

-- 23. Tính tỷ lệ % NULL của từng cột trong bảng yellow_taxi_data.

-- 24. Tạo bảng summary gồm:
--     zone, month, total_trips, total_revenue, avg_trip_distance.

-- 25. Tạo materialized view chỉ chứa dữ liệu của tháng gần nhất.


-- ==============================
-- LEVEL 6: HARD / INTERVIEW STYLE
-- ==============================

-- 26. Tìm 5 zone có doanh thu cao nhưng số chuyến đi thấp (avg revenue/trip cao).

-- 27. Tìm tuyến đường có thời gian trung bình dài nhất.

-- 28. Tìm giờ trong ngày có revenue per trip cao nhất.

-- 29. Xác định 10 tuyến đường đóng góp 50% tổng doanh thu.

-- 30. Tìm các trip bất thường dựa trên:
--     - duration quá dài
--     - distance quá ngắn
--     - fare quá cao so với distance
