-----------------------------------------------------
-- Bussines questions that this project can answer--
-----------------------------------------------------

-- 1.Total Revenue Over Time
-- How does revenue evolve over time?
SELECT 
    d.year,
    d.month,
    d.day,
    SUM(f.total_amount) AS total_revenue
FROM fact_trips f
JOIN dim_date d
    ON DATE(f.pickup_datetime) = d.date
GROUP BY d.year, d.month, d.day
ORDER BY d.year, d.month, d.day;

-- 2.Average Trip Distance by Passenger Count
-- Do more passengers mean longer trips?
SELECT 
    passenger_count,
    AVG(trip_distance) AS avg_distance
FROM fact_trips
GROUP BY passenger_count
ORDER BY passenger_count;

-- 3.Revenue by Payment Type
-- Which payment method generates more revenue?
SELECT 
    payment_type,
    SUM(total_amount) AS total_revenue,
    AVG(total_amount) AS avg_trip_value
FROM fact_trips
GROUP BY payment_type
ORDER BY total_revenue DESC;

-- 4.Peak Demand (Trips per Weekday)
-- Which days have the highest demand?
SELECT 
    d.weekday,
    COUNT(*) AS total_trips
FROM fact_trips f
JOIN dim_date d
    ON DATE(f.pickup_datetime) = d.date
GROUP BY d.weekday
ORDER BY total_trips DESC;

-- 5.Tip Behavior Analysis
-- Do longer trips generate higher tips?
SELECT 
    CASE 
        WHEN trip_distance < 2 THEN 'Short'
        WHEN trip_distance < 5 THEN 'Medium'
        ELSE 'Long'
    END AS trip_category,
    AVG(tip_amount) AS avg_tip,
    AVG(total_amount) AS avg_total
FROM fact_trips
GROUP BY trip_category;