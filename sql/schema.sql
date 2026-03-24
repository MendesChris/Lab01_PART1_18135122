CREATE TABLE fact_trips (
    trip_id SERIAL PRIMARY KEY,
    pickup_datetime TIMESTAMP,
    dropoff_datetime TIMESTAMP,
    passenger_count INT,
    trip_distance FLOAT,
    fare_amount FLOAT,
    tip_amount FLOAT,
    total_amount FLOAT,
    payment_type INT,
    pickup_location_id INT,
    dropoff_location_id INT
);

CREATE TABLE dim_date (
    date DATE PRIMARY KEY,
    year INT,
    month INT,
    day INT,
    weekday INT
);

CREATE TABLE dim_payment (
    payment_type INT PRIMARY KEY,
    payment_description TEXT
);