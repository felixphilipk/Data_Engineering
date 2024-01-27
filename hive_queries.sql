
--- Calculating different drivers for each customer.
SELECT customer_id,
COUNT(driver_id) AS Total_driver_count 
FROM bookings
group by customer_id;

--Counting total rides taken by each customer.
SELECT customer_id,
COUNT(booking_id) as Total_rides_taken
FROM bookings
group by customer_id;


--Determining conversion ratios from clickstream.
SELECT * FROM clickstream LIMIT 10;


with temp_table AS(
SELECT 
CASE WHEN is_button_click = True THEN 1 ELSE 0 END AS is_button_bool,
CASE WHEN is_page_view = True THEN 1 ELSE 0 END AS is_page_view_bool
FROM clickstream
)
SELECT SUM(is_button_bool)/SUM(is_page_view_bool) AS conversion_raio 
FROM temp_table;


--Counting trips by cab color.

SELECT cab_color, COUNT(booking_id)
FROM bookings
GROUP BY cab_color
HAVING cab_color = 'black';

--Summarizing tips given date-wise.

SELECT 
    DATE(pickup_timestamp) AS trip_date, 
    SUM(tip_amount) AS total_tips
FROM 
    bookings
GROUP BY 
    DATE(pickup_timestamp)
ORDER BY 
    trip_date;



-- Identifying bookings with low customer ratings.

SELECT 
    CONCAT(YEAR(pickup_timestamp), '-', LPAD(MONTH(pickup_timestamp), 2, '0')) AS booking_month,
    COUNT(*) as total_low_ratings
FROM 
    bookings
WHERE 
    rating_by_customer < 2
GROUP BY 
    CONCAT(YEAR(pickup_timestamp), '-', LPAD(MONTH(pickup_timestamp), 2, '0'))
ORDER BY 
    booking_month;


-- Counting iOS users among customers.

SELECT 
    COUNT(DISTINCT customer_id) AS total_ios_users
FROM 
    bookings
WHERE 
    LOWER(customer_phone_os_version) LIKE '%ios%';
    
