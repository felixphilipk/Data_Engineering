      CREATE TABLE IF NOT EXISTS bookings(
            booking_id String,
            customer_id String,
            driver_id String,
            customer_app_version String,
            customer_phone_os_version String,
            pickup_lat String,
            pickup_lon String,
            drop_lat String,
            drop_lon String,
            pickup_timestamp timestamp,
            drop_timestamp timestamp,
            trip_fare float,
            tip_amount float,
            currency_code String,
            cab_color String,
            cab_registration_number String,
            customer_rating_by_driver int,
            rating_by_customer int,
            passenger_count int 
      )
     ROW FORMAT DELIMITED 
     FIELDS TERMINATED BY ',' 
     LINES TERMINATED BY '\n'
     STORED AS TEXTFILE;


      Load data inpath '/user/ec2-user/Data/Booking_Batch_Data/part-m-00000' into table bookings;






CREATE TABLE IF NOT EXISTS clickstream (
    customer_id int,
    app_version string,
    os_version string,
    lat double,
    lon double,
    page_id string,
    button_id string,
    is_button_click boolean,
    is_page_view boolean,
    is_scroll_up boolean,
    is_scroll_down boolean,
    `timestamp` timestamp
)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ',' 
LINES TERMINATED BY '\n'
STORED AS TEXTFILE;

Load data inpath '/user/ec2-user/Data/CleanedClickStream/part-00000-96a0efb7-ba76-4358-bc8a-11be9fc9c877-c000.csv' into table clickstream;  




CREATE TABLE IF NOT EXISTS datewise_total_bookings(
    `date` DATE,
    total_bookings INT
)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ',' 
LINES TERMINATED BY '\n'
STORED AS TEXTFILE;


Load data inpath  '/user/ec2-user/Data/Datewise_Total_Bookings/part-00000-e482f27d-5819-4081-ab8f-6bb3fd509036-c000.csv'  into table datewise_total_bookings;


-- alternative clickstream table with only strings

Create  table if not exists clickstream (
       customer_id String,
        app_version String,
        os_version String,
        lat String,
        lon String,
        page_id String,
        button_id String,
        is_button_click String,
        is_page_view String,
        is_scroll_up String,
        is_scroll_down String,
        `timestamp` String)
row format delimited 
fields terminated by ',' 
lines terminated by '\n'
stored as textfile;


Load data inpath '/user/ec2-user/Data/ClickStream/part-00000-907fa318-c718-463a-adad-565a2b6420db-c000.csv' into table clickstream;