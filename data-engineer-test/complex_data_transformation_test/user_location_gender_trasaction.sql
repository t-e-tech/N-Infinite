CREATE TABLE IF NOT EXISTS user_location_gender_transaction (
    location VARCHAR(255),
    gender VARCHAR(50),
    total_sales NUMERIC,
    min_sales NUMERIC,
    max_sales NUMERIC,
    average_sales NUMERIC,
    PRIMARY KEY (location, gender)
);
