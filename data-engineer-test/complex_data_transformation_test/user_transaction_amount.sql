CREATE TABLE IF NOT EXISTS user_transaction_amount (
    user_id VARCHAR(255) PRIMARY KEY,
    total_sales NUMERIC,
    average_sales NUMERIC
);
