CREATE TABLE IF NOT EXISTS raw_transaction (
    order_id VARCHAR(255),
    user_id VARCHAR(255),
    product_id VARCHAR(255),
    quantity INT,
    amount INT,
    order_date DATE,
);
