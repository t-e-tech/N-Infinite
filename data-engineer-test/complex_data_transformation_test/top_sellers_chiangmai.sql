CREATE TABLE IF NOT EXISTS top_sellers_chiangmai (
    transaction_date DATE,
    product_id VARCHAR(255),
    sales NUMERIC,
    PRIMARY KEY (transaction_date, product_id)
);
