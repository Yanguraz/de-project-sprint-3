CREATE TABLE mart.f_customer_retention (
    new_customers_count INT,
    returning_customers_count INT,
    refunded_customer_count INT,
    period_name VARCHAR(50),
    period_id INT,
    item_id INT,
    new_customers_revenue DECIMAL(10, 2),
    returning_customers_revenue DECIMAL(10, 2),
    customers_refunded INT,
    PRIMARY KEY (period_id, item_id)
);
