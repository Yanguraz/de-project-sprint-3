CREATE TABLE IF NOT EXISTS mart.f_customer_retention (
    new_customers_count BIGINT,
    returning_customers_count BIGINT,
    refunded_customer_count BIGINT,
    period_name VARCHAR(10),
    period_id INT,
    item_id INT,
    new_customers_revenue NUMERIC(14, 2),
    returning_customers_revenue NUMERIC(14, 2),
    customers_refunded BIGINT,
    UNIQUE(period_id, item_id)
);

DELETE FROM mart.f_customer_retention mfr
WHERE mfr.period_id = DATE_PART('week', '{{ds}}'::timestamp);

WITH SalesData AS (
    SELECT fsl.*, dc.week_of_year,
           CASE WHEN fsl.payment_amount < 0 THEN 'refunded' ELSE 'shipped' END AS status
    FROM mart.f_sales fsl
    JOIN mart.d_calendar dc ON dc.date_id = fsl.date_id
    ORDER BY fsl.customer_id
),
NewCustomerCount AS (
    SELECT customer_id, week_of_year, item_id, COUNT(DISTINCT date_id) AS c11
    FROM SalesData
    GROUP BY customer_id, week_of_year, item_id
    HAVING COUNT(DISTINCT date_id) = 1
),
DistinctNewCustomerCount AS (
    SELECT week_of_year, item_id, COUNT(DISTINCT customer_id) AS new_customers_count
    FROM NewCustomerCount
    GROUP BY week_of_year, item_id
),
ReturningCustomerCount AS (
    SELECT customer_id, week_of_year, item_id, COUNT(DISTINCT date_id) AS returning_customers_count
    FROM SalesData
    GROUP BY customer_id, week_of_year, item_id
    HAVING COUNT(DISTINCT date_id) > 1
),
DistinctReturningCustomerCount AS (
    SELECT week_of_year, item_id, COUNT(DISTINCT customer_id) AS returning_customers_count
    FROM ReturningCustomerCount
    GROUP BY week_of_year, item_id
),
RefundedCustomerCount AS (
    SELECT week_of_year, item_id, COUNT(DISTINCT customer_id) AS refunded_customer_count
    FROM SalesData
    WHERE status = 'refunded'
    GROUP BY week_of_year, item_id
),
NewCustomersRevenue AS (
    SELECT nc.week_of_year, nc.item_id, SUM(SalesData.payment_amount) AS new_customers_revenue
    FROM DistinctNewCustomerCount nc
    LEFT JOIN SalesData ON nc.week_of_year = SalesData.week_of_year
                       AND nc.item_id = SalesData.item_id
    GROUP BY nc.week_of_year, nc.item_id
),
ReturningCustomersRevenue AS (
    SELECT rtc.week_of_year, rtc.item_id, SUM(SalesData.payment_amount) AS returning_customers_revenue
    FROM DistinctReturningCustomerCount rtc
    LEFT JOIN SalesData ON rtc.week_of_year = SalesData.week_of_year
                       AND rtc.item_id = SalesData.item_id
    GROUP BY rtc.week_of_year, rtc.item_id
),
CustomerRefundedCount AS (
    SELECT customer_id, week_of_year, item_id, COUNT(*) AS c
    FROM SalesData
    WHERE SalesData.status = 'refunded'
    GROUP BY customer_id, week_of_year, item_id
),
CustomersRefundedCount AS (
    SELECT week_of_year, item_id, SUM(c) AS customers_refunded
    FROM CustomerRefundedCount
    GROUP BY week_of_year, item_id
)

INSERT INTO mart.f_customer_retention
SELECT DISTINCT
    dnc.new_customers_count,
    drc.returning_customers_count,
    rc.refunded_customer_count,
    'weekly' AS period_name,
    dcl.week_of_year AS period_id,
    di.item_id,
    ncr.new_customers_revenue,
    rcr.returning_customers_revenue,
    crc.customers_refunded
FROM mart.d_item di
LEFT JOIN SalesData dcl ON 1 = 1
LEFT JOIN DistinctNewCustomerCount dnc ON dcl.week_of_year = dnc.week_of_year
                                   AND di.item_id = dnc.item_id
LEFT JOIN DistinctReturningCustomerCount drc ON dcl.week_of_year = drc.week_of_year
                                        AND di.item_id = drc.item_id
LEFT JOIN RefundedCustomerCount rc ON dcl.week_of_year = rc.week_of_year
                              AND di.item_id = rc.item_id
LEFT JOIN NewCustomersRevenue ncr ON dcl.week_of_year = ncr.week_of_year
                                   AND di.item_id = ncr.item_id
LEFT JOIN ReturningCustomersRevenue rcr ON dcl.week_of_year = rcr.week_of_year
                                        AND di.item_id = rcr.item_id
LEFT JOIN CustomersRefundedCount crc ON dcl.week_of_year = crc.week_of_year
                                 AND di.item_id = crc.item_id
WHERE dcl.week_of_year = DATE_PART('week', '{{ds}}'::timestamp);
