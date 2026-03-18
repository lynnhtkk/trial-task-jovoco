-- What percentage of customers have placed orders in more than one quarter?

-- year*4+quarter gives a unique int per quarter across multiple years
WITH customer_quarters AS (
    SELECT
        fs.customer_id,
        COUNT(DISTINCT dd.year * 4 + dd.quarter) AS distinct_quarters
    FROM gold.fact_sales  fs
    JOIN gold.dim_date    dd ON dd.date = fs.date
    WHERE fs.customer_id IS NOT NULL
    GROUP BY fs.customer_id
),

totals AS (
    SELECT
        COUNT(*)                                      AS total_customers,
        COUNT(*) FILTER (WHERE distinct_quarters > 1) AS multi_quarter_customers
    FROM customer_quarters
)

SELECT
    total_customers,
    multi_quarter_customers,
    ROUND(100.0 * multi_quarter_customers / total_customers, 2) AS pct_multi_quarter
FROM totals;
