-- Divide customers into three groups (High / Mid / Low Value) based on their total revenue over the last 12 months. Use Window Functions.

-- last 12 months relative to the latest order in the data
WITH reference_date AS (
    SELECT MAX(date) AS max_date FROM gold.fact_sales
),

customer_revenue AS (
    SELECT
        dc.customer_id,
        dc.name,
        dc.type,
        SUM(fs.revenue) AS total_revenue
    FROM gold.fact_sales    fs
    JOIN gold.dim_customers dc ON dc.customer_id = fs.customer_id
    JOIN reference_date     rd ON fs.date >= rd.max_date - INTERVAL '12 months'
    GROUP BY dc.customer_id, dc.name, dc.type
),

segmented AS (
    SELECT
        customer_id,
        name,
        type,
        ROUND(total_revenue, 2)                     AS total_revenue,
        -- NTILE(3) splits into 3 equal buckets by revenue descending
        NTILE(3) OVER (ORDER BY total_revenue DESC) AS bucket
    FROM customer_revenue
)

SELECT
    customer_id,
    name,
    type,
    total_revenue,
    CASE bucket
        WHEN 1 THEN 'High Value'
        WHEN 2 THEN 'Mid Value'
        WHEN 3 THEN 'Low Value'
    END AS segment
FROM segmented
ORDER BY total_revenue DESC;
