-- Top 5 products by revenue in the last quarter, split by region.

-- anchor to the most recent quarter in the data
WITH last_qtr AS (
    SELECT MAX(year * 4 + quarter) AS max_yq
    FROM gold.dim_date
),

sales_by_product_region AS (
    SELECT
        dp.title                                          AS product,
        ds.region,
        SUM(fs.revenue)                                   AS total_revenue,
        -- rank within each region separately
        RANK() OVER (
            PARTITION BY ds.region
            ORDER BY SUM(fs.revenue) DESC
        )                                                 AS rnk
    FROM gold.fact_sales   fs
    JOIN gold.dim_date     dd ON dd.date      = fs.date
    JOIN gold.dim_products dp ON dp.product_id = fs.product_id
    JOIN gold.dim_stores   ds ON ds.store_id   = fs.store_id
    WHERE dd.year * 4 + dd.quarter = (SELECT max_yq FROM last_qtr)
      AND fs.store_id IS NOT NULL
    GROUP BY dp.title, ds.region
)

SELECT
    region,
    product,
    ROUND(total_revenue, 2) AS total_revenue,
    rnk                     AS rank_in_region
FROM sales_by_product_region
WHERE rnk <= 5
ORDER BY region, rnk;
