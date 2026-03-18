-- Which two products are most frequently bought together in the same order? Show the top 10 product pairs with their co-occurrence frequency.

-- self-join on order_id, product_id_2 > product_id_1 avoids (A,B) and (B,A) duplicates
SELECT
    p1.title  AS product_1,
    p2.title  AS product_2,
    COUNT(*)  AS co_occurrences
FROM gold.fact_sales   fs1
JOIN gold.fact_sales   fs2 ON  fs2.order_id   = fs1.order_id
                           AND fs2.product_id > fs1.product_id
JOIN gold.dim_products p1  ON  p1.product_id  = fs1.product_id
JOIN gold.dim_products p2  ON  p2.product_id  = fs2.product_id
GROUP BY p1.title, p2.title
ORDER BY co_occurrences DESC
LIMIT 10;
