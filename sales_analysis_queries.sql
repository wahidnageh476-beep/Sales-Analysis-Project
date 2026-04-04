-- ============================================================
--  SALES ANALYSIS PROJECT — SQL Queries
--  Author : Wahid Nageh Ahmed
--  Dataset: 10,200 sales records | Year 2023
-- ============================================================


-- ────────────────────────────────────────────────────────────
-- 0. CREATE & LOAD TABLE
-- ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS sales (
    order_id      TEXT PRIMARY KEY,
    order_date    DATE,
    product_id    TEXT,
    product_name  TEXT,
    category      TEXT,
    region        TEXT,
    sales_channel TEXT,
    quantity      INTEGER,
    unit_price    NUMERIC(10,2),
    discount_pct  NUMERIC(4,2),
    revenue       NUMERIC(12,2),
    cost          NUMERIC(12,2),
    profit        NUMERIC(12,2),
    order_status  TEXT
);

-- (In SQLite / DuckDB you'd run: .import sales_raw.csv sales)


-- ────────────────────────────────────────────────────────────
-- 1. OVERVIEW — Total KPIs (Completed orders only)
-- ────────────────────────────────────────────────────────────
SELECT
    COUNT(*)                          AS total_orders,
    SUM(quantity)                     AS total_units_sold,
    ROUND(SUM(revenue),      2)       AS total_revenue,
    ROUND(SUM(profit),       2)       AS total_profit,
    ROUND(AVG(revenue),      2)       AS avg_order_value,
    ROUND(SUM(profit)
         / NULLIF(SUM(revenue),0)*100,2) AS profit_margin_pct
FROM sales
WHERE order_status = 'Completed';


-- ────────────────────────────────────────────────────────────
-- 2. MONTHLY REVENUE TREND  (GROUP BY + date functions)
-- ────────────────────────────────────────────────────────────
SELECT
    STRFTIME('%Y-%m', order_date)            AS month,
    COUNT(*)                                  AS orders,
    SUM(quantity)                             AS units_sold,
    ROUND(SUM(revenue), 2)                    AS revenue,
    ROUND(SUM(profit),  2)                    AS profit,
    ROUND(AVG(revenue), 2)                    AS avg_order_value
FROM sales
WHERE order_status = 'Completed'
GROUP BY 1
ORDER BY 1;


-- ────────────────────────────────────────────────────────────
-- 3. TOP PRODUCTS BY REVENUE  (identifies the 35% insight)
-- ────────────────────────────────────────────────────────────
WITH product_revenue AS (
    SELECT
        product_id,
        product_name,
        category,
        COUNT(*)              AS orders,
        SUM(quantity)         AS units_sold,
        ROUND(SUM(revenue),2) AS total_revenue,
        ROUND(SUM(profit), 2) AS total_profit,
        ROUND(AVG(discount_pct)*100,1) AS avg_discount_pct
    FROM sales
    WHERE order_status = 'Completed'
    GROUP BY product_id, product_name, category
),
grand_total AS (
    SELECT SUM(revenue) AS grand_rev FROM sales WHERE order_status='Completed'
)
SELECT
    pr.*,
    ROUND(pr.total_revenue / gt.grand_rev * 100, 2) AS revenue_share_pct,
    SUM(pr.total_revenue) OVER (
        ORDER BY pr.total_revenue DESC
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    )                                                AS cumulative_revenue
FROM product_revenue pr, grand_total gt
ORDER BY pr.total_revenue DESC;


-- ────────────────────────────────────────────────────────────
-- 4. CATEGORY PERFORMANCE  (GROUP BY + aggregation)
-- ────────────────────────────────────────────────────────────
SELECT
    category,
    COUNT(*)                              AS orders,
    SUM(quantity)                         AS units_sold,
    ROUND(SUM(revenue),2)                 AS revenue,
    ROUND(SUM(profit), 2)                 AS profit,
    ROUND(SUM(profit)/SUM(revenue)*100,2) AS margin_pct
FROM sales
WHERE order_status = 'Completed'
GROUP BY category
ORDER BY revenue DESC;


-- ────────────────────────────────────────────────────────────
-- 5. REGIONAL SALES BREAKDOWN  (JOIN simulation via GROUP BY)
-- ────────────────────────────────────────────────────────────
SELECT
    region,
    sales_channel,
    COUNT(*)              AS orders,
    ROUND(SUM(revenue),2) AS revenue,
    ROUND(SUM(profit), 2) AS profit
FROM sales
WHERE order_status = 'Completed'
GROUP BY region, sales_channel
ORDER BY revenue DESC;


-- ────────────────────────────────────────────────────────────
-- 6. RETURN RATE BY PRODUCT  (Subquery)
-- ────────────────────────────────────────────────────────────
SELECT
    product_name,
    category,
    total_orders,
    returned_orders,
    ROUND(returned_orders * 100.0 / total_orders, 2) AS return_rate_pct
FROM (
    SELECT
        product_name,
        category,
        COUNT(*)                                              AS total_orders,
        SUM(CASE WHEN order_status = 'Returned' THEN 1 ELSE 0 END) AS returned_orders
    FROM sales
    GROUP BY product_name, category
) sub
ORDER BY return_rate_pct DESC;


-- ────────────────────────────────────────────────────────────
-- 7. REVENUE GROWTH MOM — Month-over-Month  (Window Function)
-- ────────────────────────────────────────────────────────────
WITH monthly AS (
    SELECT
        STRFTIME('%Y-%m', order_date) AS month,
        ROUND(SUM(revenue), 2)        AS revenue
    FROM sales
    WHERE order_status = 'Completed'
    GROUP BY 1
)
SELECT
    month,
    revenue,
    LAG(revenue) OVER (ORDER BY month)  AS prev_month_revenue,
    ROUND(
        (revenue - LAG(revenue) OVER (ORDER BY month))
        / NULLIF(LAG(revenue) OVER (ORDER BY month), 0) * 100
    , 2) AS mom_growth_pct
FROM monthly
ORDER BY month;


-- ────────────────────────────────────────────────────────────
-- 8. CHANNEL EFFECTIVENESS  (GROUP BY + aggregation)
-- ────────────────────────────────────────────────────────────
SELECT
    sales_channel,
    COUNT(*)                              AS orders,
    ROUND(SUM(revenue), 2)                AS revenue,
    ROUND(AVG(revenue), 2)                AS avg_order_value,
    ROUND(SUM(profit)/SUM(revenue)*100,2) AS margin_pct
FROM sales
WHERE order_status = 'Completed'
GROUP BY sales_channel
ORDER BY revenue DESC;


-- ────────────────────────────────────────────────────────────
-- 9. DISCOUNT IMPACT ON PROFIT  (CTE + GROUP BY)
-- ────────────────────────────────────────────────────────────
WITH discount_buckets AS (
    SELECT *,
        CASE
            WHEN discount_pct = 0    THEN '0% — No Discount'
            WHEN discount_pct <= 0.05 THEN '1-5%'
            WHEN discount_pct <= 0.10 THEN '6-10%'
            WHEN discount_pct <= 0.15 THEN '11-15%'
            ELSE '16-20%'
        END AS discount_tier
    FROM sales
    WHERE order_status = 'Completed'
)
SELECT
    discount_tier,
    COUNT(*)                              AS orders,
    ROUND(SUM(revenue), 2)                AS revenue,
    ROUND(SUM(profit),  2)                AS profit,
    ROUND(SUM(profit)/SUM(revenue)*100,2) AS margin_pct
FROM discount_buckets
GROUP BY discount_tier
ORDER BY discount_tier;
-- sync commit