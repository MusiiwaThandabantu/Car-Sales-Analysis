CREATE OR REPLACE TABLE workspace.default.car_sales_cleaned AS 
SELECT saledate,
  year AS `Manufacturing_Year`,
  COALESCE(make, 'No Information') AS make,
  COALESCE(model, 'No Information') AS model,
  COALESCE(trim, 'No Information') AS trim,
  COALESCE(body, 'No Information') AS body,
  COALESCE(transmission, 'No Information') AS transmission,
  COALESCE(state, 'No Information') AS state,
  vin,
  COALESCE(CAST( condition AS STRING), 'No Information') AS condition,
  odometer,
  COALESCE(color, 'No Information') AS color,
  mmr,
  ROUND(((sellingprice - mmr) / sellingprice) * 100, 2) AS profit_margin, --- Creating a Profit Margin 
  CONCAT('$', FORMAT_NUMBER(sellingprice, 0)) AS sellingprice,
  date_format(to_timestamp(substring(saledate, 5), 'MMM dd yyyy HH:mm:ss'), 'dd MMMM yyyy') AS sale_date,
  dayname(to_timestamp(substring(saledate, 5), 'MMM dd yyyy HH:mm:ss')) AS day_name,
  date_format(to_timestamp(substring(saledate, 5), 'MMM dd yyyy HH:mm:ss'), 'HH:mm:ss') AS sale_time,
  year(to_timestamp(substring(saledate, 5),'MMM dd yyyy HH:mm:ss')) AS sale_year
 
  FROM workspace.default.car_sales;

---Main Code 
WITH base_data AS (
  SELECT
    vin,
    state,
    make,
    odometer,
    model,

    -- Ensure saledate is parsed as DATE/TIMESTAMP
    `sale_date`,
YEAR(TRY_TO_DATE(`sale_date`, 'dd MMMM yyyy'))     AS sale_year,
QUARTER(TRY_TO_DATE(`sale_date`, 'dd MMMM yyyy'))  AS sale_quarter,
MONTH(TRY_TO_DATE(`sale_date`, 'dd MMMM yyyy'))    AS sale_month,

    -- Convert formatted selling price to numeric
    TRY_CAST(
      REGEXP_REPLACE(sellingprice, '[^0-9.]', '')
      AS DOUBLE
    ) AS sellingprice_numeric,

    mmr
  FROM workspace.default.car_sales_cleaned
  WHERE mmr > 0
),

aggregated_data AS (
  SELECT
    sale_year,
    sale_quarter,
    sale_month,
    state,
    make,
    odometer,
    model,
    COUNT(DISTINCT vin) AS units_sold,
    SUM(sellingprice_numeric) AS total_revenue,
    AVG(sellingprice_numeric) AS Avg_price,
    SUM(mmr) AS total_mmr
  FROM base_data
  WHERE sellingprice_numeric > 0
  GROUP BY
    sale_year,
    sale_quarter,
    sale_month,
    state,
    make,
    odometer,
    model
)

SELECT
  sale_year AS `Manufacturing Year`,
  sale_quarter,
  sale_month,
  state,
  make,
  odometer,
  model,
  units_sold,
  total_revenue,
  avg_price,
  ROUND(
    (total_revenue - total_mmr) / total_revenue * 100,
    2
  ) AS profit_margin_pct,
  CASE
    WHEN (total_revenue - total_mmr) / total_revenue >= 0.20 THEN 'High Margin'
    WHEN (total_revenue - total_mmr) / total_revenue >= 0.10 THEN 'Medium Margin'
    ELSE 'Low Margin'
  END AS performance_tier
FROM aggregated_data
ORDER BY
  `Manufacturing Year`,
  sale_quarter,
  sale_month,
  total_revenue DESC
