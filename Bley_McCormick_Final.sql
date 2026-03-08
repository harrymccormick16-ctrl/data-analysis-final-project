SELECT
  *
FROM
  "HOUSING_COMBINED"."PUBLIC"."HOUSING_COMBINED"
LIMIT
  10;

USE DATABASE "HOUSING_COMBINED";
USE SCHEMA PUBLIC;
 SELECT count(*) FROM "HOUSING_COMBINED";
SHOW COLUMNS in TABLE "HOUSING_COMBINED";

-- Quick sanity checks (not counted toward assignment)

-- Check first 10 rows to confirm structure and data types.
SELECT *
FROM "HOUSING_COMBINED"
LIMIT 10;

-- Check row count to confirm we have the expected 90 city‑year rows.
SELECT COUNT(*) AS row_count
FROM "HOUSING_COMBINED";

-- Inspect columns and types.
SHOW COLUMNS IN TABLE "HOUSING_COMBINED";

-- QUERY 1: RECENT CITY-LEVEL BUBBLE RISK TRENDS (3-Year Average)
-- What: Average price acceleration vs nominal income growth (2022-2024)
-- Why: Identifies cities with  bubble risk (over last 3 years, not one-off spikes)
-- How: Average growth over last 3 years per city
WITH annual_growth AS (
  SELECT 
    city, year, median_home_value, median_household_income, cpi_index,
    -- Annual price growth
    median_home_value / NULLIF(LAG(median_home_value) OVER (PARTITION BY city ORDER BY year), 0) - 1 
      AS annual_price_growth,
    -- Annual NOMINAL income growth (NO CPI adjustment)  
    median_household_income / NULLIF(LAG(median_household_income) OVER (PARTITION BY city ORDER BY year), 0) - 1
      AS annual_nominal_income_growth
  FROM "HOUSING_COMBINED"
  WHERE year >= 2016  -- Need 2015 for LAG(1)
),
city_annual_avg AS (
  SELECT 
    city,
    AVG(annual_price_growth) AS avg_annual_price_growth,
    AVG(annual_nominal_income_growth) AS avg_annual_income_growth,
    AVG(annual_price_growth) - AVG(annual_nominal_income_growth) AS annual_bubble_gap
  FROM annual_growth
  WHERE annual_price_growth IS NOT NULL
  GROUP BY city
  HAVING COUNT(*) >= 5  -- Reliable 5+ years of data
)
SELECT 
  city,
  ROUND(avg_annual_price_growth * 100, 1) AS avg_annual_price_growth_pct,
  ROUND(avg_annual_income_growth * 100, 1) AS avg_annual_income_growth_pct,
  ROUND(annual_bubble_gap * 100, 1) AS annual_bubble_gap_pct,
  CASE 
    WHEN annual_bubble_gap > 0.05 THEN 'HIGH RISK'
    WHEN annual_bubble_gap > 0.02 THEN 'MODERATE RISK'
    ELSE 'SUSTAINABLE'
  END AS bubble_trend_risk
FROM city_annual_avg
ORDER BY annual_bubble_gap DESC;

-- QUERY 2: ANNUAL RENT vs NOMINAL INCOME GROWTH (2015-2024)
-- WHAT: Average ANNUAL rent growth vs income growth per city
-- WHY: Rental market overheating detection (rent outpacing wages)
-- HOW: YoY rent/income growth → AVG() across entire period
------------------------------------------------------------
WITH annual_growth AS (
  SELECT 
    city, year, median_gross_rent, median_household_income,
    -- Annual rent growth
    median_gross_rent / NULLIF(LAG(median_gross_rent) OVER (PARTITION BY city ORDER BY year), 0) - 1 
      AS annual_rent_growth,
    -- Annual NOMINAL income growth  
    median_household_income / NULLIF(LAG(median_household_income) OVER (PARTITION BY city ORDER BY year), 0) - 1
      AS annual_nominal_income_growth
  FROM "HOUSING_COMBINED"
  WHERE year >= 2016  -- Need 2015 for LAG(1)
),
city_annual_avg AS (
  SELECT 
    city,
    AVG(annual_rent_growth) AS avg_annual_rent_growth,
    AVG(annual_nominal_income_growth) AS avg_annual_income_growth,
    AVG(annual_rent_growth) - AVG(annual_nominal_income_growth) AS rent_income_gap
  FROM annual_growth
  WHERE annual_rent_growth IS NOT NULL
  GROUP BY city
  HAVING COUNT(*) >= 5  -- Reliable 5+ years of data
)
SELECT 
  city,
  ROUND(avg_annual_rent_growth * 100, 1) AS avg_annual_rent_growth_pct,
  ROUND(avg_annual_income_growth * 100, 1) AS avg_annual_income_growth_pct,
  ROUND(rent_income_gap * 100, 1) AS annual_rent_income_gap_pct,
  CASE 
    WHEN rent_income_gap > 0.03 THEN 'RENT OVERHEATING'
    WHEN rent_income_gap > 0.01 THEN 'MODERATE PRESSURE'
    ELSE 'BALANCED'
  END AS rent_trend_risk
FROM city_annual_avg
ORDER BY rent_income_gap ASC;


-- QUERY 3: REAL RENT ACCELERATION DETECTION 
-- WHAT: Cities with fastest avg real rent growth acceleration
-- WHY: Flags nominal rental market overheating vs nominal income
-- HOW: Real rent YoY → AVG per city → RANK() by acceleration
------------------------------------------------------------
WITH cpi_adjusted AS (
  SELECT h.city, h.year, h.median_gross_rent,
    LAG(h.median_gross_rent) OVER (PARTITION BY h.city ORDER BY h.year) AS prev_rent,
    AVG(h.median_gross_rent) OVER (PARTITION BY h.city 
      ORDER BY h.year 
      ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) / 
    AVG(h.cpi_index) OVER (PARTITION BY h.city 
      ORDER BY h.year 
      ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) * 100 AS real_rent_index
  FROM "HOUSING_COMBINED" h
  WHERE h.year > 2015
),
rent_growth AS (
  SELECT city, year, real_rent_index,
    (real_rent_index / NULLIF(LAG(real_rent_index) OVER (PARTITION BY city ORDER BY year), 0) - 1)*100 
      AS real_rent_yoy_growth_pct
  FROM cpi_adjusted
)
SELECT city, 
  AVG(real_rent_yoy_growth_pct) AS avg_real_rent_growth_pct,
  RANK() OVER (ORDER BY AVG(real_rent_yoy_growth_pct) DESC) AS acceleration_rank
FROM rent_growth
WHERE real_rent_yoy_growth_pct IS NOT NULL
GROUP BY city
ORDER BY acceleration_rank;

-- QUERY 4: NORMALIZED RESPONSE TO MARKET STRESS 
-- WHAT: Measures volatility (CV) across delinquency, burden, rent, prices + credit-burden correlation  
-- WHY: Reveals market stability (negative correlations = rental market resilience)
-- HOW: Averages CTE → JOIN → STDDEV()/AVG() for CV + CORR() function
------------------------------------------------------------
WITH city_averages AS (
  SELECT city,
    AVG(mortgage_delinquency) AS avg_delinquency,
    AVG(housing_cost_burden_pct) AS avg_burden,
    AVG(median_gross_rent) AS avg_rent,
    AVG(median_home_value) AS avg_home_value
  FROM "HOUSING_COMBINED" GROUP BY city
),
city_volatility AS (
  SELECT h.city,
    STDDEV(h.mortgage_delinquency) AS delinquency_volatility,
    STDDEV(h.housing_cost_burden_pct) / a.avg_burden AS burden_cv_pct,
    STDDEV(h.median_gross_rent) / a.avg_rent AS rent_cv_pct,
    STDDEV(h.median_home_value) / a.avg_home_value AS price_cv_pct,
    CORR(h.mortgage_delinquency, h.housing_cost_burden_pct) AS credit_burden_corr
  FROM "HOUSING_COMBINED" h JOIN city_averages a ON h.city = a.city
  GROUP BY h.city, a.avg_burden, a.avg_rent, a.avg_home_value
)
SELECT * FROM city_volatility
ORDER BY GREATEST(delinquency_volatility, rent_cv_pct, price_cv_pct) DESC;

-- QUERY 5: EDUCATION SHELTER EFFECT
-- WHAT: Education vs burden/volatility ranking 
-- WHY: Shows full spectrum - which cities are education-buffered vs struggling
-- HOW: Self-JOIN → GROUP BY → RANK() all cities by education-burden gap
------------------------------------------------------------
SELECT 
  h.city,
  AVG(h.pct_bachelors_plus) AS avg_education_pct,
  AVG(h.housing_cost_burden_pct) AS avg_burden_pct,
  AVG(c.cpi_index) AS avg_cpi,
  STDDEV(h.housing_cost_burden_pct) AS burden_volatility,
  AVG(h.pct_bachelors_plus) - AVG(h.housing_cost_burden_pct) AS education_burden_gap,
  RANK() OVER (ORDER BY AVG(h.pct_bachelors_plus) - AVG(h.housing_cost_burden_pct) DESC) AS shelter_rank
FROM "HOUSING_COMBINED" h 
JOIN "HOUSING_COMBINED" c ON h.city = c.city AND h.year = c.year
GROUP BY h.city
ORDER BY shelter_rank;

-- QUERY 6: UNEMPLOYMENT SENSITIVITY
-- WHAT: Correlation between unemployment rate and housing burden by city + grand total
-- WHY: Quantifies labor market impact on affordability (policy priority)
-- HOW: Unemployment % calculation → CORR() → GROUP BY ROLLUP(city)
------------------------------------------------------------
SELECT city,
  AVG(unemployment_count / NULLIF(total_population, 0) * 100) AS avg_unemployment_pct,
  AVG(housing_cost_burden_pct) AS avg_burden,
  CORR(unemployment_count / NULLIF(total_population, 0), housing_cost_burden_pct) AS unemp_burden_corr
FROM "HOUSING_COMBINED"
GROUP BY ROLLUP(city)
HAVING city IS NOT NULL
ORDER BY unemp_burden_corr DESC;

-- QUERY 7: MACRO PRESSURE RESILIENCE
-- WHAT: Cities maintaining low burden despite high CPI + interest rates
-- WHY: Tests resilience to macro shocks
-- HOW: Average burden % when CPI > 250 or % when interest rates > 4%
------------------------------------------------------------
SELECT city,
  ROUND(AVG(housing_cost_burden_pct)*100, 1) AS normal_burden_pct,
  ROUND(AVG(CASE WHEN cpi_index > 250 OR interest_rates > 4.0 THEN housing_cost_burden_pct END)*100, 1) AS stress_burden_pct,
  ROUND(AVG(CASE WHEN cpi_index > 250 OR interest_rates > 4.0 THEN housing_cost_burden_pct END) - AVG(housing_cost_burden_pct), 4)*100 AS delta_pct
FROM "HOUSING_COMBINED"
GROUP BY city
ORDER BY delta_pct ASC;

-- QUERY 8: INCOME-TO-RENT AFFORDABILITY (GROUP BY)
-- WHAT: Median income ÷ median rent ratio (higher = more affordable)
-- WHY: Standard affordability metric for renters (income covers how many months of rent?)
-- HOW: Simple ratio → GROUP BY city → descending order
------------------------------------------------------------
SELECT city,
  ROUND(AVG(median_household_income / NULLIF(median_gross_rent, 0)), 0) AS months_income_covers_rent
FROM "HOUSING_COMBINED"
GROUP BY city
ORDER BY months_income_covers_rent DESC;

-- QUERY 9: RENT VS HOME PRICE AFFORDABILITY (JOIN)
-- WHAT: Rent-to-price ratio by city (buy vs rent decision)
-- WHY: Core investor decision metric
-- HOW: Self-join on city/year for ratio calculation
------------------------------------------------------------
SELECT 
  h1.city,
  AVG(h1.median_gross_rent / NULLIF(h2.median_home_value, 0) * 12 * 100) AS rent_to_price_pct
FROM "HOUSING_COMBINED" h1
JOIN "HOUSING_COMBINED" h2 ON h1.city = h2.city AND h1.year = h2.year
WHERE h1.year > 2020
GROUP BY h1.city
ORDER BY rent_to_price_pct DESC;

-- QUERY 10: EDUCATION AFFORDABILITY RANKING (Subquery + RANK Window)
-- WHAT: All cities ranked by education-adjusted affordability score
-- WHY: Reveals if higher education = better housing outcomes across ALL cities
-- HOW: AVG(pct_bachelors_plus) * AVG(median_household_income / median_gross_rent)
------------------------------------------------------------
SELECT 
  city,
  ROUND(AVG(pct_bachelors_plus), 4) AS avg_bachelors_pct,
  ROUND(AVG(median_household_income / NULLIF(median_gross_rent, 0)), 4) AS months_income_covers_rent,
  ROUND(AVG(pct_bachelors_plus) * AVG(median_household_income / NULLIF(median_gross_rent, 0)), 4) AS education_affordability_score,
  RANK() OVER (ORDER BY AVG(pct_bachelors_plus) * AVG(median_household_income / NULLIF(median_gross_rent, 0)) DESC) AS affordability_rank,
  CASE 
    WHEN AVG(pct_bachelors_plus) > (SELECT AVG(pct_bachelors_plus) FROM "HOUSING_COMBINED")
    THEN 'ABOVE_AVG_EDUCATION'
    ELSE 'BELOW_AVG_EDUCATION'
  END AS education_category
FROM "HOUSING_COMBINED"
GROUP BY city
ORDER BY affordability_rank;

-- QUERY 11: RECENT vs HISTORIC RENT COMPARISON (SIMPLE JOIN)
-- WHAT: Current rent (2024) vs 3-year ago rent (2021) by city
-- WHY: Quick rent acceleration snapshot for investors
-- HOW: Self-join on city matching 2024 vs 2021 data
------------------------------------------------------------
SELECT 
  h_recent.city,
  h_recent.median_gross_rent AS rent_2024,
  h_historic.median_gross_rent AS rent_2021,
  ROUND(((h_recent.median_gross_rent / h_historic.median_gross_rent) - 1) * 100, 1) AS rent_growth_3yr_pct
FROM "HOUSING_COMBINED" h_recent
JOIN "HOUSING_COMBINED" h_historic 
  ON h_recent.city = h_historic.city 
  AND h_recent.year = 2024
  AND h_historic.year = 2021
ORDER BY rent_growth_3yr_pct DESC;

-- QUERY 12: FASTEST GROWING RENT MARKETS (vs Average Growth)
-- WHAT: Cities with rent growth above overall average growth rate
-- WHY: Identifies rental market outperformers for investors  
-- HOW: CTE calculates growth first → subquery gets baseline average
------------------------------------------------------------
WITH rent_growth AS (
  SELECT city, year, median_gross_rent,
    median_gross_rent / NULLIF(LAG(median_gross_rent) OVER (PARTITION BY city ORDER BY year), 0) - 1 
      AS annual_rent_growth
  FROM "HOUSING_COMBINED"
  WHERE year >= 2016
)
SELECT city,
  ROUND(AVG(annual_rent_growth)*100, 1) AS avg_annual_rent_growth_pct,
  ROUND((SELECT AVG(annual_rent_growth)*100 FROM rent_growth), 1) AS overall_avg_growth_pct
FROM rent_growth
WHERE annual_rent_growth IS NOT NULL
GROUP BY city
HAVING AVG(annual_rent_growth) > (
    SELECT AVG(annual_rent_growth)
    FROM rent_growth
)
ORDER BY avg_annual_rent_growth_pct DESC;
