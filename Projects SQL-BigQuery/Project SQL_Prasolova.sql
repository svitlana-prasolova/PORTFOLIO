--Online Advertising Campaigns SQL Analysis
--  Author: Prasolova Svitlana
    
--  Description
--  This project focuses on analyzing detailed performance data from Google Ads and Facebook Ads using SQL queries in DBeaver.
--The goal was to explore aggregated marketing metrics, identify the most effective campaigns and periods, and compare ad performance across platforms. The project demonstrates advanced SQL querying skills, multi-table joins, aggregations, and time-based analysis.
    
--Technical justification (step by step)
-- Based on the sales funnel created using the initial "with" operator, queries were executed to perform the following tasks and obtain key conclusions:
--1. To find aggregate daily spending indicators for online companies Google and Facebook, the data for which is located in different tables, I write a subquery to form a virtual CTE (Common Table Expressions) table called facebook_google_ads, and also use the UNION ALL command to combine tables by the same fields:
  
WITH facebook_google_ads AS (
SELECT
  'Facebook' AS media_source
    , fb.ad_date
    , fb.spend
FROM public.facebook_ads_basic_daily fb
UNION ALL
SELECT
   'Google' AS media_source
    , ad_date
    , spend
FROM public.google_ads_basic_daily gb)
--Next, I apply SQL aggregate functions (AVG, MIN, MAX) with mandatory grouping (GROUP BY) and applying ROUND to the AVG function (reduces the number of decimal places) and using the logical operator (IS NULL) to exclude empty rows, and form a query:
SELECT
  ad_date
   , media_source
   , ROUND(AVG(spend), 2)  AS avg_spend
   , MIN(spend)            AS min_spend
   , MAX(spend)            AS max_spend
FROM facebook_google_ads
WHERE ad_date IS NOT NULL 
GROUP BY 1, 2
ORDER BY 5 DESC;

-- By supplementing this query with the ORDER BY command, I can sort the rows:

--2. To determine the top 5 days by ROMI level, I use a subquery to form the virtual table facebook_google_ads, as well as the UNION ALL command to combine tables by the same fields:

WITH facebook_google_ads AS (
SELECT 
  'Facebook' AS media_source
    , fb.ad_date
    , fb.spend
    , fb.value 
FROM public.facebook_ads_basic_daily fb 
UNION ALL
SELECT 
   'Google' AS media_source
    , ad_date
    , spend
    , value 
FROM public.google_ads_basic_daily gb
)
-- Next, I apply the main metric of advertising strategies - ROMI (Return on Marketing Investment), which evaluates the effectiveness of marketing investments (the difference between the cost of marketing expenses and the income received and dividing this result by the cost of marketing expenses with mandatory grouping (GROUP BY), using the data type conversion operator (::numeric) and ROUND (reduces the number of decimal places), as well as using the logical operator (IS NULL) to exclude empty rows. I form a query that is sorted by ROMI indicator (from the highest value to the lowest - DESC) and limited to 5 days (LIMIT 5):
SELECT
  ad_date
   , media_source
   , ROUND(SUM(value)::numeric / SUM(spend) - 1, 2) AS ROMI
FROM facebook_google_ads
WHERE ad_date IS NOT NULL AND spend > 0
GROUP BY 1, 2
ORDER BY ROMI DESC 
LIMIT 5;

--3. To determine the leading company with the highest total weekly value, I use a subquery to form a virtual table facebook_google_ads together with the UNION ALL command to join tables by the same fields, and I also supplement it with a query to join data from multiple tables using the LEFT JOIN command to get company names by Facebook:

WITH facebook_google_ads AS (
SELECT 
   fb.ad_date
    , fc.campaign_name
    , fb.value 
FROM public.facebook_ads_basic_daily fb 
LEFT JOIN public.facebook_campaign fc ON fb.campaign_id = fc.campaign_id
UNION ALL
SELECT 
    ad_date
    , campaign_name
    , value 
FROM public.google_ads_basic_daily gb)
-- Next, I use the date_trunc function to truncate the date in the ad_date field to the week level together with the data type cast operator (::date) and sum the value of funds received by advertising companies (SUM(value) with mandatory grouping (GROUP BY) and using the logical operator (IS NULL) to exclude empty rows. I form a query that is sorted by indicator (SUM(value) - from the highest value to the lowest - DESC) and limited to 1 company (LIMIT 1):
SELECT
  date_trunc('week', ad_date)::date AS ad_week 
   , campaign_name
   , SUM(value) AS total_value
FROM facebook_google_ads
WHERE campaign_name IS NOT NULL 
GROUP BY 1, 2
ORDER BY 3 DESC
LIMIT 1;

-- 4. To determine the leading company that had the largest month-over-month growth in reach, I use a subquery to form a virtual CTE table and a UNION ALL command to join tables on the same fields, supplemented by a query to join data from multiple tables using a LEFT JOIN command to retrieve company names on Facebook:
    WITH CTE AS (
    SELECT 
        fb.ad_date
        , fc.campaign_name
        , COALESCE(reach, 0) AS reach
    FROM public.facebook_ads_basic_daily fb 
    LEFT JOIN public.facebook_campaign fc ON fb.campaign_id = fc.campaign_id
    UNION ALL
    SELECT 
      gb.ad_date
      , gb.campaign_name
      , COALESCE(reach, 0) AS reach
    FROM public.google_ads_basic_daily gb
),

-- Next, I write a subquery to form the nested table monthly_data and use the date_trunc function to truncate the date in the ad_date field to the month level together with the data type cast operator (::date) and estimate the reach by the SUM indicator (reach - the reach of the advertisement by the number of unique users who saw it) with mandatory grouping (GROUP BY) and using the logical operator (IS NULL) to exclude empty rows:
monthly_data AS (
    SELECT
      campaign_name
      , date_trunc('month', ad_date)::date AS ad_month
      , SUM(reach) AS total_reach
    FROM CTE
    WHERE campaign_name IS NOT NULL
    GROUP BY 1, 2
),
 -- The next step is to write a subquery lagged_date containing a window function - LAG (expression [, offset [, default]]), which returns the value of the expression preceding the current row in the window (the value of the previous month - prev_reach):
lagged_date AS (
    SELECT 
      *
      , LAG(total_reach) OVER (PARTITION BY campaign_name ORDER BY ad_month) AS prev_reach
    FROM monthly_data
),
 -- Next, I write a subquery growth_data, which calculates the month-to-month growth in reach using the CASE operator and a condition to exclude null values ??(WHEN prev_reach IS NOT NULL THEN):
growth_data AS (
    SELECT
      ad_month
      , campaign_name
      , total_reach
      , prev_reach
      , CASE WHEN prev_reach IS NOT NULL THEN total_reach - prev_reach ELSE NULL 
        END AS abs_growth_reach
    FROM lagged_date
)
-- and I end with the resulting query that includes all defined fields and sorts the month-to-month coverage growth values ??from highest to lowest - ORDER BY, DESC) and is limited to 1 company (LIMIT 1):
      
SELECT 
   ad_month
    , campaign_name
    , total_reach
    , prev_reach
    , abs_growth_reach
FROM growth_data
WHERE abs_growth_reach IS NOT NULL
ORDER BY abs_growth_reach DESC
LIMIT 1;

--5. To determine the duration of the longest continuous (daily) display of a product advertisement by adset_name, I use a subquery to form a virtual table facebook_google_ads together with the UNION ALL command to combine tables by the same fields, and I also supplement it with a query to combine data from multiple tables using the LEFT JOIN command to obtain the adset_name of companies on Facebook:

WITH facebook_google_ads AS (
    SELECT 
        fb.ad_date
        , fa.adset_name
    FROM public.facebook_ads_basic_daily fb 
    LEFT JOIN public.facebook_adset fa ON fb.adset_id = fa.adset_id
        UNION ALL
    SELECT 
        ad_date
        , adset_name
    FROM public.google_ads_basic_daily gb
),
--I write a subquery with a nested table distinct_ads, in which I filter companies by date order, assigning each company a row number using DISTINCT:
    
 distinct_ads AS (
    SELECT 
      DISTINCT adset_name, ad_date
    FROM facebook_google_ads
),
--I write a subquery with a nested table diff_date, in which I use a window function to subtract ROW_NUMBER() from the date to get a constant "continuity group":
  diff_date AS (
    SELECT 
      adset_name
      , ad_date
      , ad_date - INTERVAL '1 day' * ROW_NUMBER() OVER (PARTITION BY adset_name ORDER BY ad_date) AS grp_date
    FROM distinct_ads
),
--I write a subquery with a nested table grouped_intervals, in which I group days in a sequence (series - max and min) without gaps and count their duration (COUNT(*):
    
  grouped_intervals AS (
    SELECT 
      adset_name
      , MIN(ad_date) AS start_date
      , MAX(ad_date) AS end_date
      , COUNT(*) AS duration_date
    FROM diff_date 
    GROUP BY adset_name, grp_date
),
-- I write a subquery with a nested table called longest_streak, in which I determine the longest continuous ad run using a window function (RANK()):  
longest_streak AS (
    SELECT *, RANK() OVER (ORDER BY duration_date DESC) AS rnk_date
    FROM grouped_intervals
)
--I write a final query that returns the campaign name, duration, start and end date of the longest continuous series of impressions:
SELECT 
  adset_name
  , duration_date
  , start_date
  , end_date
FROM longest_streak
WHERE rnk_date = 1;

--Answer: The "Narrow" campaign lasted 108 days continuously in both media - facebook_ads, google_ads from 05-17-2021 to 09-01-2021


