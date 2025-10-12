
https://console.cloud.google.com/bigquery?sq=444301413236:65b0cbaac45744f5aa586891fc5f0d17

--eCommerce Funnel Analysis in BigQuery and GA
--Author: Prasolova Svitlana
--Description: This project explores user behavior and sales funnel performance for an eCommerce platform, based on raw event data from Google Analytics 4 (GA4).
-- The analysis was conducted in BigQuery to extract, transform, and interpret data on events, users, and sessions.
-- Tools & Technologies: BigQuery | Google Analytics 4 (GA4)

--1. To generate data with information about events, users and sessions in GA4 in a table, necessary for building reports in BI systems according to the specified parameters:
-- I am writing a query in BigQuery that outputs data with information about unique events (event_name) using the DISTINCT operator in GA4:

SELECT  DISTINCT event_name
FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`bpd
WHERE _TABLE_SUFFIX BETWEEN '20210101' AND '20211231' 
LIMIT 1000;

--I write the final query to generate a table with data according to the task:
SELECT  
      TIMESTAMP_MICROS(event_timestamp)AS event_timestamp
      , user_pseudo_id 
      , (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS session_id
      , event_name
      , geo.country AS country
      , device.category AS device_category
      , traffic_source.source AS source
      , traffic_source.medium AS medium
      , traffic_source.name AS campaign
FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`bpd
WHERE _TABLE_SUFFIX BETWEEN '20210101' AND '20211231'
      AND event_name IN (
        'session_start', 'view_item', 'add_to_cart', 'begin_checkout',
        'add_shipping_info', 'add_payment_info', 'purchase'
      ) 
LIMIT 1000;

--2. Create a query to get a table with information about conversions from the beginning of the session to the purchase. 
-- I am writing a query in BigQuery that creates CTE (Common Table Expression):
Extracts event-level data (event_name, event_timestamp, traffic_source.*) and builds a unique user_session_id by combining user_pseudo_id with the GA4 session identifier (ga_session_id) using the UNNEST function: 

WITH CTE AS (
  SELECT  
    DATE(TIMESTAMP_MICROS(event_timestamp)) AS event_date,
    event_name,
    traffic_source.source AS source,
    traffic_source.medium AS medium,
    traffic_source.name AS campaign,
    CONCAT(user_pseudo_id,
      (SELECT value.int_value 
         FROM UNNEST(event_params) 
         WHERE key = 'ga_session_id')
    ) AS user_session_id
  FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*` bpd
  WHERE event_name IN ('session_start', 'add_to_cart', 'begin_checkout', 'purchase')
), 
-- I am writing a query in BigQuery that creates Event Aggregation: groups data by date, traffic source, medium, and campaign, and counts unique sessions for each event type (session_start, add_to_cart, begin_checkout, purchase):

count_by_categories AS (
  SELECT 
    event_date,source, medium, campaign, 
    COUNT(DISTINCT CASE WHEN event_name = 'session_start' THEN user_session_id END) AS session_start_count,
    COUNT(DISTINCT CASE WHEN event_name = 'add_to_cart' THEN user_session_id END) AS add_to_cart_count,
    COUNT(DISTINCT CASE WHEN event_name = 'begin_checkout' THEN user_session_id END) AS begin_checkout_count,
    COUNT(DISTINCT CASE WHEN event_name = 'purchase' THEN user_session_id END) AS purchase_count
  FROM CTE
  GROUP BY event_date, source, medium, campaign)
---- I am writing a query in BigQuery that  Conversion Rate Calculation: calculates conversion percentages for each funnel step: visit to Cart, visit to Checkout, Visit to Purchase; also using conditional logic (IF) to prevent division by zero and rounding results to two decimal places; also realizing Final Filtering: returns only sessions with valid starts and limits output to 50 rows for preview:
SELECT 
   event_date, source, medium, campaign,
  session_start_count, add_to_cart_count,
  begin_checkout_count, purchase_count,
  ROUND(100 * IF(session_start_count > 0, add_to_cart_count / session_start_count, 0), 2) AS visit_to_cart,
  ROUND(100 * IF(session_start_count > 0, begin_checkout_count / session_start_count, 0), 2) AS visit_to_checkout,
  ROUND(100 * IF(session_start_count > 0, purchase_count / session_start_count, 0), 2) AS visit_to_purchase
FROM count_by_categories 
WHERE session_start_count > 0
LIMIT 50;

--3. Comparison of conversion between different landing pages based on determining which pages of an eCommerce site bring in the most purchases in order to optimize navigation and user paths. To do this:
--WITH start_session AS(-- I write the CTE start_session query, which generates a basic dataset about users' start pages: selects all session start events (session_start) for 2020, creates a unique session identifier user_session_id by combining user_pseudo_id and ga_session_id, and extracts the URL of the page from which the session started (page_location):

WITH start_session AS(
SELECT 
    DISTINCT CONCAT(user_pseudo_id,(SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id')
         ) AS user_session_id
      , event_name
      , user_pseudo_id
      , event_params
      , (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location') AS page_location  
FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`bpd
WHERE event_name = 'session_start'AND _TABLE_SUFFIX BETWEEN '20200101' AND '20201231'),
---- I write the CTE query cleaned_sessions that cleans the URLs by removing the domain and query parameters (?) to leave only the page path (page_path), and also prepares a clean set of sessions with clearly defined pages that users visited at the beginning:  

 cleaned_sessions AS (
  SELECT
    user_session_id,
    user_pseudo_id,
    page_location, 
    SPLIT(REPLACE(page_location, 'https://shop.googlemerchandisestore.com/', ''), '?')[OFFSET(0)] 
          AS page_path
  FROM start_session
  WHERE page_location IS NOT NULL),
---- I write the CTE purchases query that generates a list of all sessions in which a purchase occurred for the same period: 
 
purchases AS (
  SELECT DISTINCT
    CONCAT(user_pseudo_id,(SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id')
         ) AS user_session_id
  FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
  WHERE event_name = 'purchase'
    AND _TABLE_SUFFIX BETWEEN '20200101' AND '20201231'),
--I write an intermediate CTE joined query that connects the initial sessions with purchases and for each session adds the purchase_flag = 1 flag if there was a purchase in this session:   
 joined AS (
  SELECT
    cs.page_path,
    cs.user_session_id,
    IF(p.user_session_id IS NOT NULL, 1, 0) AS purchase_flag
  FROM cleaned_sessions cs
  LEFT JOIN purchases p
    ON cs.user_session_id = p.user_session_id)
--I write a final query that counts the number of unique sessions and sessions with purchases for each page path, calculates the conversion from visit to purchase (%) and sorts the pages by the highest conversion and displays the top 100 pages:

SELECT
  page_path,
  COUNT(DISTINCT user_session_id) AS unique_sessions,
  COUNT(DISTINCT CASE WHEN purchase_flag = 1 THEN user_session_id END) AS purchase_sessions,
  ROUND(100 * COUNT(DISTINCT CASE WHEN purchase_flag = 1 THEN user_session_id END) / 
              COUNT(DISTINCT user_session_id), 2) AS visit_to_purchase
FROM joined
GROUP BY page_path
ORDER BY visit_to_purchase DESC
LIMIT 100;

-- Technical implementation:
-- Four consecutive CTEs were used for sequential processing.
-- String functions (REPLACE, SPLIT) were used to clean up the URL.
-- Unique session_ids were created with a nested subquery in UNNEST.
-- LEFT JOIN + CASE WHEN + aggregation were used to count conversions.

--4. Investigating the correlation between user engagement and purchase in Google Analytics 4 data (from a public eCommerce sample in BigQuery). The goal is to quantify how user activity in a session is related to the likelihood of purchase. To do this:
-- I write an initial WITH query to generate a base table of all events, all_events, that contain engagement metrics. Specifically, this query generates a unique session identifier (user_session_id) by concatenating user_pseudo_id and ga_session_id, and also extracts from the nested event parameters (event_params): engagement_time_msec → user activity time in milliseconds; session_engaged → session engagement indicator (0 or 1):

WITH all_events AS (
  SELECT 
    user_pseudo_id || CAST((SELECT value.int_value FROM UNNEST(event_params) 
                      WHERE key = 'ga_session_id') AS STRING) AS user_session_id,
    event_name,
    CAST((SELECT value.int_value FROM UNNEST(event_params) 
          WHERE key = 'engagement_time_msec') AS INT64) AS engagement_time,
    CAST((SELECT value.string_value FROM UNNEST(event_params) 
          WHERE key = 'session_engaged') AS INT64) AS session_engaged
  FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
),
--I write a CTE engagement_metrics query that groups events by session and calculates two key engagement metrics: is_engaged = 1 if there was at least one engagement in the session; total_engagement_time = total user activity time in the session:

engagement_metrics AS (
  SELECT
    user_session_id,
    MAX(IF(session_engaged != 0, 1, 0)) AS is_engaged,
    COALESCE(SUM(engagement_time), 0) AS total_engagement_time
  FROM all_events
  GROUP BY user_session_id
),
--I am writing a CTE purchases query that labels sessions with purchases based on which sessions a purchase event occurred and sets the is_purchase = 1 flag:

purchases AS (
  SELECT DISTINCT
    user_session_id,
    1 AS is_purchase
  FROM all_events
  WHERE event_name = 'purchase'
),
---- I write a CTE query sessions_combined, which prepares a dataset for statistical analysis based on combining data from all sessions with purchase data. Each session now has: is_engaged — 0 or 1; total_engagement_time — a numeric value; is_purchase — 0 or 1:

sessions_combined AS (
  SELECT 
    e.user_session_id,
    e.is_engaged,
    e.total_engagement_time,
    IF(p.is_purchase IS NOT NULL, 1, 0) AS is_purchase
  FROM engagement_metrics e
  LEFT JOIN purchases p
    ON e.user_session_id = p.user_session_id
)
---- I am writing a final query to calculate correlation coefficients, which uses the built-in CORR() function in BigQuery to calculate the Pearson coefficient between: is_engaged and is_purchase → whether sessions with engaged users are more likely to result in a purchase, and total_engagement_time and is_purchase → whether more activity time increases the likelihood of a purchase.

SELECT
  CORR(CAST(is_engaged AS FLOAT64), CAST(is_purchase AS FLOAT64)) AS corr_engaged_purchase,
  CORR(CAST(total_engagement_time AS FLOAT64), CAST(is_purchase AS FLOAT64)) AS corr_time_purchase
FROM sessions_combined;

-- Technical techniques (Hard Skills): 
-- Creating unique session_id from nested parameters via UNNEST.
-- Aggregating session data (MAX, SUM, GROUP BY).
-- Joining tables via LEFT JOIN.
-- Using the built-in CORR() correlation function for statistical analysis.
-- Optimizing CTE for multi-step analysis (clear logical structure).
