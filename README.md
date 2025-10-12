# Svitlana Prasolova

## 📍 About
Hi, I'm Svitlana. I am a scientist, and now my area of interest is data analytics. As a Data analyst with practical experience in data analysis, product analytics, finance, and statistical research, I am skilled in SQL, Python (Pandas, NumPy), Google Sheets, Looker Studio, Tableau, Power BI, data visualization, A/B testing. 
Completed analysis of advertising campaigns and product performance, identifying key factors affecting user engagement and conversion rates. Seeking a role to apply analytical skills and support data-driven business decisions. I am skilled in most data-science steps: data pre-processing, data analysis, data visualization, and results communication.
Recently completed the  Data Analytics Certificate - a rigorous, hands-on program that covers the entire scope of the data analysis process.

On this page, you can find some examples of my projects. Have fun in browsing through the content, and thank you for visiting!

---

## [Python Project 1](https://github.com/svitlana-prasolova/PORTFOLIO/blob/a136b624668a5e16eb1f8599c071640ea2f996a3/Projects%20Python/Project_1.Python.ipynb)

# StackOverflow Developer Survey Analysis
Data analysis of the latest StackOverflow Developer Survey using Python & Pandas. Explored trends in developers’ experience, work format, education, and compensation. Includes insights on Python usage, remote work, and global salary patterns

## Project Tasks
- Load and explore the latest StackOverflow Developer Survey dataset.
- Clean and prepare data using Pandas.
- Analyze developers’ profiles: experience, work format, education, and programming languages.
- Evaluate Python usage, remote work, and compensation trends by country.

## Results:
- Performed statistical and exploratory data analysis on survey results.
- Identified trends by experience, country, compensation, and programming languages.
- Provided insights into socio-economic patterns among Python developers.

--

## [Python Project 2] (https://github.com/svitlana-prasolova/PORTFOLIO/blob/a136b624668a5e16eb1f8599c071640ea2f996a3/Projects%20Python/Project_2%20.%20Python.ipynb)

#  Applications Rating Analysis
**Data cleaning, merging, and applicant rating calculation using Python and Pandas**
This project focuses on analyzing application data from two CSV files — applications.csv and industries.csv.
The goal is to clean and enrich the data, calculate individual application ratings, and determine the average weekly rating of accepted applications.

## Project Task:
Develop a Python-based data analysis workflow to calculate and visualize application ratings.
- The project involves cleaning and merging datasets, 
- applying rating rules to each applicant,
- filtering accepted applications,
- and analyzing average weekly ratings

## Results
- Cleaned and enriched applicant data.
- Automated the rating calculation logic based on multiple business rules.
- Calculated average weekly ratings for accepted applications.

## Key Skills Demonstrated
- Data cleaning & preprocessing
- Conditional calculations using Pandas
- Merging datasets
- Weekly grouping & aggregation
- Data visualization

### ## 🔹 Tools & Libraries
- **Python 3.x**  
- **Pandas, NumPy**  
- **Matplotlib / Seaborn**  


## [Python Project 3](https://github.com/svitlana-prasolova/PORTFOLIO/blob/e7a548a5793571f5a4ed4bbc9d9d7ace72da9a36/Projects%20Python/Project_3.%20Python.ipynb)
# Facebook Ads Time-series & ROMI Analysis
### Author: Svitlana Prasolova

## Project Description
Analysis of Facebook advertising performance using a Jupyter Notebook and Python (Pandas, NumPy, Matplotlib/Seaborn). The notebook loads facebook_ads_data.csv, performs daily and campaign-level aggregations, computes ROMI, visualizes trends and distributions, inspects correlations, and fits a regression between ad spend and value.

## Project Task:
Through the use of Pandas, NumPy, Matplotlib, and Seaborn, the project demonstrates how to:
- Analyze daily advertising spend and ROMI across 2021;
- Apply moving averages to smooth time-series data;
- Compare campaign-level efficiency via bar charts and box plots;
- Visualize ROMI distribution and correlation between performance metrics;
- Build a regression model to explore the relationship between total_spend and total_value.
The outcome is a complete analytical notebook that supports data-driven insights into Facebook Ads effectiveness and campaign optimization.

## Results
The analysis provided a clear view of advertising effectiveness:
- Identified campaigns with the highest ROMI.
- Visualized daily trends and seasonal patterns in advertising spend.
- Identified strong correlations between ad spend, impressions, clicks, and total value.

A complete analytical notebook that delivers data-driven insights into Facebook Ads campaign performance, helping to optimize marketing budgets and improve ROMI.

## Key Skills Demonstrated
- Data cleaning and preparation with Pandas.
- Daily and campaign-level performance analysis.
- ROMI calculation and time-series visualization.
- Correlation and regression analysis (lmplot).
- Data storytelling with Seaborn and Matplotlib.

## Tools & Technologies
- Language: Python
- Environment: Jupyter Notebook
- Libraries: Pandas, NumPy, Matplotlib, Seaborn


## [Project SQL Analysis](b)

# Online Advertising Campaigns SQL Analysis
### Author: Svitlana Prasolova

## Project Description
This project focuses on analyzing detailed performance data from Google Ads and Facebook Ads using SQL queries in DBeaver. The goal was to explore aggregated marketing metrics, identify the most effective campaigns and periods, and compare ad performance across platforms. The project demonstrates advanced SQL querying skills, multi-table joins, aggregations, and time-based analysis.

## Project Task:
•	Facebook Ads campaigns were more expensive but significantly more effective than Google Ads.
•	The Expansion campaign achieved the highest conversion rates.
•	The Hobbies campaign demonstrated the greatest increase in reach.
•	The Narrow campaign had the longest duration but did not show strong performance.

## Results
•	Produced clear and verified SQL outputs for each analytical question.
•	Identified performance peaks by ROMI and value across time and campaigns.
•	Demonstrated ability to apply aggregations, grouping, date functions, and conditional logic in SQL.
•	Delivered query results in separate files — one per task, ensuring reproducibility and clarity.

## Conclusion:
SQL queries from the project provided aggregate insights into marketing spend and ROMI dynamics. Key findings include:
•	Facebook Ads campaigns were more expensive but significantly more effective than Google Ads.
•	The Expansion campaign achieved the highest conversion rates.
•	The Hobbies campaign demonstrated the greatest increase in reach.
•	The Narrow campaign had the longest duration but did not show strong performance.
📈 Recommendation:
To maximize advertising efficiency and sales growth, it is advisable to increase Facebook Ads investment, especially in the Expansion and Hobbies campaigns. However, campaigns should not be overly extended, as longer duration does not correlate with better performance (as seen with the Narrow campaign).

## Key Skills Demonstrated
•	SQL (PostgreSQL): aggregations, joins, subqueries, CTEs, and date functions.
•	Data analysis in DBeaver.
•	Multi-source data integration (Google & Facebook Ads).
•	Performance metrics calculation (spend, ROMI, reach, value).
•	Analytical reporting and structured query organization.

## Tools & Technologies
- •	Environment: DBeaver
•	Language: SQL (PostgreSQL dialect)
•	Data sources: 
o	facebook_ads_basic_daily
o	google_ads_basic_daily
o	facebook_adset
o	facebook_campaign

## [Project BigQuery Analysis](b)

# eCommerce Funnel Analysis in BigQuery and GA4
### Author: Svitlana Prasolova

## Project Description
This project explores user behavior and sales funnel performance for an eCommerce platform, based on raw event data from Google Analytics 4 (GA4). The analysis was conducted in BigQuery to extract, transform, and interpret data on events, users, and sessions.

## Project Task:
•	Creating queries to retrieve event-level data  with time formatting via TIMESTAMP_MICROS and filtering by _TABLE_SUFFIX for the year 2021.
•	Using DISTINCT and UNNEST operators to flatten the nested structure of the event_params field (type RECORD) — converting it into a relational format for further processing.
•	Selecting only the defined funnel events and bilding a multi-step conversion funnel from session start to purchase, segmented by date, traffic source, medium, and campaign using subqueries and CTEs.
•	Applying filters (IN, IF) to include relevant events and prevent division by zero during conversion rate calculations.
•	Merging user engagement and purchase data with LEFT JOIN to identify the correlation between engagement time and purchase completion.

## Conclusion:
The executed SQL scripts successfully generated clean, structured outputs, which were later used for visualization and BI reporting.
Main findings:
•	Only 1 in 6 visitors who started a session completed a purchase — indicating potential for funnel optimization at registration and checkout stages.
•	Facebook Ads channels showed higher engagement rates but required cost efficiency improvements.
•	Redesign/Accessories and Redesign/Apparel campaigns demonstrated 100% conversion from session start to purchase — likely targeting a niche VIP audience.
•	Correlation analysis revealed an almost zero correlation between the engagement flag (is_engaged) and purchase, but a stronger correlation between total engagement time and purchase activity..
📈 Recommendation:
To improve overall funnel performance and boost sales, focus on simplifying user registration and checkout flow, while maintaining attention on campaigns that demonstrate strong conversion consistency (e.g., Redesign/Accessories).

## Results
•	Generated accurate and reproducible SQL outputs addressing all analytical objectives.
•	Extracted and analyzed GA4 data to build a complete eCommerce conversion funnel.
•	Identified top-performing campaigns and key traffic sources driving conversions.
•	Revealed insights on user engagement, purchase behavior, and campaign efficiency

## Key Skills Demonstrated
•   SQL (BigQuery): CTEs, JOINs, aggregations, and data transformation.
•	GA4 data extraction and event modeling.
•	Conversion funnel and traffic source analysis.
•	Correlation and engagement metrics evaluation.
•	Analytical reporting and data interpretation for eCommerce insights.

## Tools & Technologies
- BigQuery | Google Analytics 4 (GA4)