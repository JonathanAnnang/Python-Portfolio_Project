-- 1. Demographic Distribution by Country
-- Goal: Understand sample size across different regions.
SELECT 
    country, 
    gender, 
    COUNT(respondent_id) AS total_responses,
    AVG(age) AS average_age
FROM survey_responses
GROUP BY country, gender
ORDER BY country;

-- 2. Income Analysis by Age Group
-- Goal: Identify economic trends within specific demographics.
SELECT 
    age_group, 
    income_band, 
    COUNT(*) AS frequency,
    ROUND(AVG(income), 2) AS avg_income
FROM survey_responses
GROUP BY age_group, income_band
ORDER BY age_group;

-- 3. Data Integrity Audit Query
-- Goal: Meet the KPI of "Zero major data integrity failures."
-- This identifies if any respondent has a score outside the 1-5 range.
SELECT * FROM survey_responses 
WHERE response_score < 1 OR response_score > 5;

-- 4. High-Value Insights (Top Earners)
-- Goal: Support targeted research for high-income segments.
SELECT 
    respondent_id, 
    country, 
    income
FROM survey_responses
WHERE income_band = 'High'
ORDER BY income DESC;