-- EDA - Exploratory Data Analysis: World Layoff Data
-- This script explores the data to find trends, patterns, and outliers.

-- Step 1: Inspect the staging table
CREATE TABLE layoffs_staging2 AS
SELECT * FROM layoffs_staging;

SELECT * 
FROM layoffs_staging2;

describe layoffs_staging2;

-- Example for converting from 'MM/DD/YYYY' format to 'YYYY-MM-DD'
UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y')
WHERE `date` LIKE '%/%/%';  -- Ensures only dates in MM/DD/YYYY format are converted


-- Step 2: Basic Queries to Explore the Data

-- 2.1: Maximum number of layoffs in a single event
SELECT MAX(total_laid_off) AS max_laid_off
FROM layoffs_staging2;

-- 2.2: Minimum and Maximum percentage of layoffs
SELECT MAX(percentage_laid_off) AS max_percentage, MIN(percentage_laid_off) AS min_percentage
FROM layoffs_staging2
WHERE percentage_laid_off IS NOT NULL;

-- 2.3: Companies with 100% layoffs
SELECT *
FROM layoffs_staging2
WHERE percentage_laid_off = 1;
-- Insight: These are mostly startups that went out of business during this time.

-- 2.4: Companies with 100% layoffs ordered by funds raised
SELECT *
FROM layoffs_staging2
WHERE percentage_laid_off = 1
ORDER BY funds_raised_millions DESC;
-- Insight: BritishVolt (EV company) and Quibi (raised around 2 billion dollars).

-- Step 3: Intermediate Queries using GROUP BY

-- 3.1: Companies with the biggest single layoff event
SELECT company, total_laid_off
FROM layoffs_staging2
ORDER BY total_laid_off DESC
LIMIT 5;

-- 3.2: Companies with the most total layoffs
SELECT company, SUM(total_laid_off) AS total_laid_off
FROM layoffs_staging2
GROUP BY company
ORDER BY total_laid_off DESC
LIMIT 10;

-- 3.3: Total layoffs by location
SELECT location, SUM(total_laid_off) AS total_laid_off
FROM layoffs_staging2
GROUP BY location
ORDER BY total_laid_off DESC
LIMIT 10;

-- 3.4: Total layoffs by country
SELECT country, SUM(total_laid_off) AS total_laid_off
FROM layoffs_staging2
GROUP BY country
ORDER BY total_laid_off DESC;

-- 3.5: Total layoffs by year
SELECT YEAR(date) AS year, SUM(total_laid_off) AS total_laid_off
FROM layoffs_staging2
GROUP BY YEAR(date)
ORDER BY year ASC;

-- 3.6: Total layoffs by industry
SELECT industry, SUM(total_laid_off) AS total_laid_off
FROM layoffs_staging2
GROUP BY industry
ORDER BY total_laid_off DESC;

-- 3.7: Total layoffs by company stage
SELECT stage, SUM(total_laid_off) AS total_laid_off
FROM layoffs_staging2
GROUP BY stage
ORDER BY total_laid_off DESC;

-- Step 4: Advanced Queries for Deeper Insights

-- 4.1: Companies with the most layoffs per year

WITH Company_Year AS 
(
  SELECT company, YEAR(date) AS year, SUM(total_laid_off) AS total_laid_off
  FROM layoffs_staging2
  GROUP BY company, YEAR(date)
),
Company_Year_Rank AS (
  SELECT company, year, total_laid_off, DENSE_RANK() OVER (PARTITION BY year ORDER BY total_laid_off DESC) AS ranking
  FROM Company_Year
)
SELECT company, year, total_laid_off, ranking
FROM Company_Year_Rank
WHERE ranking <= 3
AND year IS NOT NULL
ORDER BY year ASC, total_laid_off DESC;

-- 4.2: Rolling total of layoffs per month
-- 4.2.1: Aggregate layoffs by month
WITH DATE_CTE AS 
(
  SELECT DATE_FORMAT(`date`, '%Y-%m') AS month, SUM(total_laid_off) AS total_laid_off
  FROM layoffs_staging2
  GROUP BY month
  ORDER BY month ASC
)
-- 4.2.2: Calculate the rolling total of layoffs
SELECT month, SUM(total_laid_off) OVER (ORDER BY month ASC) AS rolling_total_layoffs
FROM DATE_CTE
ORDER BY month ASC;
