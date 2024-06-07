-- SQL Project - Data Cleaning: World Layoff Data
-- Dataset source: https://www.kaggle.com/datasets/swaptr/layoffs-2022

-- Step 1: Inspect the raw data
SELECT * 
FROM layoffs;

-- Step 2: Create a staging table to work with, preserving the raw data
CREATE TABLE layoffs_staging 
LIKE layoffs;

INSERT INTO layoffs_staging 
SELECT * FROM layoffs;

-- Data Cleaning Process:
-- 1. Handle null values 
-- 2. Remove duplicates 
-- 3. Remove unnecessary columns and rows
-- 4. Standardize data and fix errorss 

-- Step 3: Handle Null Values

-- Check for null values in key columns
SELECT *
FROM layoffs_staging
WHERE total_laid_off IS NULL OR percentage_laid_off IS NULL;

-- Remove rows with null values in both total_laid_off and percentage_laid_off
DELETE FROM layoffs_staging
WHERE total_laid_off IS NULL AND percentage_laid_off IS NULL;

-- Step 4: Remove Duplicates

-- 4.1: Identify duplicates
SELECT company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions,
       ROW_NUMBER() OVER (PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;

-- 4.2: View duplicates
SELECT *
FROM (
    SELECT company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions,
           ROW_NUMBER() OVER (PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
    FROM layoffs_staging
) duplicates
WHERE row_num > 1;

-- 4.3: Remove duplicates
WITH DELETE_CTE AS (
    SELECT company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions,
           ROW_NUMBER() OVER (PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
    FROM layoffs_staging
)
DELETE FROM layoffs_staging
WHERE (company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) IN (
    SELECT company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions
    FROM DELETE_CTE
    where row_num > 1
) ;



-- Step 5: Standardize Data

-- 5.1: Validate and Standardize industry values
SELECT DISTINCT industry
FROM layoffs_staging
ORDER BY industry;

UPDATE layoffs_staging
SET industry = 'Crypto'
WHERE industry IN ('Crypto Currency', 'CryptoCurrency');

-- 5.2: Check for null or empty industry values
SELECT *
FROM layoffs_staging
WHERE industry IS NULL OR industry = ''
ORDER BY industry;

-- 5.3: Set empty industry values to NULL
UPDATE layoffs_staging
SET industry = NULL
WHERE industry = '';

-- 5.4: Populate null industry values from other rows with the same company name
UPDATE layoffs_staging t1
JOIN layoffs_staging t2
ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL AND t2.industry IS NOT NULL;

SELECT *
FROM layoffs_staging
WHERE industry IS NULL;


-- 5.5: Standardize country values by removing trailing periods
UPDATE layoffs_staging
SET country = TRIM(TRAILING '.' FROM country);

-- Remove
UPDATE layoffs_staging
SET country = TRIM(TRAILING '.' FROM REPLACE(RTRIM(country), '.', ' '));


-- 5.6: Convert date column to proper date format
UPDATE layoffs_staging
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

ALTER TABLE layoffs_staging
MODIFY COLUMN `date` DATE;


-- Step 6: Final Clean-up
-- Remove
-- Drop unnecessary columns (e.g., row_num if it was added)
ALTER TABLE layoffs_staging
DROP COLUMN row_num;

-- Final inspection of cleaned data
SELECT * 
FROM world_layoffs.layoffs_staging;
