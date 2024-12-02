SELECT *
FROM sqlproject.layoffs
LIMIT 5;

-- 1. Remove dubplicates 
-- 2. Standarize the data
-- 3. Null values
-- 4. remove any columns and rows that are not necessary - few ways

CREATE TABLE Back_up_1
SELECT *
FROM layoffs;

SELECT *
FROM back_up_1;

-- 1. Remove dubplicates 

SELECT *, ROW_NUMBER() OVER (PARTITION BY company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions) AS Num
FROM back_up_1;

WITH duplicate_CTE AS (SELECT * , ROW_NUMBER()
 OVER (PARTITION BY company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions) AS Num
FROM back_up_1)
SELECT * 
FROM duplicate_CTE
WHERE Num > 1;
-- ERROR BECAUSE WE NEED TO CREATE ANOTHER TABLE AND BUT Num COLUMN ON IT AS DEAFULT 
DELETE 
FROM duplicate_CTE
WHERE Num > 1;
-- HERE IS THE CODE 
CREATE TABLE `back_up_2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `Num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


SELECT *
FROM back_up_2; 
-- TO FILL THE TABLE WITH MY DATA
INSERT INTO back_up_2
SELECT * , ROW_NUMBER()
 OVER (PARTITION BY company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions) AS Num
FROM back_up_1;

SELECT * 
FROM back_up_2;

DELETE 
FROM back_up_2
WHERE Num >1;

-- 2. Standarize the data
SELECT company , TRIM(company)
FROM back_up_2;

UPDATE back_up_2
SET company = TRIM(company);

SELECT DISTINCT(location) 
FROM back_up_2
ORDER BY location ASC;

SELECT DISTINCT(industry) 
FROM back_up_2
ORDER BY industry ASC;

UPDATE back_up_2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

UPDATE back_up_2
SET date = str_to_date(`date`,'%m/%d/%Y');

alter table back_up_2
modify column `date` DATE;

SELECT *
FROM back_up_2;

SELECT * 
FROM back_up_2
WHERE industry IS NULL OR industry=  '';

SELECT * 
FROM back_up_2
WHERE location = 'SF Bay Area' AND company = 'Airbnb';

SELECT *
FROM back_up_2 b1
JOIN back_up_2 b2
    ON b1.company = b2.company
    AND b1.location = b2.location
WHERE (b1.industry IS NULL OR b1.industry = '')
  AND b2.industry IS NOT NULL;

UPDATE back_up_2 b1
JOIN back_up_2 b2
    ON b1.company = b2.company
    AND b1.location = b2.location
SET b1.industry = b2.industry
WHERE (b1.industry IS NULL OR b1.industry = '')
  AND b2.industry IS NOT NULL;

alter table back_up_2;

SELECT * 
FROM back_up_2
WHERE industry IS NULL OR industry=  '';

SELECT *
FROM back_up_2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

DELETE FROM back_up_2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

ALTER TABLE back_up_2
DROP COLUMN `Num`;

SELECT * 
FROM back_up_2;

-- ------------------------------------------------
-- NOW AFTER CLEAN THE DATA LET'S EXPLORE IT 
-- ------------------------------------------------

SELECT MAX(total_laid_off), MIN(total_laid_off)
FROM back_up_2;

SELECT MAX(percentage_laid_off), MIN(percentage_laid_off)
FROM back_up_2
WHERE  percentage_laid_off IS NOT NULL;

SELECT MAX(date), MIN(date)
FROM back_up_2;

SELECT COUNT(company) AS Total_Companies , COUNT(DISTINCT COMPANY) AS No_Companies
FROM back_up_2;

-- if we order by funcs_raised_millions we can see how big some of these companies were
SELECT *
FROM back_up_2
WHERE  percentage_laid_off = 1
ORDER BY funds_raised_millions DESC;

SELECT company, industry, `date`, SUM(total_laid_off) AS total_laid_off
FROM back_up_2
GROUP BY company, industry, `date`
ORDER BY 4 DESC;

SELECT company, `date`, SUM(total_laid_off) AS total_laid_off , SUM(funds_raised_millions) AS total_funds
FROM back_up_2
GROUP BY company, `date`
ORDER BY 4 DESC;
-- So we found that fund is inversely proportional to total laid off




-- by company
SELECT company, SUM(total_laid_off)
FROM back_up_2
GROUP BY company
ORDER BY 2 DESC
LIMIT 10;

-- the highest is Amazon then Google them Meta

-- by country 
SELECT country, SUM(total_laid_off)
FROM back_up_2
GROUP BY country
ORDER BY 2 DESC;

-- the highest country is United States

-- by year
SELECT YEAR(date), SUM(total_laid_off)
FROM back_up_2
GROUP BY YEAR(date)
ORDER BY 1 Desc;

-- ---------------------------------------------------------------------
-- Rank Top 3 company each year

WITH Company_Year AS 
(
  SELECT company, YEAR(date) AS years, SUM(total_laid_off) AS total_laid_off
  FROM back_up_2
  GROUP BY company, YEAR(date)
)
, Company_Year_Rank AS (
  SELECT company, years, total_laid_off, DENSE_RANK() OVER (PARTITION BY years ORDER BY total_laid_off DESC) AS ranking
  FROM Company_Year
)
SELECT company, years, total_laid_off, ranking
FROM Company_Year_Rank
WHERE ranking <= 3
AND years IS NOT NULL
ORDER BY years ASC, total_laid_off DESC;
-- ---------------------------------------------------------
-- total lay off per mounth
SELECT SUBSTRING(date,1,7) as dates, SUM(total_laid_off) AS total_laid_off
FROM back_up_2
GROUP BY dates
ORDER BY dates ASC;

-- now use it in a CTE so we can query off of it
WITH DATE_CTE AS 
(
SELECT SUBSTRING(date,1,7) as dates, SUM(total_laid_off) AS total_laid_off
FROM back_up_2
GROUP BY dates
ORDER BY dates ASC
)
SELECT dates, SUM(total_laid_off) OVER (ORDER BY dates ASC) as rolling_total_layoffs
FROM DATE_CTE
ORDER BY dates ASC;
 

