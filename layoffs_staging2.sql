-- DATA CLEANING

SELECT * FROM layoffs;


 CREATE TABLE layoffs_staging
 LIKE layoffs;
 
 SELECT *
 FROM layoffs_staging;
 
 INSERT layoffs_staging
 SELECT *
 FROM layoffs;
 
 
 -- REMOVING DUPLICATES
 
 SELECT *,
 ROW_NUMBER() OVER(
 PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`,
 stage, country, funds_raised_millions) AS row_num
 FROM layoffs_staging;
 
 WITH duplicate_row AS
 (
 SELECT *,
 ROW_NUMBER() OVER(
 PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`,
 stage, country, funds_raised_millions) AS row_num
 FROM layoffs_staging
 )
 SELECT *
 FROM duplicate_row
 WHERE row_num > 1;
 
 
 SELECT *
 FROM layoffs_staging
 WHERE company = 'Casper';
 

 CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT 
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

 
 SELECT *
 FROM layoffs_staging2;
 
 INSERT INTO layoffs_staging2
 SELECT *,
 ROW_NUMBER() OVER(
 PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`,
 stage, country, funds_raised_millions) AS row_num
 FROM layoffs_staging;
 

 DELETE
 FROM layoffs_staging2
 WHERE row_num > 1;
 
 
 -- STANDARDIZING THE DATA
 
 SELECT company, TRIM(company)
 FROM layoffs_staging2;
 
 UPDATE layoffs_staging2
 SET company= TRIM(company);
 
 SELECT DISTINCT(industry)
 FROM layoffs_staging2
 ORDER BY industry;
 
 
 UPDATE layoffs_staging2
 SET industry = 'Crypto'
 WHERE industry LIKE 'Crypto%';
 
 
 SELECT *
 FROM layoffs_staging2
 ;
 
 
 SELECT DISTINCT location
 FROM layoffs_staging2
 order by 1;
 
 UPDATE layoffs_staging2
 SET country = 'United States'
 WHERE country LIKE 'United States%';
 
 
 SELECT `date`,
 STR_TO_DATE(`date`, '%m/%d/%Y')
 FROM layoffs_staging2;
 
 UPDATE layoffs_staging2
 SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');
 
 ALTER TABLE layoffs_staging2
 MODIFY COLUMN `date` DATE;
 
 
 -- DEALING WITH NULL OR BLANK VALUES
 
 SELECT *
 FROM layoffs_staging2
 WHERE industry IS NULL
 OR industry = '';
 
 UPDATE layoffs_staging2
 SET industry = NULL 
 WHERE industry = '';
 
 
  SELECT t1.industry, t2.industry
  FROM layoffs_staging2 t1
  JOIN layoffs_staging2 t2
	ON t1.company = t2.company
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;
 
 UPDATE layoffs_staging2 t1
  JOIN layoffs_staging2 t2
	ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;
 
 
 DELETE
 FROM layoffs_staging2
 WHERE total_laid_off IS NULL
 AND percentage_laid_off IS NULL;
 
 
-- REMOVING ANY COULMNS

SELECT *
FROM layoffs_staging2;

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;
 
 
 
 
 
 
 
 
 
 -- EXPLORATORY DATA ANALYSIS
 
 SELECT *
 FROM layoffs_staging2;
 
 -- Highest layoff event
 SELECT *
 from layoffs_staging2
 where total_laid_off IN 
 (SELECT MAX(total_laid_off)
 FROM layoffs_staging2);
 
 
-- Companies that laid-off all their employees
 SELECT *
 FROM layoffs_staging2
 WHERE percentage_laid_off = 1
 ORDER BY funds_raised_millions DESC;

 
 
 -- Overall layoff per company
 SELECT company, SUM(total_laid_off) AS overall_layoff
 FROM layoffs_staging2
 GROUP BY company
 ORDER BY 2 DESC;
 
 
 
 -- When does this layoff event start and end
SELECT MIN(`date`), MAX(`date`)
FROM layoffs_staging2;

-- Which industry was hit the most
SELECT industry, SUM(total_laid_off) AS overall_layoff
FROM layoffs_staging2
GROUP BY industry
ORDER BY 2 DESC;
 
 
 -- Top countries affected
SELECT country, SUM(total_laid_off) AS overall_layoff
FROM layoffs_staging2
GROUP BY country
ORDER BY 2 DESC;


-- Yearly layoff trend
SELECT YEAR(`date`), SUM(total_laid_off) AS overall_layoff
FROM layoffs_staging2
GROUP BY YEAR(`date`)
ORDER BY 1 DESC;



-- which stage had highest layoff
SELECT stage, SUM(total_laid_off) AS overall_layoff
FROM layoffs_staging2
GROUP BY stage
ORDER BY 2 DESC;


-- companies with layoffs less than average layoff
SELECT company, total_laid_off
FROM layoffs_staging2
WHERE total_laid_off <
(SELECT AVG(total_laid_off)
FROM layoffs_staging2);

-- rolling total 
SELECT SUBSTRING(`date`, 1,7) AS `MONTH`, SUM(total_laid_off) AS overall_layoff
FROM layoffs_staging2
WHERE SUBSTRING(`date`, 1,7) IS NOT NULL
GROUP BY `MONTH`
ORDER BY 1 ASC;

WITH Rolling_total AS
(
SELECT SUBSTRING(`date`, 1,7) AS `MONTH`, SUM(total_laid_off) AS overall_layoff
FROM layoffs_staging2
WHERE SUBSTRING(`date`, 1,7) IS NOT NULL
GROUP BY `MONTH`
ORDER BY 1 ASC
)
SELECT `MONTH`, overall_layoff, 
SUM(overall_layoff) OVER(ORDER BY `MONTH`) AS rolling_total
FROM Rolling_total;

-- which company had multiple layoffs year wise
SELECT company, YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company, YEAR(`date`)
ORDER BY company;

-- yearwise ranking of top companies that laid-off their employees
WITH company_yearwise(company, years, overall_layoff) AS 
(
SELECT company, YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company, YEAR(`date`)
ORDER BY company
), company_rank AS
(SELECT *, DENSE_RANK() OVER(PARTITION BY years ORDER BY overall_layoff DESC) AS ranking
FROM company_yearwise
WHERE years IS NOT NULL)
SELECT *
FROM company_rank
WHERE ranking <= 5;

select * from layoffs_staging2;






