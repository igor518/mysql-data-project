-- Data cleaning

SELECT * FROM layoffs;

-- 1. Remove Duplicates
-- 2. Standardize the Data
-- 3. Null Values or blank values
-- 4. Remove any columns

CREATE TABLE layoffs_staging LIKE layoffs;

SELECT * FROM layoffs_staging;

INSERT INTO layocompanycompanyindustryffs_staging
SELECT * FROM layoffs;

SELECT *, ROW_NUMBER() OVER(
PARTITION BY company, industry, total_laid_off, percentage_laid_off, `date`, location, stage, country, funds_raised_millions
) as row_num FROM layoffs_staging;

WITH duplicate_cte AS
(
SELECT *, ROW_NUMBER() OVER(
PARTITION BY company, industry, total_laid_off, percentage_laid_off, `date`, location, stage, country, funds_raised_millions
) as row_num FROM layoffs_staging
)
SELECT * FROM duplicate_cte
WHERE row_num > 1;

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
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT * FROM layoffs_staging2 WHERE row_num > 1;

INSERT INTO layoffs_staging2
SELECT *, ROW_NUMBER() OVER(
PARTITION BY company, industry, total_laid_off, percentage_laid_off, `date`, location, stage, country, funds_raised_millions
) as row_num FROM layoffs_staging;

DELETE FROM layoffs_staging2 WHERE row_num > 1;

-- Standardize the Data

UPDATE layoffs_staging2 SET company = TRIM(company);
SELECT * FROM layoffs_staging2 WHERE industry LIKE 'Crypto%';
UPDATE layoffs_staging2 SET industry = 'Crypto' WHERE industry LIKE 'Crypto%';
UPDATE layoffs_staging2 SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';
SELECT * FROM layoffs_staging2;
UPDATE layoffs_staging2 SET `date` = str_to_date(`date`, '%m/%d/%Y');
ALTER TABLE layoffs_staging2 MODIFY COLUMN `date` DATE;
SELECT * FROM layoffs_staging2 
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;
 
SELECT *
 FROM layoffs_staging2
 WHERE industry IS NULL
 OR industry = '';
 
 SELECT *
 FROM layoffs_staging2 where
 company = 'Airbnb';
 
 select t1.industry, t2.industry from layoffs_staging2 t1
 JOIN layoffs_staging2 t2 
    ON t1.company = t2.company
    AND t1.location = t2.location 
 WHERE (t1.industry IS NULL OR t1.industry = '') AND t2.industry IS NOT NULL;
 
 UPDATE layoffs_staging2 
 SET industry = NULL 
 WHERE industry = '';
 
UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2 
    ON t1.company = t2.company
    SET t1.industry = t2.industry
    WHERE t1.industry IS NULL
    AND t2.industry IS NOT NULL;

ALTER TABLE layoffs_staging2 DROP column row_num;

SELECT * FROM layoffs_staging2;

-- Exploratory data analysis
SELECT MAX(total_laid_off), MAX(percentage_laid_off) FROM layoffs_staging2;

SELECT * FROM layoffs_staging2 WHERE percentage_laid_off=1
 ORDER BY funds_raised_millions DESC;

SELECT company, SUM(total_laid_off) FROM layoffs_staging2 GROUP BY company ORDER BY 2 DESC;
SELECT MIN(`date`), MAX(`date`) FROM layoffs_staging2;
SELECT country, SUM(total_laid_off) FROM layoffs_staging2 GROUP BY country ORDER BY 2 DESC;
SELECT YEAR(`date`), SUM(total_laid_off) FROM layoffs_staging2 GROUP BY YEAR(`date`) ORDER BY 1 DESC;
SELECT stage, SUM(total_laid_off) FROM layoffs_staging2 GROUP BY stage ORDER BY 1 DESC;

SELECT SUBSTRING(`date`,1,7) AS `MONTH`, SUM(total_laid_off) FROM layoffs_staging2 WHERE SUBSTRING(`date`,1,7) IS NOT NULL group by 1 ORDER BY 1 ASC;

WITH Rolling_Total AS (
  SELECT SUBSTRING(`date`,1,7) AS `MONTH`, SUM(total_laid_off) AS total_off FROM layoffs_staging2 WHERE SUBSTRING(`date`,1,7) IS NOT NULL group by 1 ORDER BY 1 ASC
)
SELECT `MONTH`, total_off, SUM(total_off) OVER (ORDER BY `MONTH`) AS rolling_total FROM Rolling_Total;

SELECT company, SUM(total_laid_off) FROM layoffs_staging2 GROUP BY company ORDER BY 2 DESC;

SELECT company, YEAR(`date`), SUM(total_laid_off) 
FROM layoffs_staging2 GROUP BY company, YEAR(`date`)
ORDER BY 3 DESC;

WITH Company_Year (company, years, total_laid_off) AS (
SELECT company, YEAR(`date`), SUM(total_laid_off) 
FROM layoffs_staging2 GROUP BY company, YEAR(`date`)
), Company_Year_Rank AS (
SELECT *, dense_rank() OVER (partition by years ORDER BY total_laid_off DESC) AS Ranking
FROM Company_Year
WHERE years IS NOT NULL
)
SELECT * FROM Company_Year_Rank WHERE Ranking <= 5;