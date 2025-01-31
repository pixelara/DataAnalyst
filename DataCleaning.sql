-- Data Cleaning 

select * from layoffs;

create table layoffs_staging 
like layoffs;

select * from layoffs_staging;

insert layoffs_staging 
select * from layoffs;

-- Remove duplicates

select *,
row_number() over(Partition by company, location, industry, total_laid_off, `date`, stage, country, funds_raised_millions) 
from layoffs_staging;

With duplicate_cte as 
(
select *,
row_number() over(Partition by company, location, industry, total_laid_off, `date`, stage, country, funds_raised_millions) as row_num 
from layoffs_staging 
) 
select * 
from duplicate_cte 
where row_num > 1;

With duplicate_cte as 
(
select *,
row_number() over(Partition by company, location, industry, total_laid_off, `date`, stage, country, funds_raised_millions) as row_num 
from layoffs_staging 
) 
delete 
from duplicate_cte 
where row_num > 1;

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

select * from layoffs_staging2;

Insert into layoffs_staging2 
select *,
row_number() over(Partition by company, location, industry, total_laid_off, `date`, stage, country, funds_raised_millions) as row_num 
from layoffs_staging;

select * 
from layoffs_staging2 
where row_num > 1;

delete 
from layoffs_staging2 
where row_num > 1;

-- Standardizing data

Select company,  trim(company) 
from layoffs_staging2;

update layoffs_staging2 
set company = trim(company) ;

select distinct industry from 
layoffs_staging2 
order by 1;

select * from 
layoffs_staging2 
where industry like 'Crypto%';

update layoffs_staging2 
set industry = 'Crypto' 
where industry like 'Crypto%';

select distinct country 
from layoffs_staging2 
order by 1;

select distinct country, trim(trailing '.' from country) 
from layoffs_staging2 
order by 1; 

select `date`, 
str_to_date(`date`, '%m/%d/%Y') 
from layoffs_staging2;

UPDATE layoffs_staging2 
set date = str_to_date(`date`, '%m/%d/%Y') ;

update layoffs_staging2 
set country = trim(trailing '.' from country)  
where country like 'United States%';

ALTER table layoffs_staging2 
Modify column `date` DATE;

update layoffs_staging2 
set industry = null 
where industry = '';

select *
from layoffs_staging2 
where industry is null 
or industry ='';

select * 
from layoffs_staging2 
where company = 'Airbnb';

select * 
from layoffs_staging2 t1 
join layoffs_staging2 t2 
on t1.company = t2.company 
and t1.location = t2.location 
where (t1.industry is null or t1.industry = '') 
and t2.industry is not null;

update layoffs_staging2 t1 
join layoffs_staging2 t2 
	on t1.company = t2.company 
set t1.industry = t2.industry 
where t1.industry is null 
and t2.industry is not null;

-- remove staging columns if any 

alter table layoffs_staging2 
drop column row_num;
