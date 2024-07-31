select *
from layoffs;

create table layoffs_staging
like layoffs;

select *
from layoffs_staging;

insert layoffs_staging #layoffs staging is an exact duplicate/copy of raw data to be safe
select *
from layoffs;


with duplicate_cte as(
select *, row_number() over(partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) as row_num
from layoffs_staging
)
select *
from duplicate_cte
where row_num > 1 #tested algorithm to filter duplicate entries
;

select *
from layoffs_staging
where company = 'Casper' #checking if algorithm is correct and safe
;

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

insert into layoffs_staging2
select *, row_number() over(partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) as row_num
from layoffs_staging #duplicated table with built in row_num for easier sorting and usage of delete argument since mysql cant delete from ctes
;

select *
from layoffs_staging2; #imported everything with extra row (kindof like a unique id)

delete
from layoffs_staging2
where row_num > 1; #deleted the duplicates.

-- cleaning done

-- standardizing data

select *
from layoffs_staging2;

update layoffs_staging2
set company = trim(company); #made company names compact

select distinct(industry)
from layoffs_staging2
order by industry;

update layoffs_staging2
set industry = "Crypto"
where (industry = 'Crypto') or (industry = 'Crypto Currency') or (industry = 'CryptoCurrency'); #spelled crypto wrong

select *
from layoffs_staging2
where industry = "Crpyto"; #wrong spelling

update layoffs_staging2
set industry = "Crypto"
where industry like 'Crpyto'; #corrected it

select *
from layoffs_staging2
where industry = "Crypto"
order by company;

select distinct country, trim(trailing '.' from country) as new_country
from layoffs_staging2
order by 1;

update layoffs_staging2
set country = trim(trailing '.' from country); #deletes periods from all countries

select `date`, str_to_date(`date`, '%m/%d/%Y') as new_date
from layoffs_staging2; #check algorithm for reformatting date from text type to datetime

update layoffs_staging2
set `date` = str_to_date(`date`, '%m/%d/%Y'); #udpated

select `date`
from layoffs_staging2; 

alter table layoffs_staging2
modify column `date` date; #chang date colum from text to datetime

select *
from layoffs_staging2
where industry is null or industry = ''; #some companies only have 1 industry entry and the others are null

select *
from layoffs_staging2
where company = 'Airbnb'; #airbnb is Travel. populate.

select *
from layoffs_staging2 as st1
join layoffs_staging2 as st2
	on st1.company = st2.company
where (st1.industry is null or st1.industry = '') and st2.industry is not null; #it works but there is a possibility for blank - blank 

update layoffs_staging2
set industry = null
where industry = ''; #cleaned out the blanks so its just either null or populated

update layoffs_staging2 as st1
join layoffs_staging2 as st2
	on	st1.company = st2.company
set st1.industry = st2.industry
where (st1.industry is null or st1.industry = '') and st2.industry is not null; #updated the table

select *
from layoffs_staging2
where industry is null or industry = ''; #bally's is null-null

select *
from layoffs_staging2; #there are total laid off and percentage laid off nulls. so it brings no value

select *
from layoffs_staging2
where total_laid_off is null and percentage_laid_off is null;

delete
from layoffs_staging2
where total_laid_off is null and percentage_laid_off is null; #deleted worthless info

select *
from layoffs_staging2; #unecessary row_num, it served its purpose

alter table layoffs_staging2
drop column row_num; #deleted row_num. Final clean data is layoffs_staging2.

