-- EXPLORATORY DATA ANALYSIS (EDA)

select max(total_laid_off), max(percentage_laid_off)
from layoffs_staging2;

select *
from layoffs_staging2
where percentage_laid_off = 1
order by funds_raised_millions desc;

select company, sum(total_laid_off)
from layoffs_staging2
group by company
order by 2 desc;

select min(`date`), max(`date`)
from layoffs_staging2;

select substring(`date`,1,7) as `month`, sum(total_laid_off)
from layoffs_staging2
where substring(`date`,1,7) is not null
group by 1
order by 1;

with rolling_cte as(
select substring(`date`,1,7) as `month`, sum(total_laid_off) as rollingt_off
from layoffs_staging2
where substring(`date`,1,7) is not null
group by 1
order by 1
)
select `month`, rollingt_off, sum(rollingt_off) over(order by `month`) as rolling_total
from rolling_cte;

select company, year(`date`), sum(total_laid_off)
from layoffs_staging2
group by company, year(`date`)
order by 3 desc;

with company_year(company, years, total_laid_off) as (
select company, year(`date`), sum(total_laid_off)
from layoffs_staging2
group by company, year(`date`)
order by 3 desc
), company_year_rank as (
select *, dense_rank() over(partition by `years` order by total_laid_off desc) as `dense_rank`
from company_year
where `years` is not null)
select *
from company_year_rank
where `dense_rank` <= 5;