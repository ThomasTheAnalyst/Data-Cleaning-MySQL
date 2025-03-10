-- Exploratory Data Analysis

select*
from layoffs_staging2;

select max(total_laid_off), max(percentage_laid_off) #Revisamos los mayores valores de despedidos por cantidad y porcentaje
from layoffs_staging2;

select* #Encontramos las compañías donde se despidió a todo el personal (cerraron)
from layoffs_staging2
where percentage_laid_off=1
order by total_laid_off desc;

select company, sum(total_laid_off) #Vemos la suma del total de los despidos por empresa
from layoffs_staging2
group by company
order by 2 desc;

select min(`date`),max(`date`) #Usamos esta expresión para revisar el intervalo en el que tenemos datos
from layoffs_staging2;

select industry, sum(total_laid_off) #Vemos las industrias más golpeadas por los despidos en cantidad total
from layoffs_staging2
group by industry
order by 2 desc;

select country, sum(total_laid_off) #Vemos los países más golpeados por los despidos en cantidad total
from layoffs_staging2
group by country
order by 2 desc;

select year(`date`), sum(total_laid_off) #Vemos la cantidad de despidos por año
from layoffs_staging2
group by year(`date`)
order by 1 desc;

select stage, sum(total_laid_off) #Vemos en que estado están las empresas y la cantidad de despidos de estas etapas
from layoffs_staging2
group by stage
order by 2 desc;

select substring(`date`,1,7) as `month`, sum(total_laid_off) #obtenemos los despidos de cada mes desde el inicio
from layoffs_staging2
where substring(`date`,1,7) is not null
group by `month`
order by 1 asc;

with Rolling_Total as #Para obtener la suma total cada mes hacemos un CTE
(
select substring(`date`,1,7) as `month`, sum(total_laid_off) as total_off
from layoffs_staging2
where substring(`date`,1,7) is not null
group by `month`
order by 1 asc
)
select `month`, total_off, sum(total_off) over(order by `month`) as rolling_sum #over(order by `month`) se encarga de ir sumando mes a mes
from Rolling_Total;

select company, year(`date`), sum(total_laid_off) #Revisamos los despidos de cada empresa por año
from layoffs_staging2
group by company, year(`date`)
order by company asc;

with Company_Year(Company, Years, Total_Laid_Off) as #Creamos una CTE para trabajar sobre los datos obtenidos
(
select company, year(`date`), sum(total_laid_off)
from layoffs_staging2
group by company, year(`date`)
),
Company_Year_Rank as #Creamos otra CTE para filtrar sobre lo obtenido con la primera CTE
(
select *, 
dense_rank() over(partition by Years order by Total_Laid_Off desc) as Ranking #dense_rank entrega como número 1 la compañía con más despidos por año
from Company_Year
where Years is not null
)
select *
from Company_Year_Rank
where Ranking <= 5
;
