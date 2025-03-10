-- Data Cleaning

select *
from layoffs;

-- 1. Remove Duplicates
-- 2. Standardizing Data
-- 3. Null or Blank Values
-- 4. Remove Any Columns

create table layoffs_staging #se crea una tabla igual y a la raw data para poder trabajar en ella libremente sin borrar nada importante de la original
like layoffs;

select *  
from layoffs_staging;

insert layoffs_staging 
select *
from layoffs;

select *,
row_number() over(partition by
company, industry, total_laid_off, "date") as row_num
from layoffs_staging;

with duplicate_cte as
(
select *,
row_number() over(partition by
company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) as row_num
from layoffs_staging
)
select *
from duplicate_cte
where row_num > 1;

select *  
from layoffs_staging
where company = "Casper"; #revisar un ejemplo de duplicado

with duplicate_cte as
(
select *,
row_number() over(partition by
company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) as row_num
from layoffs_staging
)
delete
from duplicate_cte
where row_num > 1; #aparecerá un error, porque no se puede actulizar una CTE con delete

CREATE TABLE `layoffs_staging2` (   #creamos una nueva tabla donde podemos copiar los datos obtenidos y luego eliminar los valores duplicados
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

select *
from layoffs_staging2;

insert into layoffs_staging2
select *,
row_number() over(partition by
company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) as row_num
from layoffs_staging;

delete
from layoffs_staging2
where row_num > 1;

select *
from layoffs_staging2
where row_num > 1;

-- Standardizing Data

select company, trim(company)
from layoffs_staging2;

update layoffs_staging2
set company = trim(company);

select distinct industry
from layoffs_staging2
order by 1;

select *
from layoffs_staging2
where industry like "crypto%";

update layoffs_staging2
set industry = "Crypto"
where industry like "Crypto%";

select distinct location  #tratar de revisar todo por duplicados
from layoffs_staging2
order by 1;

select distinct country  #encontramos "United States" con y sin punto al final
from layoffs_staging2
order by 1;

select distinct country, trim(trailing "." from country)  #trailing busca desde el final
from layoffs_staging2
order by 1;

update layoffs_staging2
set country = trim(trailing "." from country)
where country like "United States%";

#date está como texto, queremos cambiarlo a formato de fecha para trabajar con el dato

select `date`
from layoffs_staging2;

update layoffs_staging2
set `date` = str_to_date(`date`, "%m/%d/%Y"); #cambiamos el formato de la fecha, pero el dato sigue siendo string

alter table layoffs_staging2 #ahora si cambiamos el dato a fecha, que no podíamos hacer antes sin cambiar el formato
modify column `date` date;

-- Null or Blank Values

select *
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;  #estos valores no son útiles, así que los borraremos más adelante

select *
from layoffs_staging2
where industry is null
or industry = ""; #encontramos 4 filas donde tenemos null o vacíos

select *
from layoffs_staging2
where company = "Airbnb"; #vemos que hay otras filas con la información faltante

select t1.industry, t2.industry
from layoffs_staging2 t1
join layoffs_staging2 t2
	on t1.company = t2.company
    and t1.location = t2.location
where (t1.industry is null or t1.industry = "")
and (t2.industry is not null and t2.industry != "");

update layoffs_staging2 t1 #se hace un join con la misma tabla, pero poblando donde hay nulls o vacíos con la industry que si tenemos en otro campo
join layoffs_staging2 t2
	on t1.company = t2.company
set t1.industry = t2.industry
where (t1.industry is null or t1.industry = "")
and (t2.industry is not null and t2.industry != "");

select *
from layoffs_staging2
where industry is null or industry = "";

select *
from layoffs_staging2
where company = "Bally's Interactive";  #encontramos sólo una compañía que no tiene industry en ningún campo

-- Remove Any Columns

select *
from layoffs_staging2
where percentage_laid_off is null
and total_laid_off is null;

delete
from layoffs_staging2
where percentage_laid_off is null
and total_laid_off is null;

select *
from layoffs_staging2;

alter table layoffs_staging2 #elimina una columna completa
drop column row_num;



