with cte as (
	select 
		p.product_id
		,product_name
		,pn.nutrient_name
		,pn.nutrient_value
		,PERCENT_RANK() over (partition by nutrient_name order by cast(nutrient_value as float)) nutrient_value_percentile
		,case
			when PERCENT_RANK() over (partition by nutrient_name order by cast(nutrient_value as float)) >= .9 then 'high'
			when PERCENT_RANK() over (partition by nutrient_name order by cast(nutrient_value as float)) <= .1 then 'low'
			else null end nutrient_indicator
	from 
		worldfood.product_nutrients pn 
	join 
		worldfood.products p on pn.product_id=p.product_id
	where 
		pn.nutrient_name in ('carbohydrates_serving','proteins_serving','energy-kcal_serving')
)

select
	product_id
	,product_name
	--Carbs
	,max(case when nutrient_name = 'carbohydrates_serving' then nutrient_value else null end) carbs_per_serving
	,max(case when nutrient_name = 'carbohydrates_serving' then nutrient_value_percentile else null end) carbs_per_serving_percentile
	,max(case when nutrient_name = 'carbohydrates_serving' then nutrient_indicator else null end) carbs_per_serving_indicator
	--Protein
	,max(case when nutrient_name = 'proteins_serving' then nutrient_value else null end) protein_per_serving
	,max(case when nutrient_name = 'proteins_serving' then nutrient_value_percentile else null end) protein_per_serving_percentile
	,max(case when nutrient_name = 'proteins_serving' then nutrient_indicator else null end) protein_per_serving_indicator
	--Calories
	,max(case when nutrient_name = 'energy-kcal_serving' then nutrient_value else null end) calories_per_serving
	,max(case when nutrient_name = 'energy-kcal_serving' then nutrient_value_percentile else null end) calories_per_serving_percentile
	,max(case when nutrient_name = 'energy-kcal_serving' then nutrient_indicator else null end) calories_per_serving_indicator
from
	cte
group by
	product_id
	,product_name