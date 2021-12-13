select 
	ingredient_id
	,ingredient_name
	,count(distinct product_id) num_products
	,count(distinct product_id)*1.0/(select count(distinct product_id) from worldfood.products) pct_products
from 
	worldfood.product_ingredients
group by 
	ingredient_id
	,ingredient_name
order by 
	count(*) desc