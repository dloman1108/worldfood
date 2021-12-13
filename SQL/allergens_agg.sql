select 
	a.allergen_id
	,ar.allergen_name
	,count(distinct a.product_id) num_products
	,count(distinct a.product_id)*1.0/(select count(distinct product_id) from worldfood.products) pct_products
from 
	worldfood.product_allergens a
left join
	worldfood.allergens_reference ar on a.allergen_id=ar.allergen_id
group by 
	a.allergen_id
	,ar.allergen_name
order by 
	count(*) desc