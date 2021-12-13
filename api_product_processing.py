#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Dec  6 12:10:21 2021

@author: dh08loma
"""


#Import packages
import requests
import pandas as pd
import yaml
import sqlalchemy as sa
import os
import numpy as np



#Get credentials stored in secrets.yaml file (saved in same directory)
#(Note: Nuvalence credentials have read access only)
def get_engine():

	if os.path.isfile('secrets.yaml'):
		with open('secrets.yaml', 'r') as stream:
			data_loaded = yaml.load(stream)
			
			user=data_loaded['EDW']['user']
			password=data_loaded['EDW']['password']
			endpoint=data_loaded['EDW']['endpoint']
			port=data_loaded['EDW']['port']
			database=data_loaded['EDW']['database']
			
	db_string = "postgres://{0}:{1}@{2}:{3}/{4}".format(user,password,endpoint,port,database)
	engine=sa.create_engine(db_string)
	
	return engine  
        
    

#Function to process a single page of products and store results into 7 tables:
#1. Products - table of all products
#2. Ingredients - maps products to ingredients
#3. Allergens - maps products to allergens
#4. Nutrients - maps products to nutrients
#5. Brands - maps products to brands
#6. Categories - maps products to categories
#7. Logging - logs pages processed, plus if any had errors
def process_page(all_food_products,engine):
    
    products = all_food_products['products']
    
    products_df = pd.DataFrame()
    ingredients_df = pd.DataFrame()
    allergens_df = pd.DataFrame()
    nutrients_df = pd.DataFrame()
    brands_df = pd.DataFrame()
    categories_df = pd.DataFrame()
    
    for product in products:
        
        if product['lang'] == 'en' and 'product_name' in product:
            
            ###Products
            product_df = pd.DataFrame({'product_id': [product['id']], 
                                        'product_name': [product['product_name']]})
            products_df = products_df.append(product_df)
            
            ### Ingredients
            if 'ingredients' in product:
                product_ingredients_df = pd.DataFrame(product['ingredients'])
                product_ingredients_df['product_id'] = product['_id']
                ingredients_df = ingredients_df.append(product_ingredients_df)
            
            
            ### Allergens
            if 'allergens_tags' in product:
                product_ids=[]
                allergens=[]
                for allergen in product['allergens_tags']:
                    product_ids.append(product['_id'])
                    allergens.append(allergen)
                    
                product_allergens_df = pd.DataFrame({'product_id':product_ids,
                                                      'allergen_id':allergens})
                allergens_df = allergens_df.append(product_allergens_df)
            
            ###Nutrients
            if 'nutriments' in product:
                product_nutrient_df = pd.DataFrame([product['nutriments']])\
                    .transpose()\
                    .reset_index()\
                    .rename(columns = {'index': 'nutrient_name', 0: 'nutrient_value'})
                product_nutrient_df['product_id'] = product['_id']

                nutrients_df = nutrients_df.append(product_nutrient_df)
                
                
            ### Brands
            if 'brands_tags' in product:
                product_ids=[]
                brands=[]
                for brand in product['brands_tags']:
                    product_ids.append(product['_id'])
                    brands.append(brand)
                    
                product_brands_df = pd.DataFrame({'product_id':product_ids,
                                                  'brand_id':brands})
                
                brands_df = brands_df.append(product_brands_df)
                
            
            ### Categories
            if 'categories_tags' in product:
                product_ids=[]
                categories=[]
                for category in product['categories_tags']:
                    product_ids.append(product['_id'])
                    categories.append(category)
                    
                product_categories_df = pd.DataFrame({'product_id':product_ids,
                                                      'category_id':categories})
                
                categories_df = categories_df.append(product_categories_df)
            
    
    
    if len(products_df) > 0:
        products_df.to_sql('products',
                            con=engine,
                            schema='worldfood',
                            index=False,
                            if_exists='append',
                            dtype={'product_id': sa.types.BIGINT(),
                                  'product_name': sa.types.VARCHAR(length=255)})
     
    if len(ingredients_df) > 0:
        ingredients_df = ingredients_df[['product_id','id','text']]\
            .rename(columns={'id':'ingredient_id', 'text': 'ingredient_name'})
        ingredients_df.to_sql('product_ingredients',
                              con=engine,
                              schema='worldfood',
                              index=False,
                              if_exists='append',
                              dtype={'product_id': sa.types.BIGINT(),
                                      'ingredient_id': sa.types.VARCHAR(length=255),
                                      'ingredient_name': sa.types.VARCHAR(length=255)})
    
    if len(allergens_df) > 0:
        allergens_df.to_sql('product_allergens',
                            con=engine,
                            schema='worldfood',
                            index=False,
                            if_exists='append',
                            dtype={'product_id': sa.types.BIGINT(),
                                  'allergen_id': sa.types.VARCHAR(length=255)})
    
    if len(nutrients_df) > 0:
        nutrients_df.to_sql('product_nutrients',
                            con=engine,
                            schema='worldfood',
                            index=False,
                            if_exists='append',
                            dtype={'nutrient_name': sa.types.VARCHAR(length=255),
                                    'nutrient_value': sa.types.VARCHAR(length=255),
                                    'product_id': sa.types.BIGINT()})
        
        
    if len(brands_df) > 0:
        brands_df.to_sql('product_brands',
                         con=engine,
                         schema='worldfood',
                         index=False,
                         if_exists='append',
                         dtype={'product_id': sa.types.BIGINT(),
                              'brand_id': sa.types.VARCHAR(length=255)})
        
        
    if len(categories_df) > 0:
        categories_df.to_sql('product_categories',
                             con=engine,
                             schema='worldfood',
                             index=False,
                             if_exists='append',
                             dtype={'product_id': sa.types.BIGINT(),
                                   'category_id': sa.types.VARCHAR(length=255)})
        
        
    #Add logging
    logging_df=pd.DataFrame({'page': [all_food_products['page']],
                              'products': [len(products_df)],
                              'allergens': [len(allergens_df)],
                              'ingredients': [len(ingredients_df)],
                              'nutrients': [len(nutrients_df)],
                              'error_processing': [False]})
    
    logging_df.to_sql('logging',
                        con=engine,
                        schema='worldfood',
                        index=False,
                        if_exists='append',
                        dtype={'page': sa.types.INTEGER(),
                                'products': sa.types.INTEGER(),
                                'allergens': sa.types.INTEGER(),
                                'ingredients': sa.types.INTEGER(),
                                'nutrient_levels': sa.types.INTEGER(),
                                'error_processing': sa.types.BOOLEAN()})


#Function to process all product pages (24 products per page)
def process_all_pages(engine,page_num):
    all_food_products = requests.get('https://us.openfoodfacts.org/.json?page=1').json()
    num_pages = int(np.ceil(all_food_products['count']/24.))
    #num_pages=
    cnt=1
    for page in range(page_num,num_pages+1):
        all_food_products = requests.get('https://us.openfoodfacts.org/.json?page={}'.format(str(page))).json()

        
        try:
            process_page(all_food_products,engine)
        except:
            logging_df=pd.DataFrame({'page': [page],
                                      'products': [0],
                                      'allergens': [0],
                                      'ingredients': [0],
                                      'nutrients': [0],
                                      'error_processing': [True]})
    
            logging_df.to_sql('logging',
                                con=engine,
                                schema='worldfood',
                                index=False,
                                if_exists='append',
                                dtype={'page': sa.types.INTEGER(),
                                        'products': sa.types.INTEGER(),
                                        'allergens': sa.types.INTEGER(),
                                        'ingredients': sa.types.INTEGER(),
                                        'nutrient_levels': sa.types.INTEGER(),
                                        'error_processing': sa.types.BOOLEAN()})
        
        #Logging
        if np.mod(cnt,10) == 0:
            print(cnt)
            
        cnt=cnt+1
    

def main():
    engine=get_engine()
    page_num=1 #Page to start processing at
    process_all_pages(engine,page_num)
        
    
    
if __name__ == "__main__":
    main()     
