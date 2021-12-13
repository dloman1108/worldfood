#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Dec  8 09:50:50 2021

@author: dh08loma
"""


#Import packages
import requests
import pandas as pd
import yaml
import sqlalchemy as sa
import os



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

def process_allergens_reference(engine):
    
    #Pull allergens metadata
    allergens_reference = requests.get("https://us.openfoodfacts.org/allergens.json").json()
    
    #Convert to dataframe
    allergens_reference_df = pd.DataFrame(allergens_reference['tags'])
    
    #Keep column order consistent in case json data changes
    allergens_reference_df = allergens_reference_df[['id', 'known', 'name', 'products', 'url', 'sameAs']]
    
    #Rename to less generic column names
    allergens_reference_df = allergens_reference_df.rename(columns={'id':'allergen_id', 
                                                            'name':'allergen_name', 
                                                            'sameAs':'same_as'})
    
    #Save to database
    allergens_reference_df.to_sql('allergens_reference',
                                  con=engine,
                                  schema='worldfood',
                                  index=False,
                                  if_exists='replace',
                                  dtype={'allergen_id': sa.types.VARCHAR(length=255),
                                         'known': sa.types.INTEGER(),
                                         'allergen_name': sa.types.VARCHAR(length=255),
                                         'products': sa.types.INTEGER(),
                                         'url': sa.types.VARCHAR(None),
                                         'same_as': sa.types.VARCHAR(None)})
    

def process_ingredients_reference(engine):
    
    #Pull ingredients metadata
    ingredients_reference = requests.get("https://us.openfoodfacts.org/ingredients.json").json()
    
    #Convert to dataframe
    ingredients_reference_df = pd.DataFrame(ingredients_reference['tags'])
    
    #Keep column order consistent in case json data changes
    ingredients_reference_df = ingredients_reference_df[['id', 'known', 'name', 'products', 'url', 'sameAs']]
    
    #Rename to less generic column names
    ingredients_reference_df = ingredients_reference_df.rename(columns={'id':'ingredient_id', 
                                                                'name':'ingredient_name', 
                                                                'sameAs':'same_as'})
    
    #Save to database
    ingredients_reference_df.to_sql('ingredients_reference',
                                  con=engine,
                                  schema='worldfood',
                                  index=False,
                                  if_exists='replace',
                                  dtype={'ingredient_id': sa.types.VARCHAR(length=255),
                                         'known': sa.types.INTEGER(),
                                         'ingredient_name': sa.types.VARCHAR(length=255),
                                         'products': sa.types.INTEGER(),
                                         'url': sa.types.VARCHAR(None),
                                         'same_as': sa.types.VARCHAR(None)})
    
    
def process_nutrients_reference(engine):
    
    #Pull nutrients metadata
    nutrients_reference = requests.get("https://us.openfoodfacts.org/cgi/nutrients.pl").json()
    
    #Convert to dataframe
    #For simplicity, ignore sub-nutrients
    nutrients_reference_df = pd.DataFrame(nutrients_reference['nutrients']).drop('nutrients',axis=1)
    
    #Keep column order consistent in case json data changes
    nutrients_reference_df = nutrients_reference_df[['id', 'important', 'display_in_edit_form', 'name']]
    
    #Rename to less generic column names
    nutrients_reference_df = nutrients_reference_df.rename(columns={'id':'inutrient_id', 
                                                                'name':'nutrient_name'})
    
    #Save to database
    nutrients_reference_df.to_sql('nutrients_reference',
                                  con=engine,
                                  schema='worldfood',
                                  index=False,
                                  if_exists='replace',
                                  dtype={'nutrient_id': sa.types.VARCHAR(length=255),
                                         'important': sa.types.BOOLEAN(),
                                         'display_in_edit_form': sa.types.BOOLEAN(),
                                         'name': sa.types.VARCHAR(length=255)})
    
    
def process_brands_reference(engine):
    
    #Pull nutrients metadata
    brands_reference = requests.get('https://us.openfoodfacts.org/brands.json').json()
    
    #Convert to dataframe
    #For simplicity, ignore sub-nutrients
    brands_reference_df = pd.DataFrame(brands_reference['tags'])
    
    #Keep column order consistent in case json data changes
    brands_reference_df = brands_reference_df[['id', 'known', 'name', 'products', 'url']]
    
    #Rename to less generic column names
    brands_reference_df = brands_reference_df.rename(columns={'id':'brand_id', 
                                                            'name':'brand_name'})
    
    #Save to database
    brands_reference_df.to_sql('brands_reference',
                                  con=engine,
                                  schema='worldfood',
                                  index=False,
                                  if_exists='replace',
                                  dtype={'brand_id': sa.types.VARCHAR(length=255),
                                         'known': sa.types.INTEGER(),
                                         'brand_name': sa.types.VARCHAR(length=255),
                                         'products': sa.types.INTEGER(),
                                         'url': sa.types.VARCHAR(None)})
    
    
    
def process_categories_reference(engine):
    
    #Pull nutrients metadata
    categories_reference = requests.get('https://us.openfoodfacts.org/categories.json').json()
    
    #Convert to dataframe
    categories_reference_df = pd.DataFrame(categories_reference['tags'])
    
    #Keep column order consistent in case json data changes
    categories_reference_df = categories_reference_df[['id', 'known', 'name', 'products', 'url']]
    
    #Rename to less generic column names
    categories_reference_df = categories_reference_df.rename(columns={'id':'category_id', 
                                                            'name':'category_name'})
    
    #Save to database
    categories_reference_df.to_sql('categories_reference',
                                  con=engine,
                                  schema='worldfood',
                                  index=False,
                                  if_exists='replace',
                                  dtype={'category_id': sa.types.VARCHAR(length=255),
                                         'known': sa.types.INTEGER(),
                                         'category_name': sa.types.VARCHAR(length=255),
                                         'products': sa.types.INTEGER(),
                                         'url': sa.types.VARCHAR(None)})
    
def main():
    engine=get_engine()
    process_allergens_reference(engine)
    print('Allergens processed')
    process_ingredients_reference(engine)
    print('Ingredients processed')
    process_nutrients_reference(engine)
    print('Nutrients processed')
    process_brands_reference(engine)
    print('Brands processed')
    process_categories_reference(engine)
    print('Categories processed')
    
    
if __name__ == "__main__":
    main()       
    
    
    
    
    
    
    
    
    