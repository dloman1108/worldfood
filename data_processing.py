#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Dec  9 20:15:26 2021

@author: dh08loma
"""

import numpy as np
import pandas as pd
import sqlalchemy as sa 
import os
import yaml


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

	
#Function to process allergens aggregate query and store in EDW,
#for use in downstream dashboard
def write_allergens_agg(engine):

    query = open('SQL/allergens_agg.sql').read()

    allergens_agg_df=pd.read_sql(query,engine)

    allergens_agg_df.to_sql('allergens_agg',
    						 con=engine,
    						 schema='worldfood',
    						 index=False,
    						 if_exists='replace',
    						 dtype={'allergen_id': sa.types.VARCHAR(length=255),
    								'allergen_name': sa.types.VARCHAR(length=255),
    								'num_products': sa.types.INTEGER(),
                                    'pct_products': sa.types.FLOAT()})

#Function to process ingredients aggregate query and store in EDW,
#for use in downstream dashboard
def write_ingredients_agg(engine):

    query = open('SQL/ingredients_agg.sql').read()

    ingredients_agg_df=pd.read_sql(query,engine)
    
    ingredients_agg_df.to_sql('ingredients_agg',
    						 con=engine,
    						 schema='worldfood',
    						 index=False,
    						 if_exists='replace',
    						 dtype={'ingredient_id': sa.types.VARCHAR(length=255),
    								'ingredient_name': sa.types.VARCHAR(length=255),
    								'num_products': sa.types.INTEGER(),
                                    'pct_products': sa.types.FLOAT()})

	
#Function to process nutrients aggregate query and store in EDW,
#for use in downstream dashboard
def write_nutrients_agg(engine):

    query = open('SQL/nutrients_agg.sql').read()
    
    nutrients_agg_df=pd.read_sql(query,engine)

    nutrients_agg_df.to_sql('nutrients_agg',
    						 con=engine,
    						 schema='worldfood',
    						 index=False,
    						 if_exists='replace',
    						 dtype={'product_id': sa.types.BIGINT(),
    								'product_name': sa.types.VARCHAR(length=255),
                                    'carbs_per_serving': sa.types.FLOAT(),
                                    'carbs_per_serving_percentile': sa.types.FLOAT(),
                                    'carbs_per_serving_indicator': sa.types.CHAR(4),
                                    'protein_per_serving': sa.types.FLOAT(),
                                    'protein_per_serving_percentile': sa.types.FLOAT(),
                                    'protein_per_serving_indicator': sa.types.CHAR(4),
                                    'calories_per_serving': sa.types.FLOAT(),
                                    'calories_per_serving_percentile': sa.types.FLOAT(),
                                    'calories_per_serving_indicator': sa.types.CHAR(4)})
    
    
def main():
    engine=get_engine()
    write_allergens_agg(engine)
    write_ingredients_agg(engine)
    write_nutrients_agg(engine)
    
	
	
if __name__ == "__main__":
	main()


