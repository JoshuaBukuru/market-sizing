
import pandas as pd
import numpy as np

from utils import *
from mappings import *

def get_base_df(current_year):
    """ Method to get starting point for estimating market size from income statement data for CY
    
    param current_year: current year (which is the last full calendar year experienced)
    : return: pandas dataframe with transformed income statement data
    """
    base_df = get_income_statement_data(current_year)
    base_df = transform_BIP_data(base_df)
    base_df = base_df.rename(columns = {'Volume': 'Income CY'})
    return base_df


def get_IWSR_estimate(base_df, iwsr_mappings, last_year):
    """ Method to get market estimate from IWSR LY data

    param base_df: dataframe containing income statement data and other estimates
    param iwsr_mappings: dict detailing mapping between IWSR and Income Statement stats groups
    param last_year: year prior to current year (which is the last full calendar year experienced)
    : return: pandas series containing market estimate for relevant stats groups with index compatible to base_df
    
    """

    iwsr_df = get_IWSR_data(last_year)

    # map IWSR data to base df 
    IWSR_LY = base_df['index'].apply(map_to_base_data, args = [iwsr_df, iwsr_mappings])

    # calculate income statement to IWSR ratio for LY data
    ratio_LY = base_df['Income LY']/IWSR_LY
    return base_df['Income CY']/ratio_LY


def get_SALBA_estimate(base_df, salba_mappings, current_year):
    """ Method to get market estimate from SALBA CY data

    param base_df: dataframe containing income statement data and other estimates
    param salba_mappings: dict detailing mapping between SALBA and Income Statement stats groups
    param current_year: current year (which is the last full calendar year experienced)
    : return: pandas series containing market estimate for relevant stats groups with index compatible to base_df
    
    """

    salba_df = get_SALBA_data(current_year)
    salba_df = transform_SALBA_df(salba_df)
    return base_df['index'].apply(map_to_base_data, args = [salba_df, salba_mappings]) 


def get_SAWIS_estimate(base_df, sawis_mappings, current_year):
    """ Method to get market estimate from SAWIS CY data

    param base_df: dataframe containing income statement data and other estimates
    param sawis_mapping: dict detailing mapping between SAWIS and Income Statement stats groups
    param current_year: current year (which is the last full calendar year experienced)
    : return: pandas series containing market estimate for relevant stats groups with index compatible to base_df
    
    """

    sawis_df = get_SAWIS_data(current_year)
    return base_df['index'].apply(map_to_base_data, args = [sawis_df, sawis_mappings])


def get_GLOBAL_estimate(base_df, global_mappings, current_year):
    """ Method to get market estimate from GLOBAL CY data

    param base_df: dataframe containing income statement data and other estimates
    param global_mappings: dict detailing mapping between GLOBAL and Income Statement stats groups
    param current_year: current year (which is the last full calendar year experienced)
    : return: pandas series containing market estimate for relevant stats groups with index compatible to base_df
    
    """

    global_df = get_global_data(current_year)
    return base_df['index'].apply(map_to_base_data, args = [global_df, global_mappings])


def get_data_orbis_estimate(base_df, data_orbis_mappings, iwsr_mappings, current_year = '2020', last_year = '2019'):

    """ Method to get market estimate from data orbis data.
    It assumed that Data Orbis represents only about 30-40% of total domestic alcohol sales. 
    To estimate the market size from Data Orbis, we look at what proportion each stats group was of the IWSR data in LY.
    We use that ratio to scale the current year Data Orbis volumes per stats group
    
    param base_df: dataframe containing income statement data and other estimates
    param epos_mapping: dict detailing mapping between Data Orbis and Income Statement stats groups
    param iwsr_mapping: dict detailing mapping between IWSR and Income Statement stats groups
    param current_year: current year (which is the last full calendar year experienced)
    : return: pandas series containing market estimate for relevant stats groups with index compatible to base_df
    
    """
    iwsr_df = get_IWSR_data(last_year)
    IWSR_LY = base_df['index'].apply(map_to_base_data, args = [iwsr_df, iwsr_mappings])

    df_LY = get_data_orbis(last_year)
    df_CY = get_data_orbis(current_year)

    df_LY = transform_data_orbis(df_LY)
    df_CY = transform_data_orbis(df_CY)

    df_LY['Volume'] = df_LY['Volume']*(12/11) # to account for missing january data in 2019 

    data_orbis_LY = base_df['index'].apply(map_to_base_data, args = [df_LY, data_orbis_mappings]) 
    data_orbis_CY = base_df['index'].apply(map_to_base_data, args = [df_CY, data_orbis_mappings]) 

    # calculate ratio of data orbis volume to IWSR volume for last year
    ratio_LY = data_orbis_LY/IWSR_LY
    return data_orbis_CY/ratio_LY


print('Hello')

if __name__ == '__main__':

    current_year = '2020'
    last_year = '2019'

    # get starting point, which is income statement
    base_df = get_base_df(current_year)
    #Create a new column with last year's income volumes
    base_df['Income LY'] = transform_BIP_data(get_income_statement_data(last_year))
    base_df = base_df.reset_index()


    # get IWSR for previous year (most accurate estimate)
    base_df['IWSR LY'] = base_df['index'].apply(map_to_base_data, args = [get_IWSR_data(last_year), iwsr_mappings])

    # Estimate IWSR 2020 data using 2019 income-iwsr ratio per stats group and 2020 Income data
    base_df['IWSR Estimate'] = get_IWSR_estimate(base_df, iwsr_mappings, last_year)

    # Get estimates for brandy, gin, whisky, vodka and liqueurs from SALBA
    base_df['SALBA Estimate'] = get_SALBA_estimate(base_df, salba_mappings, current_year)
    base_df = base_df.set_index('index')

    base_df.loc['Liqueurs', 'SALBA Estimate'] +=  get_amarula_data(current_year)

    base_df = base_df.reset_index()

    # Get estimates for still, fortified, and sparkling wine from SAWIS
    base_df['SAWIS Estimate'] = get_SAWIS_estimate(base_df, sawis_mappings, current_year)

    # Get estimate for beer from GLOBAL data
    base_df['GLOBAL Estimate'] = get_GLOBAL_estimate(base_df, global_mappings, current_year)

    # Get data orbis estimate for brandy, gin, whisky, vodka, liqueurs, beer, all wines, Ciders & RTDS
    base_df['Data Orbis Estimate'] = get_data_orbis_estimate(base_df, data_orbis_mappings, iwsr_mappings, current_year, last_year)

    # Get the adjusted average estimate (discard furthest data point and recalcuate mean)
    base_df['Avg Estimate'] = base_df.apply(get_adjusted_mean_estimate, axis = 1)

    base_df = base_df.set_index('index')

    output_path = f'out\market_size_{current_year}.csv'
    base_df.loc[['Brandy', 'Gin', 'Vodka', 'Liqueurs', 'Whisky', 'Beer', 'Sparkling Wine', 'Wine Aperitif', 
           'Fortified Wine 1', 'Still Wine', 'Fortified Wine 2', 'CIDER & RTDs', 'Ciders',
           'FABs']].to_csv(output_path)










