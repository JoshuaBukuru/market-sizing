
import pandas as pd
from utils.mappings import *
from utils.price_bands import get_IWSR_data_estimates
from utils.utils import *
from utils.price_bands import *
from utils.proportions import *
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

def result(current_year='2020', last_year='2019'):
    """Function to produce the estimates
        param current_year: current year of analysis
        param last_year: year before current year
        : return IWSR estimates for current year
    """

    # get starting point, which is income statement
    base_df = get_base_df(current_year)
    # Create a new column with last year's income volumes
    base_df['Income LY'] = transform_BIP_data(get_income_statement_data(last_year))
    base_df = base_df.reset_index()

    # get IWSR for previous year (most accurate estimate)
    base_df['IWSR LY'] = base_df['index'].apply(map_to_base_data, args=[get_IWSR_data(last_year), iwsr_mappings])

    # Estimate IWSR 2020 data using 2019 income-iwsr ratio per stats group and 2020 Income data
    base_df['IWSR Estimate'] = get_IWSR_estimate(base_df, iwsr_mappings, last_year)

    # Get estimates for brandy, gin, whisky, vodka and liqueurs from SALBA
    base_df['SALBA Estimate'] = get_SALBA_estimate(base_df, salba_mappings, current_year)
    base_df = base_df.set_index('index')

    base_df.loc['Liqueurs', 'SALBA Estimate'] += get_amarula_data(current_year)

    base_df = base_df.reset_index()

    # Get estimates for still, fortified, and sparkling wine from SAWIS
    base_df['SAWIS Estimate'] = get_SAWIS_estimate(base_df, sawis_mappings, current_year)

    # Get estimate for beer from GLOBAL data
    base_df['GLOBAL Estimate'] = get_GLOBAL_estimate(base_df, global_mappings, current_year)

    # Get data orbis estimate for brandy, gin, whisky, vodka, liqueurs, beer, all wines, Ciders & RTDS
    base_df['Data Orbis Estimate'] = get_data_orbis_estimate(base_df, data_orbis_mappings, iwsr_mappings,
                                                             current_year, last_year)

    # Get the adjusted average estimate (discard furthest data point and recalcuate mean)
    base_df['Avg Estimate'] = base_df.apply(get_adjusted_mean_estimate, axis=1)

    base_df = base_df.set_index('index')

    output_path = f'out\market_size_test{current_year}.csv'
    # base_df.loc[['Brandy', 'Gin', 'Vodka', 'Liqueurs', 'Whisky', 'Beer', 'Sparkling Wine', 'Wine Aperitif',
    #              'Fortified Wine 1', 'Still Wine', 'Fortified Wine 2', 'CIDER & RTDs', 'Ciders',
    #              'FABs']].to_csv(output_path)
    df = base_df.loc[['Brandy', 'Gin', 'Vodka', 'Liqueurs', 'Whisky', 'Beer', 'Sparkling Wine', 'Wine Aperitif',
                  'Fortified Wine 1', 'Still Wine', 'Fortified Wine 2', 'CIDER & RTDs', 'Ciders',
                  'FABs']].to_csv(output_path)
    return df

# def test_IWSR_estimates(current_year='2020', last_year='2019'):
#     """Function to compare current year IWSR estimates to actual IWSR data for current year
#
#         param current_year: current year of analysis
#         param last_year: year before current year
#         : return : difference between the two as errors
#     """
#     IWSR_df = get_IWSR_data_estimates(current_year)
#     result_df = result(current_year, last_year)
#     # fortified_aperitif_s = result_df.loc['Fortified Wine 1'] + result_df.loc['Fortified Wine 2']\
#     #                        + result_df.loc['Wine Aperitif']
#     # fortified_aperitif_s = (pd.DataFrame(data=fortified_aperitif_s, columns=['Fortified Wine & Wine Aperitifs'])).T
#     # result_df = pd.concat([result_df, fortified_aperitif_s])
#
#     brandy_df = abs((result_df.loc['Brandy']['Avg Estimate'] / IWSR_df.loc['Brandy']['Sales Volume']) - 1)
#     gin_df = abs((result_df.loc['Gin']['Avg Estimate'] / IWSR_df.loc['Gin and Genever']['Sales Volume']) - 1)
#     #cane_df = abs((result_df.loc['Brandy']['Avg Estimate'] / IWSR_df.loc['Brandy']['Sales Volume']) - 1)
#     vodka_df = abs((result_df.loc['Vodka']['Avg Estimate'] / IWSR_df.loc['Vodka']['Sales Volume']) - 1)
#     liqueurs_df = abs((result_df.loc['Liqueurs']['Avg Estimate'] / IWSR_df.loc['Liqueurs']['Sales Volume']) - 1)
#     whisky_df = abs((result_df.loc['Whisky']['Avg Estimate'] / IWSR_df.loc['Whisky']['Sales Volume']) - 1)
#     #rum_df = abs((result_df.loc['Rum']['Avg Estimate'] / IWSR_df.loc['Rum']['Sales Volume']) - 1)
#     #tequila_df = abs((result_df.loc['Brandy']['Avg Estimate'] / IWSR_df.loc['Brandy']['Sales Volume']) - 1)
#     spark_df = abs((result_df.loc['Sparkling Wine']['Avg Estimate'] / IWSR_df.loc['Sparkling Wine']['Sales Volume']) - 1)
#     still_df = abs((result_df.loc['Still Wine']['Avg Estimate'] / IWSR_df.loc['Still Wine']['Sales Volume']) - 1)
#     fort_df = abs((result_df.loc['Fortified Wine 2']['Avg Estimate'] / IWSR_df.loc['Fortified Wine & Wine Aperitifs']['Sales Volume']) - 1)
#     cider_fabs_df = abs((result_df.loc['CIDER & RTDs']['Avg Estimate'] / IWSR_df.loc['Cider & FABs']['Sales Volume']) - 1)
#     beer_df = abs((result_df.loc['Beer']['Avg Estimate'] / IWSR_df.loc['Beer']['Sales Volume']) - 1)
#
#     df = pd.DataFrame(data=[brandy_df, gin_df, vodka_df, liqueurs_df, whisky_df,
#                             spark_df, still_df, fort_df, cider_fabs_df, beer_df], columns=['Error of Estimates'],
#                       index=['Brandy', 'Gin', 'Vodka', 'Liqueurs', 'Whisky', 'Sparkling Wine', 'Still Wine',
#                              'Fortified Wine & Wine Aperitifs', 'Cider & FABs', 'Beer'])
#
#     return df

if __name__ == '__main__':

    # get estimates
    result(current_year='2020', last_year='2019')

    # get price bands
    price_band_conversions('2020', 'SALESVOLUME')

    # get fiscal year conversions
    fiscal_year_conversion('all_years')











