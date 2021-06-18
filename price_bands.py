import pandas as pd
import numpy as np
from utils import *
from mappings import *

def get_wine_price_band(year):
    """" Function to read in and preprocess the Data Orbis file and split the data's sub categories
        into price bands for wines

        param year: year of data analysis
        : return: data frame of price band splits per sub category
        """
    base_dir = DATA_DIRECTORY / 'data_orbis' / year / ''
    path = get_file_in_directory(base_dir)

    df = pd.read_excel(path, sheet_name='Sheet1')
    df = df[df['COUNTRYNAME'] == 'South Africa']
    still_df_1 = df[df['PRODUCTSUBCATEGORY'] == 'Unfortified'][
        ['PRODUCTSUBCATEGORY', 'SALESVOLUME', 'PRICE', 'SALESQUANTITY']]
    still_df_2 = df[df['PRODUCTSUBCATEGORY'] == 'BIB'][
        ['PRODUCTSUBCATEGORY', 'SALESVOLUME', 'PRICE', 'SALESQUANTITY']]
    still_df_3 = df[df['PRODUCTSUBCATEGORY'] == 'Perle'][
        ['PRODUCTSUBCATEGORY', 'SALESVOLUME', 'PRICE', 'SALESQUANTITY']]
    spark_df = df[df['PRODUCTSUBCATEGORY'] == 'Sparkling'][
        ['PRODUCTSUBCATEGORY', 'SALESVOLUME', 'PRICE', 'SALESQUANTITY']]
    fort_df = df[df['PRODUCTSUBCATEGORY'] == 'Fortified'][
        ['PRODUCTSUBCATEGORY', 'SALESVOLUME', 'PRICE', 'SALESQUANTITY']]

    frames = [still_df_1, still_df_2, still_df_3]
    still_df = pd.concat(frames)
    #Convert unfortified, BIB and Perle to still wines sub category
    still_df['PRODUCTSUBCATEGORY'] = still_df['PRODUCTSUBCATEGORY'].apply(lambda x: 'Still')

    frames = [still_df, spark_df, fort_df]
    df = pd.concat(frames)

    df['Price_per_subcategory'] = df['PRICE'] / df['SALESQUANTITY']
    df['Price_band'] = df['Price_per_subcategory'].apply(price_band_conversion)
    df = df.groupby(['PRODUCTSUBCATEGORY', 'Price_band']).agg('sum')[['SALESVOLUME']]

    still_df = ((df.T.Still).T).rename(columns={'SALESVOLUME': 'Still Wine'})
    spark_df = ((df.T.Sparkling).T).rename(columns={'SALESVOLUME': 'Sparkling Wine'})
    fort_df = ((df.T.Fortified).T).rename(columns={'SALESVOLUME': 'Fortified Wine'})

    still_df = still_df.apply(lambda x: x / (still_df['Still Wine'].sum()))
    spark_df = spark_df.apply(lambda x: x / (spark_df['Sparkling Wine'].sum()))
    fort_df = fort_df.apply(lambda x: x / (fort_df['Fortified Wine'].sum()))

    df = pd.concat([still_df, spark_df, fort_df], axis=1)

    return df

def get_beer_price_band(year):
    """" Function to read in and preprocess the Data Orbis file and split the data's sub categories
        into price bands for beer

        param year: year of data analysis
        : return: data frame of price band splits per sub category
        """
    base_dir = DATA_DIRECTORY / 'data_orbis' / year / ''
    path = get_file_in_directory(base_dir)

    df = pd.read_excel(path, sheet_name='Sheet1')
    df = df[df['COUNTRYNAME'] == 'South Africa']

    beer_df = df[df['PRODUCTCATEGORY'] == 'Beer']
    rtd_df = df[df['PRODUCTCATEGORY'] == 'Rtds']
    non_alcoholic_df_1 =  beer_df[beer_df['PRODUCTSUBCATEGORY'] == 'Non-Alcoholic']
    non_alcoholic_df_2 = rtd_df[rtd_df['PRODUCTSUBCATEGORY'] == 'Non-Alcoholic']
    non_alcoholic_df = pd.concat([non_alcoholic_df_1,non_alcoholic_df_2])
    non_alcoholic_df['PRODUCTCATEGORY'] = non_alcoholic_df['PRODUCTCATEGORY'].apply(lambda x: 'NonAlcoholic')

    beer_df = beer_df[beer_df['PRODUCTSUBCATEGORY'] != 'Non-Alcoholic']
    rtd_df = rtd_df[rtd_df['PRODUCTSUBCATEGORY'] != 'Non-Alcoholic']
    df = pd.concat([beer_df, rtd_df, non_alcoholic_df])
    df['Price_per_subcategory'] = df['PRICE'] / df['SALESQUANTITY']
    df['Price_band'] = df['Price_per_subcategory'].apply(price_band_conversion)

    df = df.groupby(['PRODUCTCATEGORY', 'Price_band']).agg('sum')[['SALESVOLUME']]

    beer_df = ((df.T.Beer).T).rename(columns={'SALESVOLUME':'Beer'})
    rtd_df = ((df.T.Rtds).T).rename(columns={'SALESVOLUME': 'Cider and Fabs'})
    non_alcoholic_df = ((df.T.NonAlcoholic).T).rename(columns={'SALESVOLUME': 'Non-Alcoholic'})

    beer_df = beer_df.apply(lambda x: x / (beer_df['Beer'].sum()))
    rtd_df = rtd_df.apply(lambda x: x / (rtd_df['Cider and Fabs'].sum()))
    non_alcoholic_df = non_alcoholic_df.apply(lambda x: x / (non_alcoholic_df['Non-Alcoholic'].sum()))

    df = pd.concat([beer_df, rtd_df, non_alcoholic_df], axis=1)

    return df

#%%
df = get_beer_price_band('2020')
#%%
def get_spirits_price_band(year):
    """" Function to read in and preprocess the Data Orbis file and split the data's sub categories
    into price bands for spirits

    param year: year of data analysis
    : return: data frame of price band splits per sub category
    """

    base_dir = DATA_DIRECTORY / 'data_orbis' / year / ''
    path = get_file_in_directory(base_dir)

    df = pd.read_excel(path, sheet_name='Sheet1')
    df = df[df['COUNTRYNAME'] == 'South Africa']
    #df = df.reset_index()
    #df.drop('index',
    #        axis='columns', inplace=True)
    data = df
    brandy_df = df[df['PRODUCTSUBCATEGORY'] == 'Brandy'][['PRODUCTSUBCATEGORY','SALESVOLUME','PRICE','SALESQUANTITY']]
    cane_df = df[df['PRODUCTSUBCATEGORY'] == 'Cane'][['PRODUCTSUBCATEGORY','SALESVOLUME','PRICE','SALESQUANTITY']]
    cognac_df = df[df['PRODUCTSUBCATEGORY'] == 'Cognac'][['PRODUCTSUBCATEGORY','SALESVOLUME','PRICE','SALESQUANTITY']]
    gin_df = df[df['PRODUCTSUBCATEGORY'] == 'Gin'][['PRODUCTSUBCATEGORY','SALESVOLUME','PRICE','SALESQUANTITY']]
    liqueurs_df = df[df['PRODUCTSUBCATEGORY'] == 'Liqueurs'][
        ['PRODUCTSUBCATEGORY','SALESVOLUME','PRICE','SALESQUANTITY']]
    rum_df = df[df['PRODUCTSUBCATEGORY'] == 'Rum'][['PRODUCTSUBCATEGORY','SALESVOLUME','PRICE','SALESQUANTITY']]
    vodka_df = df[df['PRODUCTSUBCATEGORY'] == 'Vodka'][['PRODUCTSUBCATEGORY','SALESVOLUME','PRICE','SALESQUANTITY']]
    whisky_df = df[df['PRODUCTSUBCATEGORY'] == 'Whisky'][['PRODUCTSUBCATEGORY','SALESVOLUME','PRICE','SALESQUANTITY']]
    other_spirits_df = df[df['PRODUCTSUBCATEGORY'] == 'Other Spirits'][
        ['PRODUCTSUBCATEGORY','SALESVOLUME','PRICE','SALESQUANTITY']]

    frames = [brandy_df, cane_df, cognac_df, gin_df, liqueurs_df, rum_df, vodka_df, whisky_df, other_spirits_df]
    df = pd.concat(frames)
    #price_bands = {'Low Price': [0, 100],
    #              'Value': [100, 150],
    #              'Accessible Premium': [150, 200],
    #              'Premium': [200, 300],
    #              'Super Premium': [300, 400],
    #              'Ultra Premium': [400]}
    df['Price_per_subcategory'] = df['PRICE'] / df['SALESQUANTITY']
    df['Price_band'] = df['Price_per_subcategory'].apply(price_band_conversion)

    df = df.groupby(['PRODUCTSUBCATEGORY', 'Price_band']).agg('sum')[['SALESVOLUME']]
    brandy_df = ((df.T.Brandy).T).rename(columns={'SALESVOLUME':'Brandy'})
    cane_df = ((df.T.Cane).T).rename(columns={'SALESVOLUME': 'Cane'})
    cognac_df = ((df.T.Cognac).T).rename(columns={'SALESVOLUME': 'Cognac'})
    gin_df = ((df.T.Gin).T).rename(columns={'SALESVOLUME': 'Gin'})
    liqueurs_df = ((df.T.Liqueurs).T).rename(columns={'SALESVOLUME': 'Liqueurs'})
    rum_df = ((df.T.Rum).T).rename(columns={'SALESVOLUME': 'Rum'})
    vodka_df = ((df.T.Vodka).T).rename(columns={'SALESVOLUME': 'Vodka'})
    whisky_df = ((df.T.Whisky).T).rename(columns={'SALESVOLUME': 'Whisky'})
    #other_spirits_df = ((df.T.).T).rename(columns={'SALESVOLUME': 'Brandy'})
    #frames = [brandy_df, cane_df, cognac_df, gin_df, liqueurs_df, rum_df, vodka_df, whisky_df]
    #df = pd.concat(frames, axis=1)

    brandy_df = brandy_df.apply(lambda x: x / (brandy_df['Brandy'].sum()))
    cane_df = cane_df.apply(lambda x: x / (cane_df['Cane'].sum()))
    cognac_df = cognac_df.apply(lambda x: x / (cognac_df['Cognac'].sum()))
    gin_df = gin_df.apply(lambda x: x / (gin_df['Gin'].sum()))
    liqueurs_df = liqueurs_df.apply(lambda x: x / (liqueurs_df['Liqueurs'].sum()))
    rum_df = rum_df.apply(lambda x: x / (rum_df['Rum'].sum()))
    vodka_df = vodka_df.apply(lambda x: x / (vodka_df['Vodka'].sum()))
    whisky_df = whisky_df.apply(lambda x: x / (whisky_df['Whisky'].sum()))

    frames = [brandy_df, cane_df, cognac_df, gin_df, liqueurs_df, rum_df, vodka_df, whisky_df]
    df = pd.concat(frames, axis=1)
    return data

def price_band_conversion(x):
    """" Function to convert the subcategories in the data orbis dataset to
    price bands

        param x: dataset
        : return: price band
        """
    if x > 0 and x <= 100:
        return 'Low Price'
    elif x > 100 and x <= 150:
        return 'Value'
    elif x > 150 and x <= 200:
        return 'Accessible Premium'
    elif x > 200 and x <= 300:
        return 'Premium'
    elif x > 300 and x <= 400:
        return 'Super Premium'
    elif x > 400:
        return 'Ultra Premium'



#%%
df = get_spirits_price_band('2020')
#%%
counts = df['PRODUCTSUBCATEGORY'].value_counts()
#%%
S=df.groupby(['PRODUCTCATEGORY','PRODUCTSUBCATEGORY']).agg('sum')[['SALESVOLUME']]
#%%
df_prem = df[df['PRODUCTSEGMENT'] == 'Mainstream']['PRODUCTSUBCATEGORY'].value_counts()
#%%
df_prem['PRODUCTCATEGORY'].value_counts()
