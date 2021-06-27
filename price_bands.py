import pandas as pd
import numpy as np
from utils import *
from mappings import *
import re
def get_product_category(year, product_category):
    """" Function to get the data and filter by product category

        : param year: year of data analysis
        : param product_category: product category
        : param x: x is the pack size
        : return: data frame filtered by product category
        """
    base_dir = DATA_DIRECTORY / 'data_orbis_low_level'
    path = get_file_in_directory(base_dir)

    df = pd.read_excel(path, sheet_name='Sheet1')
    # Filtered by country name
    df = df[df['COUNTRYNAME'] == 'South Africa']

    # filtered by year
    df['Realigned YYYYMM'] = df['Realigned YYYYMM'].apply(lambda x: str(x)[0:4])
    df = df[df['Realigned YYYYMM'] == year]

    # Filtered by Product category for wine
    df = df[df['PRODUCTCATEGORY'] == product_category]

    # Rename Low-Alchol to Low_Alcohol and No-Alcohol to No_Alcohol
    df['INDEX'] = df['INDEX'].apply(lambda x: 'Low_Alcohol' if x == 'Low-Alcohol' else
    'No_Alcohol' if x == 'No-Alcohol' else
    'Energy' if x == 'Energy' else
    'Alcohol')


    return df

def get_wine_price_band(year):
    """Function to read in and preprocess the Data Orbis file and split the data's sub categories
        into price bands for wines

    : param year: year of data analysis
    : return: data frame of price band splits per sub category
    """
    df = get_product_category(year, 'Wine')

    # Create a price per subcategory column to get a unit price for every subcategory
    df['Price_per_subcategory'] = (df['PRICE'] / df['SALESQUANTITY'])

    #Seperate the data by Still wine, aperitif, fortified wine, sparkling wine
    aperitif_wine_df = df[df['PRODUCTSUBCATEGORY'] == 'Aperitif']
    fotified_wine_df = df[df['PRODUCTSUBCATEGORY'] == 'Fortified']
    sparkling_wine_df = df[df['PRODUCTSUBCATEGORY'] == 'Sparkling']
    still_wine_df = df[df['PRODUCTSUBCATEGORY'] == 'Still wine']
    still_wine_df['PRODUCTSUBCATEGORY'] = df['PRODUCTSUBCATEGORY'].apply(lambda x: 'Unfortified')


    # Create price band column to classify the subcategory (low price, value, premium, etc)
    aperitif_wine_df['Price_band'] = aperitif_wine_df['Price_per_subcategory'].apply(
        price_band_wine_conversion, args=['Still Wine'])
    sparkling_wine_df['Price_band'] = sparkling_wine_df['Price_per_subcategory'].apply(
        price_band_wine_conversion, args=['Sparkling Wine'])
    fotified_wine_df['Price_band'] = fotified_wine_df['Price_per_subcategory'].apply(
        price_band_wine_conversion, args=['Still Wine'])
    still_wine_df['Price_band'] = still_wine_df['Price_per_subcategory'].apply(
        price_band_wine_conversion, args=['Still Wine'])
    df = pd.concat([aperitif_wine_df, still_wine_df, sparkling_wine_df, fotified_wine_df])

    # df = df.groupby(['PRODUCTSUBCATEGORY', 'Price_band']).agg('sum')[['SALESVOLUME']]

    df = df.groupby(['PRODUCTSUBCATEGORY', 'INDEX', 'Price_band']).agg('sum')[['SALESVOLUME']]
    # Get the aggregated subcategories and apply classifier (low alc, no alc, etc) and rename columns
    aperitif_df = (df.T.Aperitif)
    df_mod = pd.DataFrame(data=np.zeros(6), columns=['Zeros'], index=['Accessible Premium',
                                                                    'Low Price',
                                                                    'Premium',
                                                                    'Super Premium',
                                                                    'Ultra Premium',
                                                                    'Affordable',
                                                                    'Value'])
    df_mod = alcohol_type_classifier('Aperitif', aperitif_df, df_mod)
    df_mod.drop('Zeros', axis='columns', inplace=True)
    fortified_df = (df.T.Fortified)
    df_mod = alcohol_type_classifier('Fortified_Wine', fortified_df, df_mod)
    sparkling_df = (df.T.Sparkling)
    df_mod = alcohol_type_classifier('Sparkling_Wine', sparkling_df, df_mod)
    still_df = (df.T.Unfortified)
    df_mod = alcohol_type_classifier('Still_Wine', still_df, df_mod)

    # calculate proportions of all subcategories
    for col in df_mod.columns:
        df_mod[col] = df_mod[col].apply(lambda x: x / (df_mod[col].sum()))

    df_mod = df_mod.fillna(0)

    return df_mod

def alcohol_type_classifier(name, category, df_mod):
    """" Function to classify the dataset as either low alcohol, no alcohol, alchol and energy

    : param name: name of subcategory
    : param category: subcategory data
    : param df_mod: modified dataframe with classifiers
    : return: data frame of price band splits per sub category
    """
    if 'Alcohol' in category:
        df_mod[name + '_Alcohol'] = category.Alcohol.T
    if 'Low_Alcohol' in category:
        df_mod[name + '_Low_Alcohol'] = category.Low_Alcohol.T
    if 'No_Alcohol' in category:
        df_mod[name + '_No_Alcohol'] = category.No_Alcohol.T
    if 'Energy' in category:
        df_mod[name + '_Energy'] = category.Energy.T

    return df_mod


def get_beer_price_band(year):
    """" Function to read in and preprocess the Data Orbis file and split the data's sub categories
        into price bands for beers

        param year: year of data analysis
        : return: data frame of price band splits per sub category
        """
    df = get_product_category(year, 'Beer')
    df = convert_product_description_beer_and_rtds(df)

    # # Aggregate by subcategory, alcohol presence and price band
    df = df.groupby(['PRODUCTSUBCATEGORY', 'INDEX', 'Price_band']).agg('sum')[['SALESVOLUME']]
    # #
    # Get the aggregated subcategories and apply classifier (low alc, no alc, etc) and rename columns
    beer_df = df.T.Beer
    df_mod = pd.DataFrame(data=np.zeros(6), columns=['Zeros'], index=['Accessible Premium',
                                                                      'Low Price',
                                                                      'Premium',
                                                                      'Super Premium',
                                                                      'Ultra Premium',
                                                                      'Affordable'])
    df_mod = alcohol_type_classifier('Beer', beer_df, df_mod)
    df_mod.drop('Zeros', axis='columns', inplace=True)

    # calculate proportions of all subcategories
    for col in df_mod.columns:
        df_mod[col] = df_mod[col].apply(lambda x: x / (df_mod[col].sum()))

    df_mod = df_mod.fillna(0)

    return df_mod

def get_Rtds_price_band(year):
    """Function to read in and preprocess the Data Orbis file and split the data's sub categories
        into price bands for RTDs

        : param year: year of data analysis
        : return: data frame of price band splits per sub category
        """
    df = get_product_category(year, 'Rtds')
    df = convert_product_description_beer_and_rtds(df)

    # # Aggregate by subcategory, alcohol presence and price band
    df = df.groupby(['PRODUCTSUBCATEGORY', 'INDEX', 'Price_band']).agg('sum')[['SALESVOLUME']]
    # #
    # Get the aggregated subcategories and apply classifier (low alc, no alc, etc) and rename columns
    cider_df = df.T.Cider
    df_mod = pd.DataFrame(data=np.zeros(6), columns=['Zeros'], index=['Accessible Premium',
                                                                      'Low Price',
                                                                      'Premium',
                                                                      'Super Premium',
                                                                      'Ultra Premium',
                                                                      'Affordable'])
    df_mod = alcohol_type_classifier('Cider', cider_df, df_mod)
    df_mod.drop('Zeros', axis='columns', inplace=True)
    fabs_df = df.T.Fabs
    df_mod = alcohol_type_classifier('Fabs', fabs_df, df_mod)

    # calculate proportions of all subcategories
    for col in df_mod.columns:
        df_mod[col] = df_mod[col].apply(lambda x: x / (df_mod[col].sum()))

    df_mod = df_mod.fillna(0)

    return df_mod
#%%
df = get_beer_price_band('2020')

#%%
df = get_wine_price_band('2020')
#%%

#%%

#sd = df['PRODUCTDESCRIPTION'].value_counts()
#%%

#%%
def convert_product_description_beer_and_rtds(df):
    """..."""
    df['PRODUCTDESCRIPTION'] = df['PRODUCTDESCRIPTION'].apply(lambda x: re.split('\s', x)[-1])
    df['PRODUCTDESCRIPTION'] = df['PRODUCTDESCRIPTION'].apply(lambda x: '330ml'
        if search_product_description('330', x) or
        search_product_description('340', x) or
        search_product_description('300', x) or
        search_product_description('275', x) or
        search_product_description('250', x) or
        search_product_description('200', x) or
        search_product_description('100', x) or
        search_product_description('375', x) else
        '500ml'
        if search_product_description('500', x) or
        search_product_description('440', x) else
        '660ml'
        if search_product_description('660', x) or
        search_product_description('750', x) or
        search_product_description('30l', x) or
        search_product_description('2l', x) or
        search_product_description('1l', x) or
        search_product_description('5l', x) or
        search_product_description('700', x) else '500ml')

    df_330ml = df[df["PRODUCTDESCRIPTION"] == "330ml"]
    df_500ml = df[df["PRODUCTDESCRIPTION"] == "500ml"]
    df_660ml = df[df["PRODUCTDESCRIPTION"] == "660ml"]

    # Create a price per subcategory column to get a unit price for every subcategory
    df_330ml['Price_per_subcategory'] = (df_330ml['PRICE'] / df_330ml['SALESQUANTITY']) * 6
    df_500ml['Price_per_subcategory'] = (df_500ml['PRICE'] / df_500ml['SALESQUANTITY']) * 6
    df_660ml['Price_per_subcategory'] = (df_660ml['PRICE'] / df_660ml['SALESQUANTITY']) * 12

    # Apply price band conversion and price column
    df_330ml['Price_band'] = df_330ml['Price_per_subcategory'].apply(price_band_beer_conversion, args=['330ml'])
    df_500ml['Price_band'] = df_500ml['Price_per_subcategory'].apply(price_band_beer_conversion, args=['500ml'])
    df_660ml['Price_band'] = df_660ml['Price_per_subcategory'].apply(price_band_beer_conversion, args=['660ml'])

    df = pd.concat([df_330ml, df_500ml, df_660ml])

    return df

def search_product_description(search, txt):
    """..."""
    x = re.search(search, txt)
    if x == None:
        return False
    else:
        if x.group() == search:
            return True
        else:
            return False

def get_spirits_price_band(year):
    """Function to read in and preprocess the Data Orbis file and split the data's sub categories
        into price bands for wines

    : param year: year of data analysis
    : return: data frame of price band splits per sub category
    """
    df = get_product_category(year, 'Spirits')

    # Create a price per subcategory column to get a unit price for every subcategory
    df['Price_per_subcategory'] = (df['PRICE'] / df['SALESQUANTITY'])

    # Apply price and column
    df['Price_band'] = df['Price_per_subcategory'].apply(
        price_band_spirit_conversion)

    # Aggregate by subcategory, alcohol presence and price band
    df = df.groupby(['PRODUCTSUBCATEGORY', 'INDEX', 'Price_band']).agg('sum')[['SALESVOLUME']]

    # Get the aggregated subcategories and apply classifier (low alc, no alc, etc) and rename columns
    brandy_df = df.T.Brandy
    df_mod = pd.DataFrame(data=np.zeros(6), columns=['Zeros'], index=['Accessible Premium',
                                                                      'Low Price',
                                                                      'Premium',
                                                                      'Super Premium',
                                                                      'Ultra Premium',
                                                                      'Value'])
    df_mod = alcohol_type_classifier('Brandy', brandy_df, df_mod)
    df_mod.drop('Zeros', axis='columns', inplace=True)
    cane_df = df.T.Cane
    df_mod = alcohol_type_classifier('Cane', cane_df, df_mod)
    cognac_df = df.T.Cognac
    df_mod = alcohol_type_classifier('Cognac', cognac_df, df_mod)
    gin_df = df.T.Gin
    df_mod = alcohol_type_classifier('Gin', gin_df, df_mod)
    liqueurs_df = df.T.Liqueurs
    df_mod = alcohol_type_classifier('Liqueurs', liqueurs_df, df_mod)
    rum_df = df.T.Rum
    df_mod = alcohol_type_classifier('Rum', rum_df, df_mod)
    tequila_df = df.T.Tequila
    df_mod = alcohol_type_classifier('Tequila', tequila_df, df_mod)
    vodka_df = df.T.Vodka
    df_mod = alcohol_type_classifier('Vodka', vodka_df, df_mod)
    whisky_df = df.T.Whisky
    df_mod = alcohol_type_classifier('Whisky', whisky_df, df_mod)

    # calculate proportions of all subcategories
    for col in df_mod.columns:
        df_mod[col] = df_mod[col].apply(lambda x: x / (df_mod[col].sum()))

    df_mod = df_mod.fillna(0)

    return df_mod

def price_band_spirit_conversion(x):
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
def price_band_wine_conversion(x, category):
    """" Function to convert the subcategories in the data orbis dataset to
    price bands

    : param x: dataset
    : param category: type of wine (fortified, still, etc)
    : return: price band
    """
    if category == 'Still Wine':
        if x > 0 and x <= 30:
            return 'Low Price'
        elif x > 30 and x <= 40:
            return 'Affordable'
        elif x > 40 and x <= 60:
            return 'Value'
        elif x > 60 and x <= 85:
            return 'Accessible Premium'
        elif x > 85 and x <= 120:
            return 'Premium'
        elif x > 120 and x <= 300:
            return 'Super Premium'
        elif x > 300:
            return 'Ultra Premium'

    if category == 'Sparkling Wine':
        if x > 0 and x <= 80:
            return 'Value'
        elif x > 80 and x <= 120:
            return 'Accessible Premium'
        elif x > 120 and x <= 200:
            return 'Premium'
        elif x > 200 and x <= 400:
            return 'Super Premium'
        elif x > 400:
            return 'Ultra Premium'

def price_band_beer_conversion(x, category):
    """" Function to convert the subcategories in the data orbis dataset to
    price bands

    : param x: dataset
    : param category: type of wine (fortified, still, etc)
    : return: price band
    """
    if category == '330ml':
        if x > 0 and x <= 60:
            return 'Low Price'
        elif x > 60 and x <= 70:
            return 'Affordable'
        elif x > 70 and x <= 80:
            return 'Accessible Premium'
        elif x > 80 and x <= 90:
            return 'Premium'
        elif x > 90 and x <= 110:
            return 'Super Premium'
        elif x > 110:
            return 'Ultra Premium'

    if category == '500ml':
        if x > 0 and x <= 85:
            return 'Low Price'
        elif x > 85 and x <= 100:
            return 'Affordable'
        elif x > 100 and x <= 115:
            return 'Accessible Premium'
        elif x > 115 and x <= 130:
            return 'Premium'
        elif x > 130 and x <= 150:
            return 'Super Premium'
        elif x > 150:
            return 'Ultra Premium'

    if category == '660ml':
        if x > 0 and x <= 200:
            return 'Low Price'
        elif x > 200 and x <= 240:
            return 'Affordable'
        elif x > 240 and x <= 250:
            return 'Accessible Premium'
        elif x > 250 and x <= 260:
            return 'Premium'
        elif x > 260 and x <= 280:
            return 'Super Premium'
        elif x > 280:
            return 'Ultra Premium'


def get_IWSR_data_estimates(year):
    """"
       Function to read in and preprocess the IWSR file

       param year:
       : return: preprocessed df with IWSR data with stats group as index
       """

    base_dir = DATA_DIRECTORY / 'IWSR' / year / ''
    path = get_file_in_directory(base_dir)

    df = pd.read_excel(path, sheet_name='REVISED', skiprows=2)
    df_name = df['R Million / Million Litres']
    df = df.iloc[:16, 9:12]
    sales_volume_df = df['Sales Volumes.2'] * 1000000
    df['Sales Volume'] = sales_volume_df
    df['Index'] = df_name
    df = df.set_index(['Index'])

    return df

def category_to_priceband(priceband_df, category_df):
    """..."""
    brandy_df = priceband_df['Brandy'] * category_df.loc['Brandy']['Sales Volume']
    gin_df = priceband_df['Gin'] * category_df.loc['Gin and Genever']['Sales Volume']
    cane_df = priceband_df['Cane'] * category_df.loc['Cane']['Sales Volume']
    vodka_df = priceband_df['Vodka'] * category_df.loc['Vodka']['Sales Volume']
    liqueurs_df = priceband_df['Liqueurs'] * category_df.loc['Liqueurs']['Sales Volume']
    whisky_df = priceband_df['Whisky'] * category_df.loc['Whisky']['Sales Volume']
    rum_df = priceband_df['Rum'] * category_df.loc['Rum']['Sales Volume']
    #tequila_df = priceband_df['Brandy'] * category_df.loc['Brandy']['Sales Volume']
    spark_df = priceband_df['Sparkling Wine'] * category_df.loc['Sparkling Wine']['Sales Volume']
    still_df = priceband_df['Still Wine'] * category_df.loc['Still Wine']['Sales Volume']
    fort_df = priceband_df['Fortified Wine'] * category_df.loc['Fortified Wine & Wine Aperitifs']['Sales Volume']
    cider_fabs_df = priceband_df['Cider and Fabs'] * category_df.loc['Cider & FABs']['Sales Volume']
    beer_df = priceband_df['Beer'] * category_df.loc['Beer']['Sales Volume']

    brandy_df = pd.DataFrame(brandy_df)
    brandy_df = brandy_df.rename(columns={0: 'Brandy'})
    gin_df = pd.DataFrame(gin_df)
    gin_df = gin_df.rename(columns={0: 'Gin and Genever'})
    cane_df = pd.DataFrame(cane_df)
    cane_df = cane_df.rename(columns={0: 'Cane'})
    vodka_df = pd.DataFrame(vodka_df)
    vodka_df = vodka_df.rename(columns={0: 'Vodka'})
    liqueurs_df = pd.DataFrame(liqueurs_df)
    liqueurs_df = liqueurs_df.rename(columns={0: 'Liqueurs'})
    whisky_df = pd.DataFrame(whisky_df)
    whisky_df = whisky_df.rename(columns={0: 'Whisky'})
    rum_df = pd.DataFrame(rum_df)
    rum_df = rum_df.rename(columns={0: 'Rum'})
    spark_df = pd.DataFrame(spark_df)
    spark_df = spark_df.rename(columns={0: 'Sparkling Wine'})
    still_df = pd.DataFrame(still_df)
    still_df = still_df.rename(columns={0: 'Still wine'})
    fort_df = pd.DataFrame(fort_df)
    fort_df = fort_df.rename(columns={0: 'Fortified Wine & Wine Aperitifs'})
    cider_fabs_df = pd.DataFrame(cider_fabs_df)
    cider_fabs_df = cider_fabs_df.rename(columns={0: 'Cider & FABs'})
    beer_df = pd.DataFrame(beer_df)
    beer_df = beer_df.rename(columns={0: 'Beer'})

    frames = [brandy_df, gin_df, cane_df, vodka_df, liqueurs_df, whisky_df, rum_df,
              spark_df, still_df, fort_df, cider_fabs_df, beer_df]
    df = pd.concat(frames, axis=1)
    return df

#%%
#dff = get_IWSR_data_estimates('2020')
df = get_spirits_price_band('2020')
#%%
dff = df.T

#%%
# df1 = get_spirits_price_band('2020')
# df2 = get_beer_price_band('2020')
# df3 = get_wine_price_band('2020')

# df = pd.concat([df1, df2, df3], axis=1)

#%%
# df_final = category_to_priceband(df, dff)
#%%
# df_trans = df_final.T

#counts = df['PRODUCTSUBCATEGORY'].value_counts()
#%%
#S=df.groupby(['PRODUCTCATEGORY','PRODUCTSUBCATEGORY']).agg('sum')[['SALESVOLUME']]
#%%
#df_prem = df[df['PRODUCTSEGMENT'] == 'Mainstream']['PRODUCTSUBCATEGORY'].value_counts()
#%%
#df_prem['PRODUCTCATEGORY'].value_counts()
