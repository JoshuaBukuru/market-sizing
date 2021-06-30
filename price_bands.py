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
    'Alcohol' if x == 'Alcohol' else
    'Low_AlcoholEnergy' if x == 'Low-AlcoholEnergy' else
    'No_AlcoholEnergy' if x == 'No-AlcoholEnergy' else
    'Energy')


    return df

def get_spark_wine_price_band(year, Value_Volume):
    """Function to read in and preprocess the Data Orbis file and split the data's sub categories
        into price bands for wines

    : param year: year of data analysis
    : return: data frame of price band splits per sub category
    """
    df = get_product_category(year, 'Wine')

    # Create a price per subcategory column to get a unit price for every subcategory
    df['Price_per_subcategory'] = (df['PRICE'] / df['SALESQUANTITY'])

    #Seperate the data by Still wine, aperitif, fortified wine, sparkling wine
    sparkling_wine_df = df[df['PRODUCTSUBCATEGORY'] == 'Sparkling']

    # still_wine_df = df[df['PRODUCTSUBCATEGORY'] == 'Still wine']
    # still_wine_df['PRODUCTSUBCATEGORY'] = df['PRODUCTSUBCATEGORY'].apply(lambda x: 'Unfortified')


    # Create price band column to classify the subcategory (low price, value, premium, etc)
    sparkling_wine_df['Price_band'] = sparkling_wine_df['Price_per_subcategory'].apply(
        price_band_wine_conversion, args=['Sparkling Wine'])

    df = sparkling_wine_df


    # df = df.groupby(['PRODUCTSUBCATEGORY', 'Price_band']).agg('sum')[['SALESVOLUME']]

    df = df.groupby(['PRODUCTSUBCATEGORY', 'INDEX', 'Price_band']).agg('sum')[[Value_Volume]]
    # Get the aggregated subcategories and apply classifier (low alc, no alc, etc) and rename columns
    sparkling_df = (df.T.Sparkling)
    df_mod = pd.DataFrame(data=np.zeros(5), columns=['Zeros'], index=['Accessible Premium',
                                                                    'Premium',
                                                                    'Super Premium',
                                                                    'Ultra Premium',
                                                                    'Value'])

    df_mod = alcohol_type_classifier('Sparkling_Wine', sparkling_df, df_mod)
    df_mod.drop('Zeros', axis='columns', inplace=True)




    # # calculate proportions of all subcategories
    # for col in df_mod.columns:
    #     df_mod[col] = df_mod[col].apply(lambda x: x / (df_mod[col].sum()))

    df_mod = df_mod.fillna(0)

    return df_mod
def get_still_wine_price_band(year, Value_Volume):
    """Function to read in and preprocess the Data Orbis file and split the data's sub categories
        into price bands for wines

    : param year: year of data analysis
    : return: data frame of price band splits per sub category
    """
    df = get_product_category(year, 'Wine')

    # Create a price per subcategory column to get a unit price for every subcategory
    df['Price_per_subcategory'] = (df['PRICE'] / df['SALESQUANTITY'])

    #Seperate the data by Still wine, aperitif, fortified wine, sparkling wine
    still_wine_df = df[df['PRODUCTSUBCATEGORY'] == 'Still wine']
    still_wine_df['PRODUCTSUBCATEGORY'] = df['PRODUCTSUBCATEGORY'].apply(lambda x: 'Unfortified')
    fotified_wine_df = df[df['PRODUCTSUBCATEGORY'] == 'Fortified']

    aperitif_wine_df = df[df['PRODUCTSUBCATEGORY'] == 'Aperitif']

    # Create price band column to classify the subcategory (low price, value, premium, etc)
    aperitif_wine_df['Price_band'] = aperitif_wine_df['Price_per_subcategory'].apply(
        price_band_wine_conversion, args=['Still Wine'])
    still_wine_df['Price_band'] = still_wine_df['Price_per_subcategory'].apply(
        price_band_wine_conversion, args=['Still Wine'])

    fotified_wine_df['Price_band'] = fotified_wine_df['Price_per_subcategory'].apply(
        price_band_wine_conversion, args=['Still Wine'])
    df = pd.concat([still_wine_df, aperitif_wine_df, fotified_wine_df])

    # df = df.groupby(['PRODUCTSUBCATEGORY', 'Price_band']).agg('sum')[['SALESVOLUME']]

    df = df.groupby(['PRODUCTSUBCATEGORY', 'INDEX', 'Price_band']).agg('sum')[[Value_Volume]]
    # Get the aggregated subcategories and apply classifier (low alc, no alc, etc) and rename columns
    still_df = (df.T.Unfortified)
    df_mod = pd.DataFrame(data=np.zeros(7), columns=['Zeros'], index=['Accessible Premium',
                                                                    'Low Price',
                                                                    'Premium',
                                                                    'Super Premium',
                                                                    'Ultra Premium',
                                                                    'Affordable',
                                                                    'Value'])
    df_mod = alcohol_type_classifier('Still_Wine', still_df, df_mod)
    df_mod.drop('Zeros', axis='columns', inplace=True)
    fortified_df = (df.T.Fortified)
    df_mod = alcohol_type_classifier('Fortified_Wine', fortified_df, df_mod)
    aperitif_df = df.T.Aperitif
    df_mod = alcohol_type_classifier('Aperitif', aperitif_df, df_mod)

    # # calculate proportions of all subcategories
    # for col in df_mod.columns:
    #     df_mod[col] = df_mod[col].apply(lambda x: x / (df_mod[col].sum()))

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
    if 'Low_AlcoholEnergy' in category:
        df_mod[name + '_Low_AlcoholEnergy'] = category.Energy.T
    if 'No_AlcoholEnergy' in category:
        df_mod[name + '_No_AlcoholEnergy'] = category.Energy.T

    return df_mod


def get_beer_price_band(year, Value_Volume):
    """" Function to read in and preprocess the Data Orbis file and split the data's sub categories
        into price bands for beers

        param year: year of data analysis
        : return: data frame of price band splits per sub category
        """
    df = get_product_category(year, 'Beer')
    df = convert_product_description_beer_and_rtds(df)

    # # Aggregate by subcategory, alcohol presence and price band
    df = df.groupby(['PRODUCTSUBCATEGORY', 'INDEX', 'Price_band']).agg('sum')[[Value_Volume]]
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

    # # calculate proportions of all subcategories
    # for col in df_mod.columns:
    #     df_mod[col] = df_mod[col].apply(lambda x: x / (df_mod[col].sum()))

    df_mod = df_mod.fillna(0)

    return df_mod

def get_Rtds_price_band(year, Value_Volume):
    """Function to read in and preprocess the Data Orbis file and split the data's sub categories
        into price bands for RTDs

        : param year: year of data analysis
        : return: data frame of price band splits per sub category
        """
    df = get_product_category(year, 'Rtds')
    df = convert_product_description_beer_and_rtds(df)

    # # Aggregate by subcategory, alcohol presence and price band
    df = df.groupby(['PRODUCTSUBCATEGORY', 'INDEX', 'Price_band']).agg('sum')[[Value_Volume]]
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

    # # calculate proportions of all subcategories
    # for col in df_mod.columns:
    #     df_mod[col] = df_mod[col].apply(lambda x: x / (df_mod[col].sum()))

    df_mod = df_mod.fillna(0)

    return df_mod

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

def get_spirits_price_band(year, Value_Volume):
    """Function to read in and preprocess the Data Orbis file and split the data's sub categories
        into price bands for wines

    : param year: year of data analysis
    : return: data frame of price band splits per sub category
    """
    df = get_product_category(year, 'Spirits')

    # Create a price per subcategory column to get a unit price for every subcategory
    df['Price_per_subcategory'] = (df['PRICE'] / df['SALESQUANTITY'])

    # Convert all cognac's to brandy
    df['PRODUCTSUBCATEGORY'] = df['PRODUCTSUBCATEGORY'].replace(['Cognac'], 'Brandy')

    # Apply price and column
    df['Price_band'] = df['Price_per_subcategory'].apply(
        price_band_spirit_conversion)

    # Aggregate by subcategory, alcohol presence and price band
    df = df.groupby(['PRODUCTSUBCATEGORY', 'INDEX', 'Price_band']).agg('sum')[[Value_Volume]]

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

    # # calculate proportions of all subcategories
    # for col in df_mod.columns:
    #     df_mod[col] = df_mod[col].apply(lambda x: x / (df_mod[col].sum()))

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

    base_dir = DATA_DIRECTORY / 'Estimates' / year / 'Nikki_estimates'
    path = get_file_in_directory(base_dir)

    df = pd.read_excel(path, sheet_name='Porportions')
    df_name = df['IWSR_Category2.1']
    df = df.iloc[:18, 15:]
    df['Unnamed: 15'] = df['Unnamed: 15'] * 1000
    #df['Sales Volume'] = sales_volume_df
    df['Index'] = df_name
    df['Alcohol_volume'] = df['Alcoholic'] * df['Unnamed: 15']
    df['No_Alcohol_volume'] = df['No Alcohol'] * df['Unnamed: 15']
    df['Low_Alcohol_volume'] = df['Low Alcohol'] * df['Unnamed: 15']
    df = df.set_index(['Index'])

    return df

def category_to_priceband(category_df, priceband_spirits, priceband_beer, priceband_rtds, priceband_wine,
                          priceband_still_wine):
    """..."""
    # RTDS
    cider_alcohol_df = category_df.loc['Cider']['Alcohol_volume'] * priceband_rtds['Cider_Alcohol']
    cider_low_alcohol_df = category_df.loc['Cider']['Low_Alcohol_volume'] * priceband_rtds['Cider_Low_Alcohol']
    cider_no_alcohol_df = category_df.loc['Cider']['No_Alcohol_volume'] * priceband_rtds['Cider_No_Alcohol']
    fabs_alcohol_df = category_df.loc['RTDs']['Alcohol_volume'] * priceband_rtds['Fabs_Alcohol']
    fabs_low_alcohol_df = category_df.loc['RTDs']['Low_Alcohol_volume'] * priceband_rtds['Fabs_Low_Alcohol']
    fabs_no_alcohol_df = category_df.loc['RTDs']['No_Alcohol_volume'] * priceband_rtds['Fabs_No_Alcohol']

    # Beers
    beer_alcohol_df = category_df.loc['Beer']['Alcohol_volume'] * priceband_beer['Beer_Alcohol']
    beer_low_alcohol_df = category_df.loc['Beer']['Low_Alcohol_volume'] * priceband_beer['Beer_Low_Alcohol']
    beer_no_alcohol_df = category_df.loc['Beer']['No_Alcohol_volume'] * priceband_beer['Beer_No_Alcohol']

    # Spirits
    brandy_alcohol_df = category_df.loc['Brandy']['Alcohol_volume'] * priceband_spirits['Brandy_Alcohol']
    brandy_low_alcohol_df = category_df.loc['Brandy']['Low_Alcohol_volume'] * priceband_spirits['Brandy_Low_Alcohol']
    # brandy_no_alcohol_df = category_df.loc['Brandy']['No_Alcohol_volume'] * priceband_spirits['Brandy_No_Alcohol']

    cane_alcohol_df = category_df.loc['Cane']['Alcohol_volume'] * priceband_spirits['Cane_Alcohol']
    # cane_low_alcohol_df = category_df.loc['Cane']['Low_Alcohol_volume'] * priceband_spirits['Cane_Low_Alcohol']
    # cane_no_alcohol_df = category_df.loc['Cane']['No_Alcohol_volume'] * priceband_spirits['Cane_No_Alcohol']

    gin_alcohol_df = category_df.loc['Gin and Genever']['Alcohol_volume'] * priceband_spirits['Gin_Alcohol']
    gin_low_alcohol_df = category_df.loc['Gin and Genever']['Low_Alcohol_volume'] * priceband_spirits['Gin_Low_Alcohol']
    # gin_no_alcohol_df = category_df.loc['Gin and Genever']['No_Alcohol_volume'] * priceband_spirits['Gin_No_Alcohol']

    liqueurs_alcohol_df = category_df.loc['Liquers']['Alcohol_volume'] * priceband_spirits['Liqueurs_Alcohol']
    # liqueurs_low_alcohol_df = category_df.loc['Liquers']['Low_Alcohol_volume'] * priceband_spirits['Liqueurs_Low_Alcohol']
    # liqueurs_no_alcohol_df = category_df.loc['Liquers']['No_Alcohol_volume'] * priceband_spirits['Liqueurs_No_Alcohol']

    rum_alcohol_df = category_df.loc['Rum']['Alcohol_volume'] * priceband_spirits['Rum_Alcohol']
    # rum_low_alcohol_df = category_df.loc['Rum']['Low_Alcohol_volume'] * priceband_spirits['Rum_Low_Alcohol']
    # rum_no_alcohol_df = category_df.loc['Rum']['No_Alcohol_volume'] * priceband_spirits['Rum_No_Alcohol']

    tequila_alcohol_df = category_df.loc['Tequila']['Alcohol_volume'] * priceband_spirits['Tequila_Alcohol']
    tequila_low_alcohol_df = category_df.loc['Tequila']['Low_Alcohol_volume'] * priceband_spirits['Tequila_Low_Alcohol']
    # tequila_no_alcohol_df = category_df.loc['Tequila']['No_Alcohol_volume'] * priceband_spirits['Tequila_No_Alcohol']

    vodka_alcohol_df = category_df.loc['Vodka']['Alcohol_volume'] * priceband_spirits['Vodka_Alcohol']
    vodka_low_alcohol_df = category_df.loc['Vodka']['Low_Alcohol_volume'] * priceband_spirits['Vodka_Low_Alcohol']
    # vodka_no_alcohol_df = category_df.loc['Vodka']['No_Alcohol_volume'] * priceband_spirits['Vodka_No_Alcohol']

    whisky_alcohol_df = category_df.loc['Whisky']['Alcohol_volume'] * priceband_spirits['Whisky_Alcohol']
    # whisky_low_alcohol_df = category_df.loc['Whisky']['Low_Alcohol_volume'] * priceband_spirits['Whisky_Low_Alcohol']
    # whisky_no_alcohol_df = category_df.loc['Whisky']['No_Alcohol_volume'] * priceband_spirits['Whisky_No_Alcohol']

    # Wine
    # aperitif_alcohol_df = category_df.loc['Light Aperitifs']['Alcohol_volume'] \
    #                       * priceband_wine['Aperitif_Alcohol']
    aperitif_low_alcohol_df = category_df.loc['Light Aperitifs']['Low_Alcohol_volume'] \
                              * priceband_wine['Aperitif_Low_Alcohol']
    # aperitif_no_alcohol_df = category_df.loc['Light Aperitifs']['No_Alcohol_volume'] \
    #                           * priceband_wine['Aperitif_No_Alcohol']

    fortified_alcohol_df = category_df.loc['Fortified Wine']['Alcohol_volume'] \
                           * priceband_wine['Fortified_Wine_Alcohol']
    # fortified_low_alcohol_df = category_df.loc['Fortified Wine']['Low_Alcohol_volume'] \
    #                            * priceband_wine['Fortified_Wine_Low_Alcohol']
    # fortified_no_alcohol_df = category_df.loc['Fortified Wine']['No_Alcohol_volume'] \
    #                            * priceband_wine['Fortified_Wine_No_Alcohol']

    spark_alcohol_df = category_df.loc['Sparkling Wine']['Alcohol_volume'] \
                           * priceband_wine['Sparkling_Wine_Alcohol']
    # spark_low_alcohol_df = category_df.loc['Sparkling Wine']['Low_Alcohol_volume'] \
    #                            * priceband_wine['Sparkling_Wine_Low_Alcohol']
    spark_no_alcohol_df = category_df.loc['Sparkling Wine']['No_Alcohol_volume'] \
                               * priceband_wine['Sparkling_Wine_No_Alcohol']

    still_alcohol_df = category_df.loc['Still Wine']['Alcohol_volume'] \
                           * priceband_still_wine['Still_Wine_Alcohol']
    still_low_alcohol_df = category_df.loc['Still Wine']['Low_Alcohol_volume'] \
                               * priceband_still_wine['Still_Wine_Low_Alcohol']
    still_no_alcohol_df = category_df.loc['Still Wine']['No_Alcohol_volume'] \
                               * priceband_still_wine['Still_Wine_No_Alcohol']


    # RTDs

    cider_alcohol_df = pd.DataFrame(cider_alcohol_df)
    cider_alcohol_df = cider_alcohol_df.rename(columns={0: 'Cider_Alcohol'})
    cider_low_alcohol_df = pd.DataFrame(cider_low_alcohol_df)
    cider_low_alcohol_df = cider_low_alcohol_df.rename(columns={0: 'Cider_Low_Alcohol'})
    cider_no_alcohol_df = pd.DataFrame(cider_no_alcohol_df)
    cider_no_alcohol_df = cider_no_alcohol_df.rename(columns={0: 'Cider_No_Alcohol'})

    fabs_alcohol_df = pd.DataFrame(fabs_alcohol_df)
    fabs_alcohol_df = fabs_alcohol_df.rename(columns={0: 'Fabs_Alcohol'})
    fabs_low_alcohol_df = pd.DataFrame(fabs_low_alcohol_df)
    fabs_low_alcohol_df = fabs_low_alcohol_df.rename(columns={0: 'Fabs_Low_Alcohol'})
    fabs_no_alcohol_df = pd.DataFrame(fabs_no_alcohol_df)
    fabs_no_alcohol_df = fabs_no_alcohol_df.rename(columns={0: 'Fabs_No_Alcohol'})

    # Beers

    beer_alcohol_df = pd.DataFrame(beer_alcohol_df)
    beer_alcohol_df = beer_alcohol_df.rename(columns={0: 'Beer_Alcohol'})
    beer_low_alcohol_df = pd.DataFrame(beer_low_alcohol_df)
    beer_low_alcohol_df = beer_low_alcohol_df.rename(columns={0: 'Beer_Low_Alcohol'})
    beer_no_alcohol_df = pd.DataFrame(beer_no_alcohol_df)
    beer_no_alcohol_df = beer_no_alcohol_df.rename(columns={0: 'Beer_No_Alcohol'})

    # Spirits

    brandy_alcohol_df = pd.DataFrame(brandy_alcohol_df)
    brandy_alcohol_df = brandy_alcohol_df.rename(columns={0: 'Brandy_Alcohol'})
    brandy_low_alcohol_df = pd.DataFrame(brandy_low_alcohol_df)
    brandy_low_alcohol_df = brandy_low_alcohol_df.rename(columns={0: 'Brandy_Low_Alcohol'})

    cane_alcohol_df = pd.DataFrame(cane_alcohol_df)
    cane_alcohol_df = cane_alcohol_df.rename(columns={0: 'Cane_Alcohol'})

    gin_alcohol_df = pd.DataFrame(gin_alcohol_df)
    gin_alcohol_df = gin_alcohol_df.rename(columns={0: 'Gin_Alcohol'})
    gin_low_alcohol_df = pd.DataFrame(gin_low_alcohol_df)
    gin_low_alcohol_df = gin_low_alcohol_df.rename(columns={0: 'Gin_Low_Alcohol'})

    liqueurs_alcohol_df = pd.DataFrame(liqueurs_alcohol_df)
    liqueurs_alcohol_df = liqueurs_alcohol_df.rename(columns={0: 'Liqueurs_Alcohol'})

    rum_alcohol_df = pd.DataFrame(rum_alcohol_df)
    rum_alcohol_df = rum_alcohol_df.rename(columns={0: 'Rum_Alcohol'})

    tequila_alcohol_df = pd.DataFrame(tequila_alcohol_df)
    tequila_alcohol_df = tequila_alcohol_df.rename(columns={0: 'Tequila_Alcohol'})
    tequila_low_alcohol_df = pd.DataFrame(tequila_low_alcohol_df)
    tequila_low_alcohol_df = tequila_low_alcohol_df.rename(columns={0: 'Tequila_Low_Alcohol'})

    vodka_alcohol_df = pd.DataFrame(vodka_alcohol_df)
    vodka_alcohol_df = vodka_alcohol_df.rename(columns={0: 'Vodka_Alcohol'})
    vodka_low_alcohol_df = pd.DataFrame(vodka_low_alcohol_df)
    vodka_low_alcohol_df = vodka_low_alcohol_df.rename(columns={0: 'Vodka_Low_Alcohol'})

    whisky_alcohol_df = pd.DataFrame(whisky_alcohol_df)
    whisky_alcohol_df = whisky_alcohol_df.rename(columns={0: 'Whisky_Alcohol'})

    # Wine

    aperitif_low_alcohol_df = pd.DataFrame(aperitif_low_alcohol_df)
    aperitif_low_alcohol_df = aperitif_low_alcohol_df.rename(columns={0: 'Aperitif_Low_Alcohol'})

    fortified_alcohol_df = pd.DataFrame(fortified_alcohol_df)
    fortified_alcohol_df = fortified_alcohol_df.rename(columns={0: 'Fortified_Alcohol'})

    spark_alcohol_df = pd.DataFrame(spark_alcohol_df)
    spark_alcohol_df = spark_alcohol_df.rename(columns={0: 'Sparkling_Alcohol'})
    spark_no_alcohol_df = pd.DataFrame(spark_no_alcohol_df)
    spark_no_alcohol_df = spark_no_alcohol_df.rename(columns={0: 'Sparkling_No_Alcohol'})

    still_alcohol_df = pd.DataFrame(still_alcohol_df)
    still_alcohol_df = still_alcohol_df.rename(columns={0: 'Still_Alcohol'})
    still_low_alcohol_df = pd.DataFrame(still_low_alcohol_df)
    still_low_alcohol_df = still_low_alcohol_df.rename(columns={0: 'Still_Low_Alcohol'})
    still_no_alcohol_df = pd.DataFrame(still_no_alcohol_df)
    still_no_alcohol_df = still_no_alcohol_df.rename(columns={0: 'Still_No_Alcohol'})


    frames = [still_alcohol_df.T, still_low_alcohol_df.T, still_no_alcohol_df.T,
              spark_alcohol_df.T, spark_no_alcohol_df.T,
              fortified_alcohol_df.T,
              aperitif_low_alcohol_df.T,
              brandy_alcohol_df.T, brandy_low_alcohol_df.T,
              cane_alcohol_df.T,
              gin_alcohol_df.T, gin_low_alcohol_df.T,
              liqueurs_alcohol_df.T,
              rum_alcohol_df.T,
              tequila_alcohol_df.T, tequila_low_alcohol_df.T,
              vodka_alcohol_df.T, vodka_low_alcohol_df.T,
              whisky_alcohol_df.T,
              cider_alcohol_df.T, cider_low_alcohol_df.T, cider_no_alcohol_df.T,
              fabs_alcohol_df.T, fabs_low_alcohol_df.T, fabs_no_alcohol_df.T,
              beer_alcohol_df.T, beer_low_alcohol_df.T, beer_no_alcohol_df.T]

    df = pd.concat(frames)

    return df

def final_outut_to_csv(year, Value_Volume):
    """..."""
    df_spirits = get_spirits_price_band(year, Value_Volume)
    df_beer = get_beer_price_band(year, Value_Volume)
    df_rtds = get_Rtds_price_band(year, Value_Volume)
    df_wine = get_spark_wine_price_band(year, Value_Volume)
    df_still_wine = get_still_wine_price_band(year, Value_Volume)
    df = pd.concat([df_still_wine, df_wine, df_rtds, df_beer, df_spirits], axis=1)
    df = df.fillna(0)

    # output_path = f'out\price_band_Nikki{year}.csv'
    # df.to_csv(output_path)
    return df
def test_beer(year):
    """Function to read in and preprocess the Data Orbis file and split the data's sub categories
            into price bands for RTDs

            : param year: year of data analysis
            : return: data frame of price band splits per sub category
            """
    df = get_product_category(year, 'Rtds')
    df = convert_product_description_beer_and_rtds(df)

    return df
#%%


#%%
df_volume = final_outut_to_csv('2019', 'SALESVOLUME')
df_value = final_outut_to_csv('2019', 'SALESVALUEINCL')

#%%
df_vol = df_volume.T
df_val = df_value.T
#%%
df_merge = pd.concat([df_vol, df_val], axis=1)
output_path = f'out\Volume_Value_2019.csv'
df_merge.to_csv(output_path)



