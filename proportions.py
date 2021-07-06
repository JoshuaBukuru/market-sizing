
import pandas as pd
import numpy as np
from utils import *
from mappings import *
#from main import *

def map_to_base_data_prop(group, df, mappings, prop):
    """ In some datasources, certain stats groups are named differently.
    This function uses the mapping provided to overlay the stats group of
    the base df with that of the external datasource. Instead of the volumes
    we using the proportions column in the output

    param group: statsgroup index originating from the base df (income statement)
    param df: external datasource, e.g. GLOBAL_data to be overlayed with base df
    param mappings: dictionary that details the mapping of the indexes of the base df with the indexes of the external datasources.
                    E.g. {'Gin': ['Gin and Genever']}
    param prop: Either an H1 or H2 proportion
    : return: The correpsonding volume for that index from the external datasource.
    """

    if group in mappings.keys():
        if len(mappings[group]) == 1:
            _map = mappings[group][0]
            return df.loc[_map][prop]
        else:
            total = 0
            for g in mappings[group]:
                total += df.loc[g][prop]
            return total
    return None


def H1_H2_SALBA(year):
    """ Function to read in and convert the SAlBA dates to h1 and h2 proportions where
    h1 maps Jan - June and h2 maps July - december data

    param file_path: file path of source within the data directory
    param base_df: external data source
    param mappings: dictionary that details the mapping of the indexes of the base df
    param prop: Either an H1 or H2 proportion
    : return: preprocessed df with SALBA data with stats group as index
    """
    base_dir = DATA_DIRECTORY / 'SALBA' / year / ''
    path = get_file_in_directory(base_dir)

    df = pd.read_excel(path, sheet_name='SUMMARY')
    df = df.iloc[:,[1, 18, 19, 20]]
    # df = transform_SALBA_df(df)
    # df['H1'] = df['1st Quarter'] + df['2nd Quarter']
    # df['H2'] = df['3rd Quarter'] + df['4th Quarter']
    # df['Prop H1'] = (df['H1'] / (df['H1'] + df['H2']))
    # df['Prop H2'] = (df['H2'] / (df['H1'] + df['H2']))
    # df.index.name = "index"
    # df = base_df['index'].apply(map_to_base_data_prop, args=[df, mappings, prop])

    #Avergae of H1 and H2 across three years (2017, 2018 and 2019)
    df['Average'] = df.iloc[:,1:].agg('mean', axis=1)
    brandy_df_H1 = df.iloc[1]['Average']
    brandy_df_H2 = df.iloc[3]['Average']
    gin_df_H1 = df.iloc[10]['Average']
    gin_df_H2 = df.iloc[12]['Average']
    vodka_and_cane_H1 = df.iloc[19]['Average']
    vodka_and_cane_H2 = df.iloc[21]['Average']
    whisky_df_H1 = df.iloc[28]['Average']
    whisky_df_H2 = df.iloc[30]['Average']
    liqueurs_df_H1 = df.iloc[37]['Average']
    liqueurs_df_H2 = df.iloc[39]['Average']

    df = pd.DataFrame({'H1': [brandy_df_H1, gin_df_H1, vodka_and_cane_H1, whisky_df_H1, liqueurs_df_H1 ],
                       'H2': [brandy_df_H2, gin_df_H2, vodka_and_cane_H2, whisky_df_H2, liqueurs_df_H2]},
                      index=['Brandy', 'Gin', 'Vodka & Cane', 'Whisky', 'Liqueurs'])


    return df

def H1_H2_Epos(year):
    """ Function to read in and convert the EPOS dates to h1 and h2 proportions where
        h1 maps Jan - June and h2 maps July - december data

    param year: year of data source
    param base_df: external data source
    prop mappings: dictionary that details the mapping of the indexes of the base df
    param prop: Either an H1 or H2 proportion
        : return: proportions of H1 and H2 of the various groups (such as Beer, Gin, etc)
        """
    # Read in the data
    base_dir = DATA_DIRECTORY / 'data_orbis_low_level' / 'Data_Orbis_Charl'
    path = get_file_in_directory(base_dir)
    df = pd.read_excel(path, sheet_name='Sheet1')

    # seperate the data by subcategory
    df['PRODUCTSUBCATEGORY'] = df['PRODUCTSUBCATEGORY'].replace(['Cognac'], 'Brandy')
    df['month'] = df['Realigned YYYYMM'].apply(lambda x: int(str(x)[-1]))
    df['year'] = df['Realigned YYYYMM'].apply(lambda x: str(x)[:4])

    #filter by year 2020
    df = df[df['year'] == year]

    aperitif_df = df[df['PRODUCTSUBCATEGORY'] == 'Aperitif']
    beer_df = df[df['PRODUCTSUBCATEGORY'] == 'Beer']
    brandy_df = df[df['PRODUCTSUBCATEGORY'] == 'Brandy']
    cane_df = df[df['PRODUCTSUBCATEGORY'] == 'Cane']
    cider_df = df[df['PRODUCTSUBCATEGORY'] == 'Cider']
    fabs_df = df[df['PRODUCTSUBCATEGORY'] == 'Fabs']
    fortified_df = df[df['PRODUCTSUBCATEGORY'] == 'Fortified']
    gin_df = df[df['PRODUCTSUBCATEGORY'] == 'Gin']
    liqueurs_df = df[df['PRODUCTSUBCATEGORY'] == 'Liqueurs']
    rum_df = df[df['PRODUCTSUBCATEGORY'] == 'Rum']
    sparkling_df = df[df['PRODUCTSUBCATEGORY'] == 'Sparkling']
    still_df = df[df['PRODUCTSUBCATEGORY'] == 'Still Wine']
    tequila_df = df[df['PRODUCTSUBCATEGORY'] == 'Tequila']
    vodka_df = df[df['PRODUCTSUBCATEGORY'] == 'Vodka']
    whisky_df = df[df['PRODUCTSUBCATEGORY'] == 'Whisky']

    # get total sales volume by H1 and H2
    h1_aperitif = aperitif_df[aperitif_df['month'] <= 6]['SALESVOLUME'].sum()
    h2_aperitif = aperitif_df[aperitif_df['month'] > 6]['SALESVOLUME'].sum()
    h1_beer = beer_df[beer_df['month'] <= 6]['SALESVOLUME'].sum()
    h2_beer = beer_df[beer_df['month'] > 6]['SALESVOLUME'].sum()
    h1_brandy = brandy_df[brandy_df['month'] <= 6]['SALESVOLUME'].sum()
    h2_brandy = brandy_df[brandy_df['month'] > 6]['SALESVOLUME'].sum()
    h1_cane = cane_df[cane_df['month'] <= 6]['SALESVOLUME'].sum()
    h2_cane = cane_df[cane_df['month'] > 6]['SALESVOLUME'].sum()
    h1_cider = cider_df[cider_df['month'] <= 6]['SALESVOLUME'].sum()
    h2_cider = cider_df[cider_df['month'] > 6]['SALESVOLUME'].sum()
    h1_fortified = fortified_df[fortified_df['month'] <= 6]['SALESVOLUME'].sum()
    h2_fortified = fortified_df[fortified_df['month'] > 6]['SALESVOLUME'].sum()
    h1_fabs = fabs_df[fabs_df['month'] <= 6]['SALESVOLUME'].sum()
    h2_fabs = fabs_df[fabs_df['month'] > 6]['SALESVOLUME'].sum()
    h1_gin = gin_df[gin_df['month'] <= 6]['SALESVOLUME'].sum()
    h2_gin = gin_df[gin_df['month'] > 6]['SALESVOLUME'].sum()
    h1_liqueurs = liqueurs_df[liqueurs_df['month'] <= 6]['SALESVOLUME'].sum()
    h2_liqueurs = liqueurs_df[liqueurs_df['month'] > 6]['SALESVOLUME'].sum()
    h1_rum = rum_df[rum_df['month'] <= 6]['SALESVOLUME'].sum()
    h2_rum = rum_df[rum_df['month'] > 6]['SALESVOLUME'].sum()











    return df

#%%
import pandas as pd
year = ['2019-01','2019-02','2019-03','2019-04','2019-05','2019-06','2019-07','2020-01', '2020-02']
df = pd.DataFrame(data=year, columns=['Year'])
df['month'] = df['Year'].apply(lambda x: int(str(x)[-1]))
df_h1 = df[df['month'] > 6]['month'].sum()

#int(str(year[2])[-1])
#str(year[0])[:4]

#%%

def H1_H2_SAWIS(year):
    """ Function to read in and convert the SAWIS dates to h1 and h2 proportions where
            h1 maps Jan - June and h2 maps July - december data

        param year: year of data source
        param base_df: external data source
        prop mappings: dictionary that details the mapping of the indexes of the base df
        param prop: Either an H1 or H2 proportion
            : return: proportions of H1 and H2 of the various groups (such as Beer, Gin, etc)
            """
    base_dir = DATA_DIRECTORY / 'SAWIS' / year / ''
    path = get_file_in_directory(base_dir)

    sawis_df = pd.read_excel(path, sheet_name='Local', skiprows=2)
    still_wine = sawis_df.T[:5].T[1:].iloc[:13]
    spark_wine = sawis_df.T[5:10].T[1:].iloc[:13]
    fortified_wine = sawis_df.T[10:].T[1:].iloc[:13]

    # set the dates as indices
    still_wine = still_wine.set_index(['Still Wine'])
    spark_wine = spark_wine.set_index('Sparkling Wine')
    fortified_wine = fortified_wine.set_index('Fortified Wine')

    # Add a new column for the averages for 2018 and 2019 only
    still_wine['Average_Volumes'] = (still_wine[2018] + still_wine[2019]) / 2
    spark_wine['Average_Volumes'] = (spark_wine['2018.1'] + spark_wine['2019.1']) / 2
    fortified_wine['Average_Volumes'] = (fortified_wine['2018.2'] + fortified_wine['2019.2']) / 2



    # Get H1 and H2 volume totals
    H1_Still = still_wine.loc['Jan':'Jun']['Average_Volumes'].sum()
    H2_Still = still_wine.loc['Jul':'Dec']['Average_Volumes'].sum()
    H1_Spark = spark_wine.loc['Jan':'Jun']['Average_Volumes'].sum()
    H2_Spark = spark_wine.loc['Jul':'Dec']['Average_Volumes'].sum()
    H1_Fort = fortified_wine.loc['Jan':'Jun']['Average_Volumes'].sum()
    H2_Fort = fortified_wine.loc['Jul':'Dec']['Average_Volumes'].sum()

    # Get the proportions
    df = pd.DataFrame(data={
        'H1': [(H1_Still / (H1_Still + H2_Still)),
                    (H1_Spark / (H1_Spark + H2_Spark)),
                    (H1_Fort / (H1_Fort + H2_Fort))],
        'H2': [(H2_Still / (H1_Still + H2_Still)),
                    (H2_Spark / (H1_Spark + H2_Spark)),
                    (H2_Fort / (H1_Fort + H2_Fort))],
    }, index=['Still Wine', 'Sparkling Wine', 'Fortified Wine'])

    return df

def H1_H2_SARS(year):
    """ Function to read in and convert the SARS dates to h1 and h2 proportions where
                h1 maps Jan - June and h2 maps July - december data

        param year: year of data source
        : return: proportions of H1 and H2 of the various groups (such as Beer, Gin, etc)"""

    base_dir = DATA_DIRECTORY / 'SARS' / year / ''
    path = get_file_in_directory(base_dir)

    sars_df = pd.read_excel(path, sheet_name='Sheet1', skiprows=1)
    sars_df = sars_df.iloc[:20,14:]
    sars_df = sars_df.fillna(0)
    sars_df["Unnamed: 14"] = sars_df["Unnamed: 14"].apply(lambda x: str(round(x)))
    sars_df = sars_df.set_index(["Unnamed: 14"])

    # Get beer H1 and H2 for 2019, 2018, 2017 and get the average of their splits
    sars_df = sars_df.loc['2017':'2019']
    h1 = (sars_df['H1'].sum()) / 3
    h2 = (sars_df['H2'].sum()) / 3
    df = pd.DataFrame(data={
        'H1': [h1],
        'H2': [h2],

    }, index=['Beer'])

    return df

def base_H1_H2_proportions(year):
    """..."""
    df = pd.concat([H1_H2_SARS(year), H1_H2_SAWIS(year), H1_H2_SALBA(year)])

    return df

#%%
df = base_H1_H2_proportions('all_years')
#%%







