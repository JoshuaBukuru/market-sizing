
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


def H1_H2_Epos(base_df, year, mappings, prop):
    """ Function to read in and convert the EPOS dates to h1 and h2 proportions where
        h1 maps Jan - June and h2 maps July - december data

    param year: year of data source
    param base_df: external data source
    prop mappings: dictionary that details the mapping of the indexes of the base df
    param prop: Either an H1 or H2 proportion
        : return: proportions of H1 and H2 of the various groups (such as Beer, Gin, etc)
        """
    base_dir = DATA_DIRECTORY / 'H1_H2_Epos' / year / ''
    path = get_file_in_directory(base_dir)

    df = pd.read_excel(path, sheet_name='H1')
    df_H2 = pd.read_excel(path, sheet_name='H2')
    df = pd.DataFrame(df.groupby(['Category']).agg('sum')[['L']])
    df = transform_data_epos(df)
   # df_H1['H1'] = df_H1
    df = df.rename(columns={'Volume': 'H1'})
    df_H2 = pd.DataFrame(df_H2.groupby(['Category']).agg('sum')[['L']])
    df_H2 = transform_data_epos(df_H2)
    df_H2 = df_H2.rename(columns={'Volume': 'H2'})
    df['H2'] = df_H2
    df['Prop H1'] = df['H1'] / (df['H1'] + df['H2'])
    df['Prop H2'] = df['H2'] / (df['H1'] + df['H2'])
    df = base_df['index'].apply(map_to_base_data_prop, args=[df, mappings, prop])
    return df

def H1_H2_SAWIS( year):
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
    fortified = sawis_df.T[10:].T[1:].iloc[:13]
    #
    # still_wine = still_wine.set_index(['Still Wine'])
    # spark_wine = spark_wine.set_index('Sparkling Wine')
    # fortified = fortified.set_index('Fortified Wine')
    #
    # H1_Still = still_wine.loc['Jan':'Jun'][int(year)].sum()
    # H2_Still = still_wine.loc['Jul':'Dec'][int(year)].sum()
    # H1_Spark = spark_wine.loc['Jan':'Jun'][year + ".1"].sum()
    # H2_Spark = spark_wine.loc['Jul':'Dec'][year + ".1"].sum()
    # H1_Fort = fortified.loc['Jan':'Jun'][year + ".2"].sum()
    # H2_Fort = fortified.loc['Jul':'Dec'][year + ".2"].sum()
    #
    # #df = None
    # df = pd.DataFrame(data={
    #     'H1': [H1_Still, H1_Spark, H1_Fort],
    #     'H2': [H2_Still, H2_Spark, H2_Fort],
    #     'Prop H1': [(H1_Still / (H1_Still + H2_Still)),
    #                 (H1_Spark / (H1_Spark + H2_Spark)),
    #                 (H1_Fort / (H1_Fort + H2_Fort))],
    #     'Prop H2': [(H2_Still / (H1_Still + H2_Still)),
    #                 (H2_Spark / (H1_Spark + H2_Spark)),
    #                 (H2_Fort / (H1_Fort + H2_Fort))],
    # }, index=['Still Wine', 'Sparkling Wine', 'Fortified Wine'])

    return spark_wine, fortified
#%%
df, df2 = H1_H2_SAWIS('all_years')
#%%

def base_H1_H2_proportions(df_Salba, df_Sawis, df_Sars):
    """..."""


#%%
#spark= H1_H2_SAWIS('2020')
#%%
#Place in main file
base_df = get_base_df('2020')
base_df = base_df.reset_index()
base_df['SALBA prop h1']= H1_H2_SALBA(base_df,'2020', salba_mappings,'Prop H1')
base_df['SALBA prop h2']= H1_H2_SALBA(base_df,'2020', salba_mappings,'Prop H2')
base_df['Epos prop h1'] = H1_H2_Epos(base_df,'2020', data_Epos_mappings, 'Prop H1')
base_df['Epos prop h2'] = H1_H2_Epos(base_df,'2020', data_Epos_mappings, 'Prop H2')
base_df['SAWIS prop h1'] = H1_H2_SAWIS(base_df,'2020', sawis_mappings, 'Prop H1')
base_df['SAWIS prop h2'] = H1_H2_SAWIS(base_df,'2020', sawis_mappings, 'Prop H2')
base_df = base_df.set_index('index')
#base_df.loc[['Brandy', 'Gin', 'Vodka', 'Liqueurs', 'Whisky', 'Beer', 'Sparkling Wine', 'Wine Aperitif',
#             'Fortified Wine 1', 'Still Wine', 'Fortified Wine 2', 'CIDER & RTDs', 'Ciders',
#             'FABs']]







