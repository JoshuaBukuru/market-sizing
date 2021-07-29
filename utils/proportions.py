import os
import pathlib
import pandas as pd
import numpy as np
from utils.utils import *

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

    param year: year of analysis
    : return: H1 and H2 proportions of SALBA data i.e. (Gin, Vodka, Brandy, etc)
    """
    base_dir = DATA_DIRECTORY / 'SALBA' / year / ''
    path = get_file_in_directory(base_dir)

    df = pd.read_excel(path, sheet_name='SUMMARY')
    df_2020 = df.iloc[:, [1, 21]]
    df = df.iloc[:, [1, 18, 19, 20]]

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
    brandy_df_H1_2020 = df_2020.iloc[1]["Unnamed: 21"]
    brandy_df_H2_2020 = df_2020.iloc[3]["Unnamed: 21"]

    gin_df_H1 = df.iloc[10]['Average']
    gin_df_H2 = df.iloc[12]['Average']
    gin_df_H1_2020 = df_2020.iloc[10]["Unnamed: 21"]
    gin_df_H2_2020 = df_2020.iloc[12]["Unnamed: 21"]

    vodka_and_cane_H1 = df.iloc[19]['Average']
    vodka_and_cane_H2 = df.iloc[21]['Average']
    vodka_df_H1_2020 = df_2020.iloc[19]["Unnamed: 21"]
    vodka_df_H2_2020 = df_2020.iloc[21]["Unnamed: 21"]

    whisky_df_H1 = df.iloc[28]['Average']
    whisky_df_H2 = df.iloc[30]['Average']
    whisky_df_H1_2020 = df_2020.iloc[28]["Unnamed: 21"]
    whisky_df_H2_2020 = df_2020.iloc[30]["Unnamed: 21"]

    liqueurs_df_H1 = df.iloc[37]['Average']
    liqueurs_df_H2 = df.iloc[39]['Average']
    liqueurs_df_H1_2020 = df_2020.iloc[37]["Unnamed: 21"]
    liqueurs_df_H2_2020 = df_2020.iloc[39]["Unnamed: 21"]


    df_2020 = pd.DataFrame({'H1': [ brandy_df_H1_2020, gin_df_H1_2020,
                               vodka_df_H1_2020, vodka_df_H1_2020, whisky_df_H1_2020,
                               liqueurs_df_H1_2020 ],
                       'H2': [brandy_df_H2_2020, gin_df_H2_2020,
                                vodka_df_H2_2020, vodka_df_H2_2020, whisky_df_H2_2020,
                               liqueurs_df_H2_2020]},
                      index=['Brandy', 'Gin', 'Vodka', 'Cane',
                             'Whisky', 'Liqueurs'])
    df_base = pd.DataFrame({'H1': [brandy_df_H1, gin_df_H1,
                              vodka_and_cane_H1, vodka_and_cane_H1, whisky_df_H1,
                              liqueurs_df_H1],
                       'H2': [brandy_df_H2, gin_df_H2,
                              vodka_and_cane_H2, vodka_and_cane_H2, whisky_df_H2,
                              liqueurs_df_H2]},
                      index=['Brandy', 'Gin', 'Vodka', 'Cane',
                             'Whisky',  'Liqueurs'])


    return df_base, df_2020

def H1_H2_Epos(year):
    """ Function to read in and convert the EPOS dates to h1 and h2 proportions where
        h1 maps Jan - June and h2 maps July - december data. Remember Epos provides all categories.
        The categories that are not available in SALBA, SAWIS and SARS are taken from these results

    param year: year of analysis
    : return: H1 and H2 proportions of EPOS data
        """
    # Read in the data
    base_dir = DATA_DIRECTORY / 'data_orbis_low_level' / 'Data_Orbis_Charl'
    path = get_file_in_directory(base_dir)
    df = pd.read_excel(path, sheet_name='Sheet1')

    #filter for South Africa
    df = df[df['COUNTRYNAME'] == 'South Africa']

    # seperate the data by subcategory
    df['PRODUCTSUBCATEGORY'] = df['PRODUCTSUBCATEGORY'].replace(['Cognac'], 'Brandy')
    df['month'] = df['Realigned YYYYMM'].apply(lambda x: int(str(x)[5:]))
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
    still_df = df[df['PRODUCTSUBCATEGORY'] == 'Still wine']
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
    h1_fabs = fabs_df[fabs_df['month'] <= 6]['SALESVOLUME'].sum()
    h2_fabs = fabs_df[fabs_df['month'] > 6]['SALESVOLUME'].sum()
    h1_fortified = fortified_df[fortified_df['month'] <= 6]['SALESVOLUME'].sum()
    h2_fortified = fortified_df[fortified_df['month'] > 6]['SALESVOLUME'].sum()
    h1_gin = gin_df[gin_df['month'] <= 6]['SALESVOLUME'].sum()
    h2_gin = gin_df[gin_df['month'] > 6]['SALESVOLUME'].sum()
    h1_liqueurs = liqueurs_df[liqueurs_df['month'] <= 6]['SALESVOLUME'].sum()
    h2_liqueurs = liqueurs_df[liqueurs_df['month'] > 6]['SALESVOLUME'].sum()
    h1_rum = rum_df[rum_df['month'] <= 6]['SALESVOLUME'].sum()
    h2_rum = rum_df[rum_df['month'] > 6]['SALESVOLUME'].sum()
    h1_sparkling = sparkling_df[sparkling_df['month'] <= 6]['SALESVOLUME'].sum()
    h2_sparkling = sparkling_df[sparkling_df['month'] > 6]['SALESVOLUME'].sum()
    h1_still = still_df[still_df['month'] <= 6]['SALESVOLUME'].sum()
    h2_still = still_df[still_df['month'] > 6]['SALESVOLUME'].sum()
    h1_tequila = tequila_df[tequila_df['month'] <= 6]['SALESVOLUME'].sum()
    h2_tequila = tequila_df[tequila_df['month'] > 6]['SALESVOLUME'].sum()
    h1_vodka = vodka_df[vodka_df['month'] <= 6]['SALESVOLUME'].sum()
    h2_vodka = vodka_df[vodka_df['month'] > 6]['SALESVOLUME'].sum()
    h1_whisky = whisky_df[whisky_df['month'] <= 6]['SALESVOLUME'].sum()
    h2_whisky = whisky_df[whisky_df['month'] > 6]['SALESVOLUME'].sum()

    # Create a dataframe

    df_mod = pd.DataFrame(data={
        'H1': [(h1_aperitif / (h1_aperitif + h2_aperitif)),
               (h1_beer / (h1_beer + h2_beer)),
               (h1_brandy / (h1_brandy + h2_brandy)),
               (h1_cane / (h1_cane + h2_cane)),
               (h1_cider / (h1_cider + h2_cider)),
               (h1_fabs / (h1_fabs + h2_fabs)),
               (h1_fortified / (h1_fortified + h2_fortified)),
               (h1_gin / (h1_gin + h2_gin)),
               (h1_liqueurs / (h1_liqueurs + h2_liqueurs)),
               (h1_rum / (h1_rum + h2_rum)),
               (h1_sparkling / (h1_sparkling + h2_sparkling)),
               (h1_still / (h1_still + h2_still)),
               (h1_tequila / (h1_tequila + h2_tequila)),
               (h1_vodka / (h1_vodka + h2_vodka)),
               (h1_whisky / (h1_whisky + h2_whisky)),],

        'H2': [(h2_aperitif / (h1_aperitif + h2_aperitif)),
               (h2_beer / (h1_beer + h2_beer)),
               (h2_brandy / (h1_brandy + h2_brandy)),
               (h2_cane / (h1_cane + h2_cane)),
               (h2_cider / (h1_cider + h2_cider)),
               (h2_fabs / (h1_fabs + h2_fabs)),
               (h2_fortified / (h1_fortified + h2_fortified)),
               (h2_gin / (h1_gin + h2_gin)),
               (h2_liqueurs / (h1_liqueurs + h2_liqueurs)),
               (h2_rum / (h1_rum + h2_rum)),
               (h2_sparkling / (h1_sparkling + h2_sparkling)),
               (h2_still / (h1_still + h2_still)),
               (h2_tequila / (h1_tequila + h2_tequila)),
               (h2_vodka / (h1_vodka + h2_vodka)),
               (h2_whisky / (h1_whisky + h2_whisky))]

    }, index=['Aperitif', 'Beer', 'Brandy', 'Cane', 'Cider', 'Fabs', 'Fortified Wine', 'Gin',
              'Liqueurs', 'Rum', 'Sparkling Wine', 'Still Wine', 'Tequila', 'Vodka', 'Whisky'])

    return df_mod

def H1_H2_SAWIS(year):
    """ Function to read in and convert the SAWIS dates to h1 and h2 proportions where
            h1 maps Jan - June and h2 maps July - december data

        param year: year of analysis
    : return: H1 and H2 proportions of SALBA data i.e. (still, fortified and sparkling wine)
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
    H1_Still_2020 = still_wine.loc['Jan':'Jun'][2020].sum()
    H2_Still_2020 = still_wine.loc['Jul':'Dec'][2020].sum()

    H1_Spark = spark_wine.loc['Jan':'Jun']['Average_Volumes'].sum()
    H2_Spark = spark_wine.loc['Jul':'Dec']['Average_Volumes'].sum()
    H1_Spark_2020 = spark_wine.loc['Jan':'Jun']['2020.1'].sum()
    H2_Spark_2020 = spark_wine.loc['Jul':'Dec']['2020.1'].sum()

    H1_Fort = fortified_wine.loc['Jan':'Jun']['Average_Volumes'].sum()
    H2_Fort = fortified_wine.loc['Jul':'Dec']['Average_Volumes'].sum()
    H1_Fort_2020 = fortified_wine.loc['Jan':'Jun']['2020.2'].sum()
    H2_Fort_2020 = fortified_wine.loc['Jul':'Dec']['2020.2'].sum()

    # Get the proportions
    df_2020 = pd.DataFrame(data={
        'H1': [
               (H1_Still_2020 / (H1_Still_2020 + H2_Still_2020)),
               (H1_Spark_2020 / (H1_Spark_2020 + H2_Spark_2020)),
               (H1_Fort_2020 / (H1_Fort_2020 + H2_Fort_2020)), ],
        'H2': [
               (H2_Still_2020 / (H1_Still_2020 + H2_Still_2020)),
               (H2_Spark_2020 / (H1_Spark_2020 + H2_Spark_2020)),
               (H2_Fort_2020 / (H1_Fort_2020 + H2_Fort_2020))
               ],
    }, index=[
              'Still Wine', 'Sparkling Wine', 'Fortified Wine'])
    df_base = pd.DataFrame(data={
        'H1': [(H1_Still / (H1_Still + H2_Still)),
                    (H1_Spark / (H1_Spark + H2_Spark)),
                    (H1_Fort / (H1_Fort + H2_Fort)),
                    ],
        'H2': [(H2_Still / (H1_Still + H2_Still)),
                    (H2_Spark / (H1_Spark + H2_Spark)),
                    (H2_Fort / (H1_Fort + H2_Fort)),

                   ],
    }, index=['Still Wine', 'Sparkling Wine', 'Fortified Wine'
             ])

    return df_base, df_2020

def H1_H2_SARS(year):
    """ Function to read in and convert the SARS dates to h1 and h2 proportions where
                h1 maps Jan - June and h2 maps July - december data

        param year: year of analysis
        : return: proportions of H1 and H2 of the various groups (such as Beer)"""

    base_dir = DATA_DIRECTORY / 'SARS' / year / ''
    path = get_file_in_directory(base_dir)

    sars_df = pd.read_excel(path, sheet_name='Sheet1', skiprows=1)
    sars_df = sars_df.iloc[:20,14:]
    sars_df = sars_df.fillna(0)
    sars_df["Unnamed: 14"] = sars_df["Unnamed: 14"].apply(lambda x: str(round(x)))
    sars_df = sars_df.set_index(["Unnamed: 14"])

    # Get sars 2020
    sars_df_2020 = sars_df.loc['2020']
    h1_2020 = sars_df_2020['H1']
    h2_2020 = sars_df_2020['H2']
    # Get beer H1 and H2 for 2019, 2018, 2017 and get the average of their splits
    sars_df = sars_df.loc['2017':'2019']
    h1 = (sars_df['H1'].sum()) / 3
    h2 = (sars_df['H2'].sum()) / 3

    df_base = pd.DataFrame(data={
        'H1': [h1],
        'H2': [h2],

    }, index=['Beer'])

    df_2020 = pd.DataFrame(data={
        'H1': [h1_2020],
        'H2': [h2_2020],

    }, index=['Beer'])

    return df_base, df_2020

def H1_H2_base(year):
    """ Function to concat all proportions

        param year: year of analysis
        : return: concatenated dataframes
    """
    df_base_sars, df_2020_sars = H1_H2_SARS(year)
    df_base_sawis, df_2020_sawis = H1_H2_SAWIS(year)
    df_base_salba, df_2020_salba = H1_H2_SALBA(year)
    df_base = pd.concat([df_base_sars, df_base_sawis, df_base_salba])
    df_2020 = pd.concat([df_2020_sars, df_2020_sawis, df_2020_salba])

    # Rename certain columns
    df_base.rename(index={'Gin': 'Gin and Genever'}, inplace=True)
    df_2020.rename(index={'Gin': 'Gin and Genever'}, inplace=True)

    df_2020_epos = H1_H2_Epos('2020')
    df_2019_epos = H1_H2_Epos('2019')

    # add missing categories
    df_addition = pd.DataFrame(data={'H1': [
                                            df_2019_epos.loc['Aperitif']['H1'],df_2019_epos.loc['Cider']['H1'],
                                            df_2019_epos.loc['Fabs']['H1'], df_2019_epos.loc['Rum']['H1'],
                                            df_2019_epos.loc['Tequila']['H1']],
                                    'H2': [
                                          df_2019_epos.loc['Aperitif']['H2'], df_2019_epos.loc['Cider']['H2'],
                                          df_2019_epos.loc['Fabs']['H2'], df_2019_epos.loc['Rum']['H2'],
                                          df_2019_epos.loc['Tequila']['H2']
                                          ]},
                                     index=['Aperitifs', 'Cider', 'FABs', 'Rum', 'Tequila'])
    df_addition_2020 = pd.DataFrame(data={'H1': [
                                            df_2020_epos.loc['Aperitif']['H1'], df_2020_epos.loc['Cider']['H1'],
                                            df_2020_epos.loc['Fabs']['H1'], df_2020_epos.loc['Rum']['H1'],
                                            df_2020_epos.loc['Tequila']['H1']],
                                     'H2': [
                                            df_2020_epos.loc['Aperitif']['H2'], df_2020_epos.loc['Cider']['H2'],
                                            df_2020_epos.loc['Fabs']['H2'], df_2020_epos.loc['Rum']['H2'],
                                            df_2020_epos.loc['Tequila']['H2']
                                            ]},
                               index=[ 'Aperitifs', 'Cider', 'FABs', 'Rum', 'Tequila'])

    df_base = pd.concat([df_base, df_addition])
    df_2020 = pd.concat([df_2020, df_addition_2020])

    return df_base, df_2020
def fiscal_year_cagr(year):
    """..."""
    # Get the CAGR data (from 2011 to 2020)
    base_dir = DATA_DIRECTORY / 'CAGR' /''
    path = get_file_in_directory(base_dir)
    df = pd.read_excel(path, sheet_name='CY_2019base', skiprows=6)
    df = df.iloc[:,:12]
    df = df.dropna()
    df = df.set_index(['CATEGORY'])
    df = df.drop(index='Other Wines')

    # Get the base and 2020 proportions
    df_base, df_2020 = H1_H2_base(year) # TODO check naming convention with retrieved file

    # calculate splits
    length, _ = df.shape
    df_mod_2011 = pd.DataFrame(data={'H1': np.zeros(length),
                                     'H2': np.zeros(length)}, index=df.index)
    df_mod_2012 = pd.DataFrame(data={'H1': np.zeros(length),
                                     'H2': np.zeros(length)}, index=df.index)
    df_mod_2013 = pd.DataFrame(data={'H1': np.zeros(length),
                                     'H2': np.zeros(length)}, index=df.index)
    df_mod_2014 = pd.DataFrame(data={'H1': np.zeros(length),
                                     'H2': np.zeros(length)}, index=df.index)
    df_mod_2015 = pd.DataFrame(data={'H1': np.zeros(length),
                                     'H2': np.zeros(length)}, index=df.index)
    df_mod_2016 = pd.DataFrame(data={'H1': np.zeros(length),
                                     'H2': np.zeros(length)}, index=df.index)
    df_mod_2017 = pd.DataFrame(data={'H1': np.zeros(length),
                                     'H2': np.zeros(length)}, index=df.index)
    df_mod_2018 = pd.DataFrame(data={'H1': np.zeros(length),
                                     'H2': np.zeros(length)}, index=df.index)
    df_mod_2019 = pd.DataFrame(data={'H1': np.zeros(length),
                                     'H2': np.zeros(length)}, index=df.index)
    df_mod_2020 = pd.DataFrame(data={'H1': np.zeros(length),
                                     'H2': np.zeros(length)}, index=df.index)
    #
    for index in df.index:
        df_mod_2011.loc[index]['H1'] = df_base.loc[index]['H1'] * df.loc[index][2011]
        df_mod_2011.loc[index]['H2'] = df_base.loc[index]['H2'] * df.loc[index][2011]
    for index in df.index:
        df_mod_2012.loc[index]['H1'] = df_base.loc[index]['H1'] * df.loc[index][2012]
        df_mod_2012.loc[index]['H2'] = df_base.loc[index]['H2'] * df.loc[index][2012]
    for index in df.index:
        df_mod_2013.loc[index]['H1'] = df_base.loc[index]['H1'] * df.loc[index][2013]
        df_mod_2013.loc[index]['H2'] = df_base.loc[index]['H2'] * df.loc[index][2013]
    for index in df.index:
        df_mod_2014.loc[index]['H1'] = df_base.loc[index]['H1'] * df.loc[index][2014]
        df_mod_2014.loc[index]['H2'] = df_base.loc[index]['H2'] * df.loc[index][2014]
    for index in df.index:
        df_mod_2015.loc[index]['H1'] = df_base.loc[index]['H1'] * df.loc[index][2015]
        df_mod_2015.loc[index]['H2'] = df_base.loc[index]['H2'] * df.loc[index][2015]
    for index in df.index:
        df_mod_2016.loc[index]['H1'] = df_base.loc[index]['H1'] * df.loc[index][2016]
        df_mod_2016.loc[index]['H2'] = df_base.loc[index]['H2'] * df.loc[index][2016]
    for index in df.index:
        df_mod_2017.loc[index]['H1'] = df_base.loc[index]['H1'] * df.loc[index][2017]
        df_mod_2017.loc[index]['H2'] = df_base.loc[index]['H2'] * df.loc[index][2017]
    for index in df.index:
        df_mod_2018.loc[index]['H1'] = df_base.loc[index]['H1'] * df.loc[index][2018]
        df_mod_2018.loc[index]['H2'] = df_base.loc[index]['H2'] * df.loc[index][2018]
    for index in df.index:
        df_mod_2019.loc[index]['H1'] = df_base.loc[index]['H1'] * df.loc[index][2019]
        df_mod_2019.loc[index]['H2'] = df_base.loc[index]['H2'] * df.loc[index][2019]
    for index in df.index:
        df_mod_2020.loc[index]['H1'] = df_2020.loc[index]['H1'] * df.loc[index][2020]
        df_mod_2020.loc[index]['H2'] = df_2020.loc[index]['H2'] * df.loc[index][2020]

    # Fiscal year conversions
    df_mod_final = pd.DataFrame()
    df_mod_final['2012'] = df_mod_2011['H2'] + df_mod_2012['H1']
    df_mod_final['2013'] = df_mod_2012['H2'] + df_mod_2013['H1']
    df_mod_final['2014'] = df_mod_2013['H2'] + df_mod_2014['H1']
    df_mod_final['2015'] = df_mod_2014['H2'] + df_mod_2015['H1']
    df_mod_final['2016'] = df_mod_2015['H2'] + df_mod_2016['H1']
    df_mod_final['2017'] = df_mod_2016['H2'] + df_mod_2017['H1']
    df_mod_final['2018'] = df_mod_2017['H2'] + df_mod_2018['H1']
    df_mod_final['2019'] = df_mod_2018['H2'] + df_mod_2019['H1']
    df_mod_final['2020'] = df_mod_2019['H2'] + df_mod_2020['H1']

    output_path = f'out\Fiscal_year_CAGR.csv'
    df_mod_final.to_csv(output_path)

    return df_mod_final

#%%
df_mod_2011, df_mod_2015, df_2020, df_base, df_2020 = fiscal_year_cagr('all_years')
#%%
def fiscal_year_conversion(year):
    """ Function to apply proportions to sales volume and convert to Fiscal year

        param year: year of analysis
        : return: Fiscal year conversions
    """
    df_base, df_2020 = H1_H2_base(year)
    # calculate splits
    df_forecasts = get_forecasts_volume()
    length, _ = df_forecasts.shape
    df_mod_2019 = pd.DataFrame(data={'H1': np.zeros(length),
                                'H2': np.zeros(length)}, index=df_forecasts.index)
    df_mod_2020 = pd.DataFrame(data={'H1': np.zeros(length),
                                     'H2': np.zeros(length)}, index=df_forecasts.index)
    df_mod_2021 = pd.DataFrame(data={'H1': np.zeros(length),
                                     'H2': np.zeros(length)}, index=df_forecasts.index)
    df_mod_2022 = pd.DataFrame(data={'H1': np.zeros(length),
                                     'H2': np.zeros(length)}, index=df_forecasts.index)
    df_mod_2023 = pd.DataFrame(data={'H1': np.zeros(length),
                                     'H2': np.zeros(length)}, index=df_forecasts.index)
    df_mod_2024 = pd.DataFrame(data={'H1': np.zeros(length),
                                     'H2': np.zeros(length)}, index=df_forecasts.index)
    df_mod_2025 = pd.DataFrame(data={'H1': np.zeros(length),
                                     'H2': np.zeros(length)}, index=df_forecasts.index)
    df_mod_2026 = pd.DataFrame(data={'H1': np.zeros(length),
                                     'H2': np.zeros(length)}, index=df_forecasts.index)

    for index in df_forecasts.index:
        df_mod_2019.loc[index]['H1'] = df_base.loc[index]['H1'] * df_forecasts.loc[index][2019]
        df_mod_2019.loc[index]['H2'] = df_base.loc[index]['H2'] * df_forecasts.loc[index][2019]
    for index in df_forecasts.index:
        df_mod_2020.loc[index]['H1'] = df_2020.loc[index]['H1'] * df_forecasts.loc[index][2020]
        df_mod_2020.loc[index]['H2'] = df_2020.loc[index]['H2'] * df_forecasts.loc[index][2020]
    for index in df_forecasts.index:
        df_mod_2021.loc[index]['H1'] = df_base.loc[index]['H1'] * df_forecasts.loc[index][2021]
        df_mod_2021.loc[index]['H2'] = df_base.loc[index]['H2'] * df_forecasts.loc[index][2021]
    for index in df_forecasts.index:
        df_mod_2022.loc[index]['H1'] = df_base.loc[index]['H1'] * df_forecasts.loc[index][2022]
        df_mod_2022.loc[index]['H2'] = df_base.loc[index]['H2'] * df_forecasts.loc[index][2022]
    for index in df_forecasts.index:
        df_mod_2023.loc[index]['H1'] = df_base.loc[index]['H1'] * df_forecasts.loc[index][2023]
        df_mod_2023.loc[index]['H2'] = df_base.loc[index]['H2'] * df_forecasts.loc[index][2023]
    for index in df_forecasts.index:
        df_mod_2024.loc[index]['H1'] = df_base.loc[index]['H1'] * df_forecasts.loc[index][2024]
        df_mod_2024.loc[index]['H2'] = df_base.loc[index]['H2'] * df_forecasts.loc[index][2024]
    for index in df_forecasts.index:
        df_mod_2025.loc[index]['H1'] = df_base.loc[index]['H1'] * df_forecasts.loc[index][2025]
        df_mod_2025.loc[index]['H2'] = df_base.loc[index]['H2'] * df_forecasts.loc[index][2025]
    for index in df_forecasts.index:
        df_mod_2026.loc[index]['H1'] = df_base.loc[index]['H1'] * df_forecasts.loc[index][2026]
        df_mod_2026.loc[index]['H2'] = df_base.loc[index]['H2'] * df_forecasts.loc[index][2026]
    # output_path = f'out\base.csv'
    # df_1.to_csv(output_path)

    df_mod_final = pd.DataFrame()
    df_mod_final['2020'] = df_mod_2019['H2'] + df_mod_2020['H1']
    df_mod_final['2021'] = df_mod_2020['H2'] + df_mod_2021['H1']
    df_mod_final['2022'] = df_mod_2021['H2'] + df_mod_2022['H1']
    df_mod_final['2023'] = df_mod_2022['H2'] + df_mod_2023['H1']
    df_mod_final['2024'] = df_mod_2023['H2'] + df_mod_2024['H1']
    df_mod_final['2025'] = df_mod_2024['H2'] + df_mod_2025['H1']
    df_mod_final['2026'] = df_mod_2025['H2'] + df_mod_2026['H1']

    output_path = f'out\Forecast_Fiscal_Year.csv'
    df_mod_final.to_csv(output_path)

    return df_mod_final

def get_forecasts_volume():
    """ Function to get and preprocess forecasts

        param year: year of analysis
        : return: processed forecasts
    """
    base_dir = DATA_DIRECTORY / 'Forecasts' / 'sales_volume'
    path = get_file_in_directory(base_dir)

    df = pd.read_excel(path, sheet_name='Summary')
    df = df.set_index(['CATEGORY'])
    # Drop other wines
    df = df.drop('Other Wines')
    df = df.drop(np.nan)
    df.drop('Unnamed: 10', inplace=True, axis=1)
    df.drop('INST', inplace=True, axis=1)

    return df

def get_forecasts_value():
    """ Function to get and preprocess forecasts

            param year: year of analysis
            : return: processed forecasts
        """
    base_dir = DATA_DIRECTORY / 'Forecasts' / 'sales_value'
    path = get_file_in_directory(base_dir)

    df = pd.read_excel(path, sheet_name='Prices')

    return df
def fiscal_year_conversion_value(year):
    """ Function to apply proportions to sales value and convert to Fiscal year

        param year: year of analysis
        : return: Fiscal year conversions
    """
    df_base, df_2020 = H1_H2_base(year)

    df_forecast = get_forecasts_value()
    df_forecast = df_forecast.replace(['Fortified', 'Gin', 'Aperitif', 'Sparkling', 'Fabs'],
                                  ['Fortified Wine', 'Gin and Genever', 'Aperitifs', 'Sparkling Wine', 'FABs'])

    length, _ = df_forecast.shape
    df_mod_2019 = pd.DataFrame(data={'H1': np.zeros(length),
                                'H2': np.zeros(length),
                                })
    df_mod_2020 = pd.DataFrame(data={'H1': np.zeros(length),
                                     'H2': np.zeros(length),
                                     })
    df_mod_2021 = pd.DataFrame(data={'H1': np.zeros(length),
                                     'H2': np.zeros(length),
                                     })
    df_mod_2022 = pd.DataFrame(data={'H1': np.zeros(length),
                                     'H2': np.zeros(length),
                                     })
    df_mod_2023 = pd.DataFrame(data={'H1': np.zeros(length),
                                     'H2': np.zeros(length),
                                     })
    df_mod_2024 = pd.DataFrame(data={'H1': np.zeros(length),
                                     'H2': np.zeros(length),
                                     })
    df_mod_2025 = pd.DataFrame(data={'H1': np.zeros(length),
                                     'H2': np.zeros(length),
                                     })
    df_mod_2026 = pd.DataFrame(data={'H1': np.zeros(length),
                                     'H2': np.zeros(length),
                                     })

    for index in df_forecast.index:
        df_mod_2019.iloc[index]['H1'] = df_base.loc[df_forecast.iloc[index]['SELECT']]['H1'] * df_forecast.iloc[index][2019]
        df_mod_2019.iloc[index]['H2'] = df_base.loc[df_forecast.iloc[index]['SELECT']]['H2'] * df_forecast.iloc[index][2019]

    for index in df_forecast.index:
        df_mod_2020.iloc[index]['H1'] = df_base.loc[df_forecast.iloc[index]['SELECT']]['H1'] * df_forecast.iloc[index][2020]
        df_mod_2020.iloc[index]['H2'] = df_base.loc[df_forecast.iloc[index]['SELECT']]['H2'] * df_forecast.iloc[index][2020]

    for index in df_forecast.index:
        df_mod_2021.iloc[index]['H1'] = df_base.loc[df_forecast.iloc[index]['SELECT']]['H1'] * df_forecast.iloc[index][2021]
        df_mod_2021.iloc[index]['H2'] = df_base.loc[df_forecast.iloc[index]['SELECT']]['H2'] * df_forecast.iloc[index][2021]

    for index in df_forecast.index:
        df_mod_2022.iloc[index]['H1'] = df_base.loc[df_forecast.iloc[index]['SELECT']]['H1'] * df_forecast.iloc[index][2022]
        df_mod_2022.iloc[index]['H2'] = df_base.loc[df_forecast.iloc[index]['SELECT']]['H2'] * df_forecast.iloc[index][2022]

    for index in df_forecast.index:
        df_mod_2023.iloc[index]['H1'] = df_base.loc[df_forecast.iloc[index]['SELECT']]['H1'] * df_forecast.iloc[index][2023]
        df_mod_2023.iloc[index]['H2'] = df_base.loc[df_forecast.iloc[index]['SELECT']]['H2'] * df_forecast.iloc[index][2023]

    for index in df_forecast.index:
        df_mod_2024.iloc[index]['H1'] = df_base.loc[df_forecast.iloc[index]['SELECT']]['H1'] * df_forecast.iloc[index][2024]
        df_mod_2024.iloc[index]['H2'] = df_base.loc[df_forecast.iloc[index]['SELECT']]['H2'] * df_forecast.iloc[index][2024]

    for index in df_forecast.index:
        df_mod_2025.iloc[index]['H1'] = df_base.loc[df_forecast.iloc[index]['SELECT']]['H1'] * df_forecast.iloc[index][2025]
        df_mod_2025.iloc[index]['H2'] = df_base.loc[df_forecast.iloc[index]['SELECT']]['H2'] * df_forecast.iloc[index][2025]

    for index in df_forecast.index:
        df_mod_2026.iloc[index]['H1'] = df_base.loc[df_forecast.iloc[index]['SELECT']]['H1'] * df_forecast.iloc[index][2026]
        df_mod_2026.iloc[index]['H2'] = df_base.loc[df_forecast.iloc[index]['SELECT']]['H2'] * df_forecast.iloc[index][2026]

    df_mod_final = pd.DataFrame()
    df_mod_final['Price_band'] = df_forecast['PRICE BAND CORRECT']
    df_mod_final['Index'] = df_forecast['INDEX']
    df_mod_final['category'] = df_forecast['SELECT']

    df_mod_final['2020'] = df_mod_2019['H2'] + df_mod_2020['H1']
    df_mod_final['2021'] = df_mod_2020['H2'] + df_mod_2021['H1']
    df_mod_final['2022'] = df_mod_2021['H2'] + df_mod_2022['H1']
    df_mod_final['2023'] = df_mod_2022['H2'] + df_mod_2023['H1']
    df_mod_final['2024'] = df_mod_2023['H2'] + df_mod_2024['H1']
    df_mod_final['2025'] = df_mod_2024['H2'] + df_mod_2025['H1']
    df_mod_final['2026'] = df_mod_2025['H2'] + df_mod_2026['H1']

    df_mod_final = df_mod_final.set_index(['category'])

    return df_mod_final











