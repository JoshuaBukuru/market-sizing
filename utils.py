
import pandas as pd
import numpy as np
from pathlib import Path

full_path = Path().resolve()
DATA_DIRECTORY = full_path.parent / 'Market Sizing' / 'data'
#DATA_DIRECTORY
#%%
def get_file_in_directory(base_dir):
    """ Method to get name of file in base_dir, regardless of naming convention. 
    If multiple files exist in the directory, then function raises a warning and returns the first

    param base_dir: directory which to look in
    : return: path of file found in base_dir
    """

    path = []

    for x in base_dir.iterdir():
        path.append(x)

    if len(path) > 1:
        print("Warning: Too many files in specified directory. Choosing first file found.")

    return path[0]


def get_income_statement_data(year):
    """"
    Function to read in and preprocess the Income Statement file located in the income statement CY directory
    
    param year: 
    : return: preprocessed base df with Distell stats group as index 
    """

    base_dir = DATA_DIRECTORY / 'income_statement' / year / '' 
    path = get_file_in_directory(base_dir)
    
    df = pd.read_excel(path, sheet_name='Sheet1')
    #if 'Unnamed: 0' in df.columns:
    #    df = df[df['Unnamed: 0'] == 'Sales Litres']
        
    #df = pd.DataFrame(df.groupby(['Stats Group']).agg('sum').T.sum())
    #df = df.rename(columns = {0: 'Volume'})
    df = df[df["Unnamed: 3"] == 'L']
    df = pd.DataFrame(df.groupby(['Stats Group']).agg('sum').T.sum())
    df = df.rename(columns={0: 'Volume'})

    return df


#Test out the get income statement function
#income_stat = get_income_statement_data('2019')
#The sum across the Beer column is equal to 72265 when rounded
#assert income_stat['Volume'][0].round() == 72265, "The aggregation is incorrect"
#Test passed




def get_amarula_data(year):
    """"
    Function to get amarula volumes from the Income Statement data
    
    param year: 
    : return: preprocessed base df with Distell stats group as index 
    """

    base_dir = DATA_DIRECTORY / 'income_statement' / year / '' 
    path = get_file_in_directory(base_dir)
    
    df = pd.read_excel(path, sheet_name='Sheet1')

    #print(df['Unnamed: 3'].value_counts())
    if 'Unnamed: 0' in df.columns:
        df = df[df['Unnamed: 0'] == 'Sales Litres']
    elif 'Unnamed: 3' in df.columns: 
        df = df[df['Unnamed: 3'] == 'L']
    
    #print(df[df['Brand'] == 'Amarula Cream Liqueur'])
    agg_df = df.groupby('Brand').agg('sum')
    amarula = agg_df.loc['Amarula Cream Liqueur'].T.sum() + agg_df.loc['Amarula Gold'].T.sum()
    return amarula

##Test out the get amarula data function
#amarula_data = get_amarula_data('2019')
#Check that the aggregation equals 3242250, this values was calculated manually from the dataset
#assert amarula_data.round() == 3242250, "Aggregation incorrect"
#Test passed


def get_IWSR_data(year = '2019'):
    """"
    Function to read in and preprocess the IWSR file 
    
    param year: 
    : return: preprocessed df with IWSR data with stats group as index 
    """

    base_dir = DATA_DIRECTORY / 'IWSR' / year / '' 
    path = get_file_in_directory(base_dir)
    
    df = pd.read_excel(path, sheet_name='IWSR', skiprows = 7)
    df = df.groupby(['Category 2']).agg('sum')[[year]]
    df = df.rename(columns = {year: 'Volume'})
    df['Volume'] = df['Volume']*1000
    return df

##Test out the get IWSR data function
#IWSR_data = get_IWSR_data()
#assert IWSR_data['Volume'][0].round() == 1176750, "Aggregation incorrect"
#Test passed


def get_SAWIS_data(year):
    """"
    Function to read in and preprocess the SAWIS file 
    
    param file_path: file path of source within the data directory
    param year:
    : return: preprocessed df with SAWIS data with stats group as index 
    """

    
    base_dir = DATA_DIRECTORY / 'SAWIS' / year / '' 
    path = get_file_in_directory(base_dir)
    
    sawis_df = pd.read_excel(path, sheet_name='SAWIS', skiprows = 2, nrows = 17)
    still_wine = sawis_df.T[:5].T[1:]
    spark_wine = sawis_df.T[5:10].T[1:]
    fortified  = sawis_df.T[10:].T[1:]

    
    still_wine_val = still_wine.set_index('Still Wine').loc['Total'][int(year)]
    spark_wine_val = spark_wine.set_index('Sparkling Wine').loc['Total'][year + '.1']
    fortified_val = fortified.set_index('Fortified Wine').loc['Total'][year + '.2']
    df = pd.DataFrame(index = ['Still Wine', 'Sparkling Wine', 'Fortified Wine'], data =
                      [still_wine_val, spark_wine_val, fortified_val], columns = ['Volume'])
    return df


##Test out the get IWSR data function
#SAWIS_data = get_SAWIS_data('2020')
#Value from total column in the dataset
#assert SAWIS_data['Volume'][0] == 298417621, "Aggregation incorrect"
#Test passed

def get_SALBA_data(year):
    """" Function to read in and preprocess the SALBA file 

    param file_path: file path of source within the data directory
    : return: preprocessed df with SALBA data with stats group as index 
    """

    base_dir = DATA_DIRECTORY / 'SALBA' / year / '' 
    path = get_file_in_directory(base_dir)

    df = pd.read_excel(path, sheet_name='SALBA')
    df = df[df['Year'] == int(year)] # TODO: generalize this
    df = df.rename(columns={df.columns[-1]: 'Sales'})
    df = df.groupby(['Category', 'Quarter']).agg('sum')[['Sales']].reset_index().pivot(index = 'Category', 
                                                                                       columns = 'Quarter', 
                                                                                       values = 'Sales')
    df['Volume'] = df['1st Quarter'] + df['2nd Quarter'] + df['3rd Quarter'] + df['4th Quarter']
    return df

##Test out the get IWSR data function
#SALBA_data = get_SALBA_data('2020')
#assert SALBA_data['Volume'][0].round() == 257316970, "Aggregation incorrect"

#Test passed


def get_global_data(year):
    """" Function to read in and preprocess the GLOBAL data file 
    
    param file_path: file path of source within the data directory
    : return: preprocessed df with GLOBAL data with stats group as index 
    """
    
    base_dir = DATA_DIRECTORY / 'GLOBAL_data' / year / '' 
    path = get_file_in_directory(base_dir)

    global_df = pd.read_excel(path, sheet_name='GLOBALdata', skiprows = 18)
    global_df = global_df.rename({"Unnamed: 0": "Country", 
                  "Unnamed: 1": 'Category', 
                  "Unnamed: 2": 'Brand Owner',
                  "Unnamed: 3": 'Beer and Cider Type'}, axis = 1)

    global_df = global_df[1:] 
    global_df[year] = global_df[year].apply(lambda x: 0 if x == '-' else x)
    global_df = global_df[['Country', 'Category', 'Brand Owner', 'Beer and Cider Type', year]].dropna()
    
    global_df = global_df.groupby('Category').agg('sum')[[year]].rename(columns = {year: 'Volume'})
    global_df['Volume'] = global_df['Volume']*1000000
    return global_df

##Test out the get global data function
#global_data = get_global_data('2020')
#assert global_data['Volume'][0].round() == 2765252732, "Aggregation incorrect"

#Test passed

def get_data_orbis(year):
    """" Function to read in and preprocess the Data Orbis file 
    
    param file_path: file path of source within the data directory
    : return: preprocessed dataframe with external datasource
    """
    
    base_dir = DATA_DIRECTORY / 'data_orbis' / year / '' 
    path = get_file_in_directory(base_dir)

    df = pd.read_excel(path, sheet_name='Sheet1')
    df = df[df['COUNTRYNAME'] == 'South Africa']
    agg_df = df.groupby(['PRODUCTCATEGORY', 'PRODUCTSUBCATEGORY']).agg('sum')[['SALESVOLUME']]
    agg_df = agg_df.reset_index()
    return agg_df

##Test out the get orbis data function
#orbis_data = get_data_orbis('2020')
#assert orbis_data['SALESVOLUME'][0].round() == 2265514, "Aggregation incorrect"
#Test passed


def map_to_base_data(group, df, mappings):
    """ In some datasources, certain stats groups are named differently. 
    This function uses the mapping provided to overlay the stats group of the base df with that of the external datasource. 

    param group: statsgroup index originating from the base df (income statement)
    param df: external datasource, e.g. GLOBAL_data to be overlayed with base df
    param mappings: dictionary that details the mapping of the indexes of the base df with the indexes of the external datasources. 
                    E.g. {'Gin': ['Gin and Genever']}
    : return: The correpsonding volume for that index from the external datasource.
    """

    if group in mappings.keys():
        if len(mappings[group]) == 1:
            _map = mappings[group][0]
            return df.loc[_map]['Volume']
        else:
            total = 0
            for g in mappings[group]:
                total += df.loc[g]['Volume']
            return total
    return None


def transform_data_orbis(df):
    """ Function to transform EPOS dataframe to indexes which are compatible with BIP indexes. 
    
    param df: dataframe containing aggregated EPOS data
    : return: transformed dataframe compatible with base df
    """

    new_df = None
    new_df = pd.concat([new_df, pd.DataFrame(data = [df[df['PRODUCTCATEGORY'] == 'Beer']['SALESVOLUME'].sum() +
                                                    df[(df['PRODUCTCATEGORY'] == 'Rtds') & 
                                                       (df['PRODUCTSUBCATEGORY'] == 'Flavoured Beer')]['SALESVOLUME'].sum()], 
                                           index = ['Beer'], 
                                           columns = ['SALESVOLUME'])])
    new_df = pd.concat([new_df, pd.DataFrame(data = [df[df['PRODUCTSUBCATEGORY'] == 'Brandy']['SALESVOLUME'].sum()], 
                                           index = ['Brandy'], 
                                           columns = ['SALESVOLUME'])])
    new_df = pd.concat([new_df, pd.DataFrame(data = [df[df['PRODUCTSUBCATEGORY'] == 'Cane']['SALESVOLUME'].sum()], 
                                           index = ['Cane'], 
                                           columns = ['SALESVOLUME'])])
    new_df = pd.concat([new_df, pd.DataFrame(data = [df[df['PRODUCTSUBCATEGORY'] == 'Cider']['SALESVOLUME'].sum() + 
                                                     df[(df['PRODUCTCATEGORY'] == 'Rtds') & 
                                                        (df['PRODUCTSUBCATEGORY'] == 'Non-Alcoholic')]['SALESVOLUME'].sum()], 
                                           index = ['Cider'], 
                                           columns = ['SALESVOLUME'])])
    new_df = pd.concat([new_df, pd.DataFrame(data = [df[df['PRODUCTSUBCATEGORY'] == 'Cocktails']['SALESVOLUME'].sum()], 
                                           index = ['Cocktails'], 
                                           columns = ['SALESVOLUME'])])
    new_df = pd.concat([new_df, pd.DataFrame(data = [df[df['PRODUCTSUBCATEGORY'] == 'Cognac']['SALESVOLUME'].sum()], 
                                           index = ['Cognac'], 
                                           columns = ['SALESVOLUME'])])
    new_df = pd.concat([new_df, pd.DataFrame(data = [df[df['PRODUCTSUBCATEGORY'] == 'Fabs']['SALESVOLUME'].sum()], 
                                           index = ['Fabs'], 
                                           columns = ['SALESVOLUME'])])
    new_df = pd.concat([new_df, pd.DataFrame(data = [df[df['PRODUCTSUBCATEGORY'] == 'Fortified']['SALESVOLUME'].sum()], 
                                           index = ['Fortified Wine'], 
                                           columns = ['SALESVOLUME'])])
    new_df = pd.concat([new_df, pd.DataFrame(data = [df[df['PRODUCTSUBCATEGORY'] == 'Gin']['SALESVOLUME'].sum()], 
                                           index = ['Gin'], 
                                           columns = ['SALESVOLUME'])])
    new_df = pd.concat([new_df, pd.DataFrame(data = [df[df['PRODUCTSUBCATEGORY'] == 'Liqueurs']['SALESVOLUME'].sum()], 
                                           index = ['Liqueurs'], 
                                           columns = ['SALESVOLUME'])])
    new_df = pd.concat([new_df, pd.DataFrame(data = [df[df['PRODUCTSUBCATEGORY'] == 'Rum']['SALESVOLUME'].sum()], 
                                           index = ['Rum'], 
                                           columns = ['SALESVOLUME'])])
    new_df = pd.concat([new_df, pd.DataFrame(data = [df[df['PRODUCTSUBCATEGORY'] == 'Sparkling']['SALESVOLUME'].sum()], 
                                           index = ['Sparkling Wine'], 
                                           columns = ['SALESVOLUME'])])
    new_df = pd.concat([new_df, pd.DataFrame(data = [df[df['PRODUCTSUBCATEGORY'] == 'Spirit Cooler']['SALESVOLUME'].sum()], 
                                           index = ['Spirit Cooler'], 
                                           columns = ['SALESVOLUME'])])
    new_df = pd.concat([new_df, pd.DataFrame(data = [df[df['PRODUCTSUBCATEGORY'] == 'Unfortified']['SALESVOLUME'].sum() + 
                                                     df[df['PRODUCTSUBCATEGORY'] == 'BIB']['SALESVOLUME'].sum() + 
                                                     df[df['PRODUCTSUBCATEGORY'] == 'Perle']['SALESVOLUME'].sum()
                                                    ], 
                                           index = ['Still Wine'], 
                                           columns = ['SALESVOLUME'])])
    new_df = pd.concat([new_df, pd.DataFrame(data = [df[df['PRODUCTSUBCATEGORY'] == 'Vodka']['SALESVOLUME'].sum()], 
                                           index = ['Vodka'], 
                                           columns = ['SALESVOLUME'])])    
    new_df = pd.concat([new_df, pd.DataFrame(data = [df[df['PRODUCTSUBCATEGORY'] == 'Whisky']['SALESVOLUME'].sum()], 
                                           index = ['Whisky'], 
                                           columns = ['SALESVOLUME'])])
    new_df = new_df.rename(columns = {'SALESVOLUME': 'Volume'})
    return new_df


def transform_data_epos(df):
    """ Function to transform EPOS dataframe to indexes which are compatible with BIP indexes.

    param df: dataframe containing aggregated EPOS data
    : return: transformed dataframe compatible with base df
    """

    new_df = None
    new_df = pd.concat([new_df, pd.DataFrame(data=(df.loc['BEER']['L'] + df.loc['BEER FLAVOURED']['L']),
                                             index=['Beer'],
                                             columns=['Volume'])])
    new_df = pd.concat([new_df, pd.DataFrame(data=(df.loc['BIB']['L']),
                                             index=['BIB'],
                                             columns=['Volume'])])
    new_df = pd.concat([new_df, pd.DataFrame(data=(df.loc['BRANDY']['L']),
                                             index=['Brandy'],
                                             columns=['Volume'])])
    new_df = pd.concat([new_df, pd.DataFrame(data=(df.loc['CANE']['L']),
                                             index=['Cane'],
                                             columns=['Volume'])])
    new_df = pd.concat([new_df, pd.DataFrame(data=(df.loc['CIDER']['L']),
                                             index=['Cider'],
                                             columns=['Volume'])])
    new_df = pd.concat([new_df, pd.DataFrame(data=(df.loc['COCKTAILS']['L']),
                                             index=['Cocktails'],
                                             columns=['Volume'])])
    new_df = pd.concat([new_df, pd.DataFrame(data=(df.loc['COGNAC']['L']),
                                             index=['Cognac'],
                                             columns=['Volume'])])
    new_df = pd.concat([new_df, pd.DataFrame(data=(df.loc['FABS']['L']),
                                             index=['Fabs'],
                                             columns=['Volume'])])
    new_df = pd.concat([new_df, pd.DataFrame(data=(df.loc['FORTIFIED']['L']),
                                             index=['Fortified Wine'],
                                             columns=['Volume'])])

    new_df = pd.concat([new_df, pd.DataFrame(data=(df.loc['GIFT PACK']['L']),
                                             index=['Gift Pack'],
                                             columns=['Volume'])])
    new_df = pd.concat([new_df, pd.DataFrame(data=(df.loc['GIN']['L']),
                                             index=['Gin'],
                                             columns=['Volume'])])
    new_df = pd.concat([new_df, pd.DataFrame(data=(df.loc['GIN ENHANCER']['L']),
                                             index=['Gin Enhancer'],
                                             columns=['Volume'])])
    new_df = pd.concat([new_df, pd.DataFrame(data=(df.loc['LIQUEURS']['L']),
                                             index=['Liqueurs'],
                                             columns=['Volume'])])
    new_df = pd.concat([new_df, pd.DataFrame(data=(df.loc['PERLE']['L'] + df.loc['RED']['L'] + df.loc['ROSE']['L'] +
                                                   df.loc['WHITE']['L'] + df.loc['BIB']['L']
                                                   + df.loc['FRUIT']['L']),
                                             index=['Still Wine'],
                                             columns=['Volume'])])
    new_df = pd.concat([new_df, pd.DataFrame(data=(df.loc['RUM']['L']),
                                             index=['Rum'],
                                             columns=['Volume'])])
    new_df = pd.concat([new_df, pd.DataFrame(data=(df.loc['SPARKLING']['L']),
                                             index=['Sparkling Wine'],
                                             columns=['Volume'])])
    new_df = pd.concat([new_df, pd.DataFrame(data=(df.loc['SPIRIT COOLER']['L']),
                                             index=['Spirit Cooler'],
                                             columns=['Volume'])])
    new_df = pd.concat([new_df, pd.DataFrame(data=(df.loc['VODKA']['L']),
                                             index=['Vodka'],
                                             columns=['Volume'])])
    new_df = pd.concat([new_df, pd.DataFrame(data=(df.loc['WHISKY']['L']),
                                             index=['Whisky'],
                                             columns=['Volume'])])
    new_df = pd.concat([new_df, pd.DataFrame(data=(df.loc['FABS']['L'] + df.loc['CIDER']['L']),
                                             index=['CIDER & RTDs'],
                                             columns=['Volume'])])


    return new_df


#transformed_orbis = transform_data_orbis(orbis_data)

def transform_SALBA_df(df):
    """ Function to transform SALBA dataframe to indexes which are compatible with BIP indexes. 
    
    param df: dataframe containing raw SALBA data
    : return: transformed dataframe compatible with BIP data
    """

    new_df = None
    new_df = pd.concat([new_df, pd.DataFrame(data = [df.loc['Brandy (Premium and Cognac)'] + df.loc['Brandy (Prop and Non-Prop)']], 
                                           index = ['Brandy'], 
                                           columns = ['1st Quarter', '2nd Quarter', '3rd Quarter', '4th Quarter', 'Volume'])])
    new_df = pd.concat([new_df, pd.DataFrame(data = [df.loc['Gin']], 
                                           index = ['Gin'], 
                                           columns = ['1st Quarter', '2nd Quarter', '3rd Quarter', '4th Quarter', 'Volume'])])
    new_df = pd.concat([new_df, pd.DataFrame(data = [df.loc['Vodka and Cane Spirits']], 
                                           index = ['Vodka and Cane Spirits'], 
                                           columns = ['1st Quarter', '2nd Quarter', '3rd Quarter', '4th Quarter', 'Volume'])])
    
    new_df = pd.concat([new_df, pd.DataFrame(data = [df.loc['Whisky (Premium)'] + df.loc['Whisky (Prop and Non-Prop)']], 
                                           index = ['Whisky'], 
                                           columns = ['1st Quarter', '2nd Quarter', '3rd Quarter', '4th Quarter', 'Volume'])])
    new_df = pd.concat([new_df, pd.DataFrame(data = [df.loc['Liqueurs']], 
                                           index = ['Liqueurs'], 
                                           columns = ['1st Quarter', '2nd Quarter', '3rd Quarter', '4th Quarter', 'Volume'])])
    new_df = pd.concat([new_df, pd.DataFrame(data = [df.loc['Sparkling Wine']], 
                                           index = ['Sparkling Wine'], 
                                           columns = ['1st Quarter', '2nd Quarter', '3rd Quarter', '4th Quarter', 'Volume'])])
    new_df = pd.concat([new_df, pd.DataFrame(data = [df.loc['Standard Still and Perlé Wine'] + 
                                                     df.loc['Super Premium Red Wine'] + 
                                                     df.loc['Super Premium Rosé Wine'] + 
                                                     df.loc['Super Premium White Wine'] + 
                                                     df.loc['Premium Wine']], 
                                           index = ['Still Wine'], 
                                           columns = ['1st Quarter', '2nd Quarter', '3rd Quarter', '4th Quarter', 'Volume'])])
    new_df = pd.concat([new_df, pd.DataFrame(data = [df.loc['Total Fortified Wines and Aperitifs']], 
                                           index = ['Fortified Wine'], 
                                           columns = ['1st Quarter', '2nd Quarter', '3rd Quarter', '4th Quarter', 'Volume'])])
    new_df = pd.concat([new_df, pd.DataFrame(data = [df.loc['Alcoholic Fruit Beverages']], 
                                           index = ['AFB'], 
                                           columns = ['1st Quarter', '2nd Quarter', '3rd Quarter', '4th Quarter', 'Volume'])])
    new_df = pd.concat([new_df, pd.DataFrame(data = [df.loc['Spirit Coolers']], 
                                           index = ['Spirit Cooler'], 
                                           columns = ['1st Quarter', '2nd Quarter', '3rd Quarter', '4th Quarter', 'Volume'])])
    return new_df


#tranformed_SALBA = transform_SALBA_df(SALBA_data)
#%%
def transform_BIP_data(df):
    """ Transform base dataframe to create aggregate stats groups
    param df: base df containing income statement data
    : return: transformed df with stats groups as indexes
    
    """
    df = pd.concat([df, pd.DataFrame(data = [df.loc['HP Fortified'] +
                                             df.loc['SP Fortified']], 
                                     index = ['Fortified Wine 1'], 
                                     columns = ['Volume'])])
    df = pd.concat([df, pd.DataFrame(data = [df.loc['HP Wine'] + 
                                             df.loc['MP Wine'] + 
                                             df.loc['SP Wine'] + 
                                             df.loc['Perle Wine'] + 
                                             df.loc['Flavoured Wines']], 
                                     index = ['Still Wine'], 
                                     columns = ['Volume'])])
    df = pd.concat([df, pd.DataFrame(data = [df.loc['HP Fortified'] + 
                                             df.loc['SP Fortified'] + 
                                             df.loc['Wine Aperitif']], 
                                     index = ['Fortified Wine 2'], 
                                     columns = ['Volume'])])
    df = pd.concat([df, pd.DataFrame(data = [df.loc['Other Flavoured Beverages'] + 
                                             df.loc['Ciders'] + 
                                             df.loc['Spirit Cooler']], 
                                     index = ['CIDER & RTDs'], 
                                     columns = ['Volume'])])
    df = pd.concat([df, pd.DataFrame(data = [df.loc['Other Flavoured Beverages'] + 
                                             df.loc['Spirit Cooler']], 
                                     index = ['FABs'], 
                                     columns = ['Volume'])])
    return df

#transformed_income_stat = transform_BIP_data(income_stat)


def get_adjusted_mean_estimate(row):
    """Method to calculate mean by discarding furthest point (outlier) in set 
    
    param row: a single row from a dataframe that represents our set of different estimates
    : return: adjusted mean for the row
    """
    
    # calculate mean, measure each estimate's distance to mean. Discard furthest estimate and recalculate mean from remainder
    mean = row[['IWSR Estimate', 'SALBA Estimate', 'SAWIS Estimate',  'GLOBAL Estimate', 'Data Orbis Estimate']].mean()

    # measure difference between each point and mean
    diff = row[['IWSR Estimate', 'SALBA Estimate', 'SAWIS Estimate',  'GLOBAL Estimate', 'Data Orbis Estimate']] - mean
    diff = diff.dropna().apply(lambda x: np.abs(x)).sort_values(ascending = True)
    
    # remove further point is |set| > 1 and recalculate mean
    if diff.shape[0] > 1:
        return row.loc[diff[:-1].index].mean()
    return row.loc[diff.index].mean()
