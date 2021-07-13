# Market Sizing
## Author: Jeanne Elizabeth Daniel & Joshua Bukuru

## About
Different stats groups have different processes associated with estimating market size. 
This is due to the fact that different data sources provide different subsets of information on the industry, 
e.g. SALBA is the trusted source for spirits, while SARS is the trusted source for Beer. 

This code base replicates the process of manually estimating the market size of each stats group. 

The estimated market size of each stats group is the adjusted average taken from estimates using multiple data sources,  
including Data Orbis (which provides insights on sales to consumers in South Africa), and data from various organisations such as Global Data, SAWIS, SALBA, and IWSR. 
These estimates are only to be used if the current year IWSR data is not available. 

This code base also deals with transforming data from data orbis to a more granular level, that is aggregated by category,
alcohol index and price band. This will be used for further analysis when forecasting. Finally, this codebase also produces
a fiscal year conversion given the forecasts.

The price band conversions are done by extracting data from Data Orbis to get aggregate the sales volume by the category (still wine, gin, beer, etc) 
alcohol index (low alcohol, no alcohol, alcohol, etc) and price band (premium, super premium, affordable, etc). The fiscal year conversions
are done by first getting the categories proportions of each year from SALBA, SAWIS and SARS. This proportions are split by H1 and H2, where
H1 represents the first 6 months total sales volume (Jan - June) and H2 represents the second half's total sales volume (Jul - Dec).
2020 is excluded from the analysis due to the alcohol ban and skewed data, instead the average of the past 3 years H1 and H2 are used.
However, it is only used when applying the proportions to 2020's data only from the forecasts. For categories that are not in SALBA,
SAWIS and SARS but are in the forecasts, these categories should be taken from the data orbis data. Once these H1 and H2 proportions are 
identified it is then applied to the forecasts to produce the fiscal year. For example if we were trying to get the fiscal year for 2021
then we would take H2 sales volume from 2020 and add it to H1 sales volume of 2021. 

Project status: working version

## Installation

In your command line tool, run the following command line:
> pip install -r requirements.txt 

## Usage
Navigate to the directory that contains main.py
Within each data source folder (SALBA, SAWIS, Data Orbis, Income Statement, IWSR, Global Data), create a new folder with your CY year (last full year). 
Add your most recent data file for each of the data sources in these folders. See the previous years folders for the desired format of each data source. 
In main.py, scroll down to the last function. Update the variables current_year and last_year with the correct years in string format, e.g. current_year = '2021' 
Then, in your command line tool, run the following command:
> python main.py
This will work through all the files provided and calculate a market estimate for each category. The final output will be written to the outputs folder, in a .csv format. 

## Content

### utils.py
Contains util functions for reading in and transfroming the data from the various sources in the folders.

### mappings.py
This file contains the dictionary mappings between the base index stats group (Beer, Still Wine, etc) and every other data source. 

### main.py
Contains the main functions required to estimate the marketsize of each stats group and convert pricebands as well as produce a fiscal year


## Requirements
This tool requires that you have python installed on your computer. 

## Limitations
This tool requires that the formats of each data source conforms to the same format of the previous year's files.

## Resources (Documentation and other links)
See the word document "Market Sizing" for a detailed documentation of all the sources and the estimation process. 


## Contributing / Reporting issues
To report an issue, reach out so Socilla Kriel at <skriel..>

## License

This project is the intellectual property of Distell Group Limited. 
