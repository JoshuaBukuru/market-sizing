# Market Sizing
## Author: Jeanne Elizabeth Daniel

## About
Different stats groups have different processes associated with estimating market size. 
This is due to the fact that different data sources provide different subsets of information on the industry, 
e.g. SALBA is the trusted source for spirits, while Global Data is the trusted source for Beer. 

This code base replicates the process of manually estimating the market size of each stats group. 

The estimated market size of each stats group is the adjusted average taken from estimates using multiple data sources,  
including Data Orbis (which provides insights on sales to consumers in South Africa), and data from various organisations such as Global Data, SAWIS, SALBA, and IWSR. 

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
Contains the main functions required to estimate the marketsize of each stats group.


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
