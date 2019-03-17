import pandasdmx as pdmx
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import inspect

# Organisation for Economic Cooperation and Development (OECD)
# root_url = "https://stats.oecd.org/restsdmx/sdmx.ashx/GetData/"
oecd_connection = pdmx.api.Request('OECD')

# ----------------------- Gross Domestic Product (GDP) ----------------------- #
# https://en.wikipedia.org/wiki/Gross_domestic_product
# GDP can be determined in three ways, all of which should, in principle, give the same result.
# They are the production (or output or value added) approach, the income approach, or the speculated expenditure approach.
# The most direct of the three is the production approach, which sums the outputs of every class of enterprise to arrive at
# the total. The expenditure approach works on the principle that all of the product must be bought by somebody,
# therefore the value of the total product must be equal to people's total expenditures in buying things.
# The income approach works on the principle that the incomes of the productive factors ("producers," colloquially)
# must be equal to the value of their product, and determines GDP by finding the sum of all producers' incomes.
# In national currency, in current prices and constant prices (national base year, previous year prices and OECD base year i.e. 2010).
# For comparative purposes in US $ current prices and constant prices, using exchange rate and for the GDP by expenditures
# (in absence of specific PPP series) GDP PPPs for all series except actual individual consumption where a specific PPP is used.
# Expressed in millions and in indices. For the Euro area countries, the data in national currency for all years are calculated
# using the fixed conversion rates against the euro.

# Data importing
dataset_identifier = "SNA_TABLE1"

try:
    gdp_connection = oecd_connection.get(resource_type='data', resource_id=dataset_identifier, params={'startPeriod': '1960'})
except UnicodeDecodeError:
    print("UnicodeDecodeError...")
except KeyError:
    print("KeyError...")
finally:
    print("Successful Connection to the API!")
    gdp_series = list(gdp_connection.data.series)
    # Explore the resulted dataset
    print("The length of the resulted data set is: " + str(len(gdp_series)))

# i = 0
# while i<=1000:
#     print(gdp_series[i].key)
#     i += 1

# List of measures
# http://www.oecd.org/sdd/na/tips-for-a-better-use-of-the-oecd-annual-national-accounts-statistics.htm
# C	Current prices
# V	Constant prices, national base year
# VP	Constant prices, previous year prices
# VOB	Constant prices, OECD base year 2010
# CXC	US $, current prices, current exchanges rates
# VXCOB	US $, current prices, constant exchange rates, OECD base year 2010
# VXVOB	US $, constant prices, constant exchange rates, OECD base year 2010
# XVP	Previous year prices and previous year exchange rates
# CPC	US $, current prices, current PPPs
# VPCOB	US $, current prices, constant PPPs, OECD base year 2010
# VPVOB	US $, constant prices, constant PPPs, OECD base year 2010
# PVP	Previous year prices. previous year PPPs
# HCXC	Per head, US $, current prices, current exchanges rates
# HVXVOB	Per head, US $, constant prices, constant exchange rates, OECD base year 2010
# HCPC	Per head, US $, current prices, current PPPs
# HVPVOB	Per head, US $, constant prices, constant PPPs, OECD base year 2010
# HCPIXOE	Per head, Index using current PPPs, OECD = 100
# HVPIXOE	Per head, Index using constant prices, constant PPPs, OECD base year 2010, OECD = 100
# VIX.OB	Volume index, 2010 = 100
# DOB	Deflator, 2010 = 100
# G	Growth rate
# CD	National currency per US dollar
# PER	Persons
# FTE	Full-time equivalents2
# JOB	Jobs
# HRS	Hours

# Extracting the VOB dataset for comparability
gdp_dataset_vob = gdp_connection.write(s for s in gdp_connection.data.series if s.key.MEASURE == 'VOB')
# Extracting the growth rate
gdp_dataset_growth_rate = gdp_connection.write(s for s in gdp_connection.data.series if s.key.MEASURE == 'G')

gdp_dataset_vob = pd.DataFrame(gdp_dataset_vob.to_records())
gdp_dataset_growth_rate = pd.DataFrame(gdp_dataset_growth_rate.to_records())

print(type(gdp_dataset_vob))
print(gdp_dataset_vob.shape)

print(type(gdp_dataset_growth_rate))
print(gdp_dataset_growth_rate.shape)

# Data cleaning - staging
# Define the function for cleaning the column names
def column_names_cleaning(oecd_dataset):
    '''
    Creating a function for cleaning the column names for the data sets
    returned from the OECD API. This way allows the user to copy paste
    the function across scripts and perform time-consuming operations.
    Provide the OECD data frame as an input data set.
    :param oecd_dataset:
    :return:
    '''

    oecd_dataset.columns = oecd_dataset.columns.str.replace('(', '')
    oecd_dataset.columns = oecd_dataset.columns.str.replace(')', '')
    oecd_dataset.columns = oecd_dataset.columns.str.replace("'", '')
    oecd_dataset.columns = oecd_dataset.columns.str.replace(', ', '_')

    return oecd_dataset

help(column_names_cleaning)

# Apply the function
gdp_dataset_vob = column_names_cleaning(oecd_dataset=gdp_dataset_vob)
gdp_dataset_growth_rate = column_names_cleaning(oecd_dataset=gdp_dataset_growth_rate)

gdp_dataset_vob = gdp_dataset_vob.filter(regex='TIME_PERIOD|_B1_GA_VOB', axis=1)
print(gdp_dataset_vob.shape)

gdp_dataset_growth_rate = gdp_dataset_growth_rate.filter(regex='TIME_PERIOD|_B1_GA_G', axis=1)
print(gdp_dataset_growth_rate.shape)

# Define the function for reshaping the data sets
def reshape_dataset(oecd_dataset):
    '''
    Creating a function for reshaping the data set returned from the OECD API.
    This way allows the user to copy paste the function across scripts and perform time-consuming operations.
    Provide the OECD data frame as an input data set.
    :param oecd_dataset:
    :return:
    '''
    oecd_dataset = pd.melt(frame=oecd_dataset, id_vars=['TIME_PERIOD'], var_name='country', value_name='gdp_b1_ga')
    oecd_dataset.columns = [x.lower() for x in oecd_dataset.columns]
    oecd_dataset['country'] = oecd_dataset['country'].str.replace('_B1_GA_VOB', '')
    oecd_dataset['country'] = oecd_dataset['country'].str.replace('_B1_GA_G', '')
    oecd_dataset['gdp_b1_ga'].fillna(0, inplace=True)
    oecd_dataset['time_period'] = oecd_dataset['time_period'].astype(str).astype(int)

    return oecd_dataset

# Apply the function
gdp_dataset_vob = reshape_dataset(oecd_dataset=gdp_dataset_vob)
gdp_dataset_vob = gdp_dataset_vob.rename(columns={gdp_dataset_vob.columns[2]:'gdp_b1_ga_vob'})
gdp_dataset_growth_rate = reshape_dataset(oecd_dataset=gdp_dataset_growth_rate)
gdp_dataset_growth_rate = gdp_dataset_growth_rate.rename(columns={gdp_dataset_growth_rate.columns[2]:'gdp_b1_ga_g'})

# Check the structure before sending the data sets to the DB
print(gdp_dataset_vob.info())
print(gdp_dataset_growth_rate.info())

# Connecting to the database
pwd_gen = pd.read_csv(filepath_or_buffer="C:/Users/james/PycharmProjects/PWDGen.csv", sep=";", encoding="UTF-8")

# Connecting to mysql by providing a sqlachemy engine
user=str(pwd_gen.loc[0, 'user'])
password=str(pwd_gen.loc[0, 'password'])
host=str(pwd_gen.loc[0, 'host'])
port=str(pwd_gen.loc[0, 'port'])
database=str(pwd_gen.loc[0, 'database'])

try:
    # connect to the MySQL server
    print("Connecting to the MySQL database...")
    engine = create_engine('mysql+mysqlconnector://'+user+':'+password+'@'+host+':'+port+'/'+database,
                            echo=False)
    connection = engine.connect()
    inspector = inspect(engine)

finally:
    if 'origin_greece' in inspector.get_table_names():
        print("Connection was successful!")

# Load the data to the data table gdp_b1_ga_vob
gdp_dataset_vob.to_sql(name='gdp_b1_ga_vob',
                       con=engine,
                       if_exists='append',
                       index=False)

# Load the data to the data table gdp_b1_ga_g
gdp_dataset_growth_rate.to_sql(name='gdp_b1_ga_g',
                               con=engine,
                               if_exists='append',
                               index=False)

# Close the MySQL connection
connection.close()
engine.dispose()