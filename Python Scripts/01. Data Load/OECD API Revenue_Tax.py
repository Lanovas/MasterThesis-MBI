from IPython import get_ipython
get_ipython().magic('reset -sf')
import pandasdmx as pdmx
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import inspect

# Organisation for Economic Cooperation and Development (OECD)
# root_url = "https://stats.oecd.org/restsdmx/sdmx.ashx/GetData/"
oecd_connection = pdmx.api.Request('OECD')

# ----------------------- Public Sector, Taxation, Regulation (AWCOMP) ----------------------- #
# These tables, which are also reported in the OECD Taxing Wages publication, provide unique
# information for each of the OECD countries on the income taxes paid by workers, their social
# security contributions, the family benefits they receive in the form of cash transfers as well
# as the social security contributions and payroll taxes paid by their employers.
# The amounts of taxes and social security contributions paid and cash benefits received are set out,
# programme by programme, for 8 different household types characterised by marital status, number
# of children, earnings levels expressed as proportion of average wages and whether there are one or two earners.
# The results reported include the average and marginal tax burden for each household type . These data on tax
# burdens and cash benefits are widely used in academic research and the preparation and evaluation of social
# economic policy-making.

# Data importing
dataset_identifier = "AWCOMP"

try:
    dataset_connection = oecd_connection.get(resource_type='data', resource_id=dataset_identifier, params={'startPeriod': '1960'})
except UnicodeDecodeError:
    print("UnicodeDecodeError...")
except KeyError:
    print("KeyError...")
finally:
    print("Successful Connection to the API!")
    dataset_series = list(dataset_connection.data.series)
    # Explore the resulted dataset
    print("The length of the resulted data set is: " + str(len(dataset_series)))

i = 0
while i <= 1000:
    print(dataset_series[i].key)
    i += 1

# i = 0
# unique_list = []
# while i <= len(dataset_series):
    # check if exists in unique_list or not
#   if dataset_series[i].key not in unique_list:
#        unique_list.append(dataset_series[i].key)
#    i += 1

# Extracting the Total Gross Earnings before taxes in national currencies
dataset_key = dataset_connection.write(s for s in dataset_connection.data.series if s.key.INDICATOR == '1_1')
total_gross_before_taxes_dataset = pd.DataFrame(dataset_key.to_records())

# Extracting the Average Income Tax Rate (percentage)
dataset_key = dataset_connection.write(s for s in dataset_connection.data.series if s.key.INDICATOR == '2_1')
average_income_tax_rate_dataset = pd.DataFrame(dataset_key.to_records())

# Extracting the Average Tax Wedge
dataset_key = dataset_connection.write(s for s in dataset_connection.data.series if s.key.INDICATOR == '2_6')
average_tax_wedge_dataset = pd.DataFrame(dataset_key.to_records())

print(type(total_gross_before_taxes_dataset))
print(total_gross_before_taxes_dataset.shape)

print(type(average_income_tax_rate_dataset))
print(average_income_tax_rate_dataset.shape)

print(type(average_tax_wedge_dataset))
print(average_tax_wedge_dataset.shape)

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
total_gross_before_taxes_dataset = column_names_cleaning(oecd_dataset=total_gross_before_taxes_dataset)
average_income_tax_rate_dataset = column_names_cleaning(oecd_dataset=average_income_tax_rate_dataset)
average_tax_wedge_dataset = column_names_cleaning(oecd_dataset=average_tax_wedge_dataset)

# Define the function for reshaping the data sets
def reshape_dataset(oecd_dataset):
    '''
    Creating a function for reshaping the data set returned from the OECD API.
    This way allows the user to copy paste the function across scripts and perform time-consuming operations.
    Provide the OECD data frame as an input data set.
    :param oecd_dataset:
    :return:
    '''
    oecd_dataset = pd.melt(frame=oecd_dataset, id_vars=['TIME_PERIOD'], var_name='family_country', value_name='metric')
    oecd_dataset.columns = [x.lower() for x in oecd_dataset.columns]
    oecd_dataset['family_country'] = oecd_dataset['family_country'].str.replace('1_1_', '')
    oecd_dataset['family_country'] = oecd_dataset['family_country'].str.replace('2_1_', '')
    oecd_dataset['family_country'] = oecd_dataset['family_country'].str.replace('2_6_', '')
    oecd_dataset['metric'].fillna(0, inplace=True)
    oecd_dataset['time_period'] = oecd_dataset['time_period'].astype(str).astype(int)

    oecd_dataset['family_type'], oecd_dataset['country'] = oecd_dataset['family_country'].str.split('_', 1).str
    oecd_dataset.drop('family_country', axis=1, inplace=True)
    oecd_dataset = oecd_dataset[['time_period', 'country', 'family_type', 'metric']]

    return oecd_dataset

# Apply the function
total_gross_before_taxes_dataset = reshape_dataset(oecd_dataset=total_gross_before_taxes_dataset)
total_gross_before_taxes_dataset = total_gross_before_taxes_dataset.rename(columns={total_gross_before_taxes_dataset.columns[3]:'total_gross'})

average_income_tax_rate_dataset = reshape_dataset(oecd_dataset=average_income_tax_rate_dataset)
average_income_tax_rate_dataset = average_income_tax_rate_dataset.rename(columns={average_income_tax_rate_dataset.columns[3]:'average_income_tax'})

average_tax_wedge_dataset = reshape_dataset(oecd_dataset=average_tax_wedge_dataset)
average_tax_wedge_dataset = average_tax_wedge_dataset.rename(columns={average_tax_wedge_dataset.columns[3]:'average_tax_wedge'})

# Check the structure before sending the data sets to the DB
print(total_gross_before_taxes_dataset.info())
print(average_income_tax_rate_dataset.info())
print(average_tax_wedge_dataset.info())

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
    engine = create_engine('mysql+mysqlconnector://'+user+':'+password+'@'+host+':'+port+'/'+database, echo=False)
    connection = engine.connect()
    inspector = inspect(engine)

finally:
    if 'origin_greece' in inspector.get_table_names():
        print("Connection was successful!")

# Load the data to the data table oecd_gross_revenue, oecd_average_income_tax and
# the data table oecd_average_tax_wedge if they have rows, otherwise
# use the data sets already in the database

if len(total_gross_before_taxes_dataset) > 0 and len(average_income_tax_rate_dataset) and len(average_tax_wedge_dataset) > 0:
    total_gross_before_taxes_dataset.to_sql(name='oecd_gross_revenue',
                                            con=engine,
                                            if_exists='replace',
                                            index=False)

    average_income_tax_rate_dataset.to_sql(name='oecd_average_income_tax',
                                           con=engine,
                                           if_exists='replace',
                                           index=False)

    average_tax_wedge_dataset.to_sql(name='oecd_average_tax_wedge',
                                     con=engine,
                                     if_exists='replace',
                                     index=False)
else:
    total_gross_before_taxes_dataset = pd.read_sql_query(sql="SELECT * FROM oecd_gross_revenue", con=engine)
    average_income_tax_rate_dataset = pd.read_sql_query(sql="SELECT * FROM oecd_average_income_tax", con=engine)
    average_tax_wedge_dataset = pd.read_sql_query(sql="SELECT * FROM oecd_average_tax_wedge", con=engine)

# Close the MySQL connection
connection.close()
engine.dispose()

# Data Analysis - Visualizations