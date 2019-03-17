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
    print("Successful Connection ot the API!")
    gdp_series = list(gdp_connection.data.series)
    # Explore the resulted dataset
    print(len(gdp_series))




    print(gdp_series[122].key)

rmw_dataset = rmw_connection.write(s for s in rmw_connection.data.series if s.key.PERIOD == 'H')
rmw_dataset = pd.DataFrame(rmw_dataset.to_records())
print(type(rmw_dataset))
print(rmw_dataset.shape)

# Data cleaning - staging


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

# Load the data to the data table oecd_rmw
print(rmw_melted.info())
print(rmw_melted.describe())
rmw_melted['time_period'] = rmw_melted['time_period'].astype(str).astype(int)

rmw_melted.to_sql(name='oecd_rmw',
                  con=engine,
                  if_exists='append',
                  index=False)

# Close the MySQL connection
connection.close()
engine.dispose()