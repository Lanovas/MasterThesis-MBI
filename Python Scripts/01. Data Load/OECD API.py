import pandasdmx as pdmx
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import inspect

# Organisation for Economic Cooperation and Development (OECD)
# root_url = "https://stats.oecd.org/restsdmx/sdmx.ashx/GetData/"
oecd_connection = pdmx.api.Request('OECD')

# ----------------------- Real minimum wages (RMW) ----------------------- #
# Real hourly and annual minimum wages are statutory minimum wages
# converted into a common hourly and annual pay period for the 28 OECD countries
# and 4 non-member countries for which they are available. The resulting estimates
# are deflated by national Consumer Price Indices (CPI). The data are then converted
# into a common currency unit using either US $ current exchange rates or US $ Purchasing
# Power Parities (PPPs) for private consumption expenditures.
# Real hourly and annual minimum wages are calculated first by deflating the series using the consumer
# price index taking 2017 as the base year.  The series are then converted into a common currency unit (USD)
# using Purchasing Power Parities (PPPs) for private consumption expenditures in 2017.

# Data importing
dataset_identifier = "RMW"

try:
    rmw_connection = oecd_connection.get(resource_type='data', resource_id=dataset_identifier, params={'startPeriod': '1960'})
except UnicodeDecodeError:
    print("UnicodeDecodeError...")
except KeyError:
    print("KeyError...")
finally:
    print("Successful Connection ot the API!")
    rmw_series = list(rmw_connection.data.series)
    # Explore the resulted dataset
    print(len(rmw_series))
    print(rmw_series[122].key)

    rmw_dataset = rmw_connection.write(s for s in rmw_connection.data.series if s.key.PERIOD == 'H')
    rmw_dataset = pd.DataFrame(rmw_dataset.to_records())
    print(type(rmw_dataset))
    print(rmw_dataset.shape)

# Data cleaning - staging
rmw_dataset.columns = rmw_dataset.columns.str.replace('(', '')
rmw_dataset.columns = rmw_dataset.columns.str.replace(')', '')
rmw_dataset.columns = rmw_dataset.columns.str.replace("'", '')
rmw_dataset.columns = rmw_dataset.columns.str.replace(', ', '_')

rmw_melted = pd.melt(frame=rmw_dataset, id_vars=['TIME_PERIOD'], var_name='country_type', value_name='hourly_minimum_wage')
rmw_melted.columns = [x.lower() for x in rmw_melted.columns]
rmw_melted['country_type'] = rmw_melted['country_type'].str.replace('_H', '')
rmw_melted['country'], rmw_melted['type'] = rmw_melted['country_type'].str.split('_', 1).str
rmw_melted = rmw_melted.loc[rmw_melted['type'] == 'PPP']

del rmw_melted['country_type']
del rmw_melted['type']

rmw_melted = rmw_melted.rename(columns={'hourly_minimum_wage':'hourly_minimum_wage_ppp'})
rmw_melted = rmw_melted[['time_period', 'country', 'hourly_minimum_wage_ppp']]
rmw_melted['hourly_minimum_wage_ppp'].fillna(0, inplace=True)

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