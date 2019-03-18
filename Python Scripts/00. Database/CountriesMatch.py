from IPython import get_ipython
get_ipython().magic('reset -sf')
import pandas as pd
import os
from sqlalchemy import create_engine
from sqlalchemy import inspect
print('The current working directory is: ' + os.getcwd())

countries = pd.read_excel('Python Scripts/00. Database/SourceData/Country Codes.xls',
                          sheetname='Code Country')

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

# Load the table to the db
countries.columns = ['country_code', 'country_full_name']

countries.to_sql(name='countries_match',
                 con=engine,
                 if_exists='append', index=False)

# Close the MySQL connection
connection.close()
engine.dispose()