# Import the necessary packages
import os
import glob
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import inspect

# Retrieve the working directory
cwd = os.getcwd()

# Load the data for analysis
path = "Python Scripts/00. Database/SourceData/Patent Data/"
extension = "csv"
allFiles = glob.glob(pathname=path + "/*." + extension)

# Merge all the csv's together
patent_list = []
cols_to_read = ['Publication', 'Publication date', 'Publication week',
                'Publication language', 'First filing date', 'IPC full level (invention information)',
                'Inventor (city)', 'Inventor (country)',  'Applicant / Proprietor (country)',
                'Representative (country)', 'Title (en)']

#patent_set = [i for i in glob.glob('*.{}'.format(extension))]
for file in allFiles:
    df = pd.read_csv(file, delimiter=";", header=0, index_col=None, usecols=cols_to_read)
    patent_list.append(df)

patent_set = pd.concat(objs=patent_list, axis=0, ignore_index=True)

# Stage the dataset for loading it into MySQL
print(patent_set.shape)
print(patent_set.columns.values)

patent_set.columns = [x.lower() for x in patent_set.columns]
patent_set.columns = patent_set.columns.str.replace(' ', '_')
patent_set.columns = patent_set.columns.str.replace('(', '')
patent_set.columns = patent_set.columns.str.replace(')', '')
patent_set.columns = patent_set.columns.str.replace('/_', '')
patent_set.columns = patent_set.columns.str.replace('_en', '_english')

# Connecting to the database
pwd_gen = pd.read_csv(filepath_or_buffer="C:/Users/james/PycharmProjects/PWDGen.csv",
                      sep=";", encoding="UTF-8")

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

# Load the data to the data table patent_data
patent_set.to_sql(name='oigin_greece',
                  con=engine,
                  if_exists='append',
                  index=False)
connection.close()
engine.dispose()

data = pd.read_sql('SELECT * FROM origin_greece', engine_connection_mysql)


# closing database connection
if (connection):
    connection.close()
    print("MySQL connection is closed")

# Data exploration




