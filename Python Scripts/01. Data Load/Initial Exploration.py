# Import the necessary packages
import os
import glob
import pandas as pd
import psycopg2 as pypos

# Retrieve the working directory
cwd = os.getcwd()

# Load the data for analysis
path = "Python Scripts/00. Database/SourceData/Patent Data/"
extension = "csv"
allFiles = glob.glob(pathname = path + "/*." + extension)

patent_list = []
#patent_set = [i for i in glob.glob('*.{}'.format(extension))]
for file in allFiles:
    df = pd.read_csv(file, delimiter = ";", header = 0, index_col = None)
    patent_list.append(df)

patent_set = pd.concat(objs = patent_list, axis = 0, ignore_index = True)
print(patent_set.shape)

# Load the data to the data table patent_data
try:
    connection = pypos.connect(user=pwd_gen.loc[0, 'user'],
                               password=pwd_gen.loc[0, 'password'],
                               host=pwd_gen.loc[0, 'host'],
                               port=pwd_gen.loc[0, 'port'],
                               database=pwd_gen.loc[0, 'database'])
    cursor = connection.cursor()
    # Print PostgreSQL Connection properties
    print(connection.get_dsn_parameters(), "\n")

    # Print PostgreSQL version
    cursor.execute("SELECT version();")
    record = cursor.fetchone()
    print("You are connected to - ", record,"\n")
    
except (Exception, psycopg2.Error) as error :
    print ("Error while connecting to PostgreSQL", error)
finally:
    #closing database connection.
        if(connection):
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")



# Data exploration




