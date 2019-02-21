# Import the necessary packages
import os
import glob
import pandas as pd
import psycopg2

# Retrieve the working directory
cwd = os.getcwd()

# Load the data for analysis
path = "SourceData/Patent Data/"
extension = "csv"
allFiles = glob.glob(pathname = path + "/*." + extension)

patent_list = []
#patent_set = [i for i in glob.glob('*.{}'.format(extension))]
for file in allFiles:
    df = pd.read_csv(file, delimiter = ";", header = 0, index_col = None)
    patent_list.append(df)

patent_set = pd.concat(objs = patent_list, axis = 0, ignore_index = True)
print(patent_set.shape)

# Create a database with the full patent data set
try:
    connection = psycopg2.connect(user = 'postgres',
                                  password = 'Depp1323!',
                                  host = '127.0.0.1',
                                  port = 5432,
                                  database = 'MasterThesisData')
    cursor = connection.cursor()
    # Print PostgreSQL Connection properties
    print(connection.get_dsn_parameters(), "\n")

    # Print PostgreSQL version
    cursor.execute("SELECT version();")
    record = cursor.fetchone()
    print("You are connected to - ", record,"\n")

except (Exception, psycopg2.Error) as error:
    print ("Error while connecting to PostgreSQL", error)

finally:
    #closing database connection.
        if(connection):
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")



# Data exploration




