# Import the necessary packages
import os
import glob
import pandas as pd
import mysql.connector
from mysql.connector import errorcode

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

# Connecting to the database
pwd_gen = pd.read_csv(filepath_or_buffer="C:/Users/james/PycharmProjects/PWDGen.csv",
                      sep=";", encoding="UTF-8")

try:
    # connect to the MySQL server
    print("Connecting to the MySQL database...")

    connection = mysql.connector.connect(user=pwd_gen.loc[0, 'user'],
                                         password=pwd_gen.loc[0, 'password'],
                                         host=pwd_gen.loc[0, 'host'],
                                         port=pwd_gen.loc[0, 'port'],
                                         database=pwd_gen.loc[0, 'database'])

except mysql.connector.Error as error:

    if error.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print("Something is wrong with your user name or password")
    elif error.errno == errorcode.ER_BAD_DB_ERROR:
        print("Database does not exist")
    else:
        print(error)
finally:
    if (connection):
        print("Connection was successful!")

# Load the data to the data table patent_data



cursor.close()

# closing database connection
if (connection):
    connection.close()
    print("MySQL connection is closed")

# Data exploration




