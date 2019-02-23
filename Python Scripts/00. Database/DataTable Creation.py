# Import the necessary packages
import pandas as pd
import mysql.connector
from mysql.connector import errorcode

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

# Create a database with the full patent data set




# closing database connection
if (connection):
    connection.close()
    print("MySQL connection is closed")
