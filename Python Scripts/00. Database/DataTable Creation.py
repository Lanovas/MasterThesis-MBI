# Import the necessary packages
import pandas as pd
import mysql.connector
from mysql.connector import errorcode

# Connecting to the database
pwd_gen = pd.read_csv(filepath_or_buffer="C:/Users/james/PycharmProjects/PWDGen.csv", sep=";", encoding="UTF-8")

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
TABLES = {}
TABLES['origin_greece'] = (
    "CREATE TABLE `origin_greece` ("
    "`publication` varchar(200) NOT NULL,"
    "`publication_date` varchar(50),"
    "`publication_week` varchar(10),"
    "`publication_language` varchar(10),"
    "`first_filing_date` varchar(50),"
    "`ipc_full_level_invention_information` varchar(10000),"
    "`inventor_city` varchar(10000),"
    "`inventor_country` varchar(10000),"
    "`applicant_proprietor_country` varchar(10000),"
    "`representative_country` varchar(10000),"
    "`title_english` varchar(10000),"
    "PRIMARY KEY (`publication`),"
    "INDEX (`publication_date`)"
") ENGINE = InnoDB")

TABLES['oecd_rmw'] = (
    "CREATE TABLE `oecd_rmw` ("
    "`time_period` int NOT NULL,"
    "`country` varchar(50),"
    "`hourly_minimum_wage_ppp` numeric(12,2),"
    "PRIMARY KEY (`time_period`),"
    "INDEX (`time_period`)"
") ENGINE = InnoDB")

cursor = connection.cursor()

'''
def create_database(cursor):
    try:
        cursor.execute(
            "CREATE DATABASE {} DEFAULT CHARACTER SET 'utf8'".format(DB_NAME))
    except mysql.connector.Error as err:
        print("Failed creating database: {}".format(err))
        exit(1)

try:
    cursor.execute("USE {}".format(DB_NAME))
except mysql.connector.Error as err:
    print("Database {} does not exists.".format(DB_NAME))
    if err.errno == errorcode.ER_BAD_DB_ERROR:
        create_database(cursor)
        print("Database {} created successfully.".format(DB_NAME))
        cnx.database = DB_NAME
    else:
        print(err)
        exit(1)
'''

# Looping over the tables existing in the tables dictionary and executing the
# SQL commands necessary to create a table

for table_name in TABLES:
    table_description = TABLES[table_name]

    try:
        print("Creating table {}: ".format(table_name), end='')
        cursor.execute(table_description)
    except mysql.connector.Error as error:
        if error.errno == errorcode.ER_TABLE_EXISTS_ERROR:
            print("Table already exists.")
        else:
            print(error.msg)
    else:
        print("Table created successfully!")

cursor.close()

# closing database connection
if (connection):
    connection.close()
    print("MySQL connection is closed")