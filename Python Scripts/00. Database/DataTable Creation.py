# Import the necessary packages
import pandas as pd
import psycopg2 as pypos

# Create a database with the full patent data set
pwd_gen = pd.read_csv(filepath_or_buffer="C:/Users/james/PycharmProjects/PWDGen.csv",
                      sep=";", encoding="UTF-8")

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
    print("You are connected to - ", record, "\n")

except (Exception, psycopg2.Error) as error:
    print("Error while connecting to PostgreSQL", error)
finally:
    # closing database connection.
    if (connection):
        cursor.close()
        connection.close()
        print("PostgreSQL connection is closed")
