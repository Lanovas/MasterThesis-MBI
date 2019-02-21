# Import the necessary packages
import pandas as pd
import psycopg2 as pypos

# Create a database with the full patent data set
pwd_gen = pd.read_csv(filepath_or_buffer="C:/Users/james/PycharmProjects/PWDGen.csv",
                      sep=";", encoding="UTF-8")

try:
    # connect to the PostgreSQL server
    print("Connecting to the PostgreSQL database...")

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

    # Create the database tables needed for the patent data
    commands = (
        """
        CREATE TABLE patent_data(
        
        
        )
        
        """
    )

    # close the communication with the PostgreSQL
    cursor.close()

except (Exception, pypos.Error) as error:
    print("Error while connecting to PostgreSQL", error)
finally:
    # closing database connection.
    if (connection):
        cursor.close()
        connection.close()
        print("PostgreSQL connection is closed")