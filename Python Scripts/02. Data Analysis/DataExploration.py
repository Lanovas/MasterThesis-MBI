# Import the necessary packages
import numpy as np
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import inspect
import matplotlib.pyplot as plt
import seaborn as sns
import datetime

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

# Load the data from MySQL
patent_set = pd.read_sql(sql="SELECT * FROM origin_greece", con=connection)

# Close the MySQL connection
connection.close()
engine.dispose()

# Exploring the dataset
print(patent_set.columns)
patent_set.head(5)

# General date manipulations and cleaning
patent_set['first_filing_year'] = patent_set['first_filing_date'].astype(str).str[0:4]
patent_set['publication_year'] = patent_set['publication_date'].astype(str).str[0:4]
patent_set['publication_month'] = patent_set['publication_date'].astype(str).str[4:6]
patent_set['publication_week_adj'] = patent_set['publication_week'].astype(str).str[4:]
patent_set['publication_day'] = patent_set['publication_date'].astype(str).str[6:]

patent_set[['first_filing_year', 'publication_year', 'publication_month', 'publication_week_adj', 'publication_day']] = patent_set[['first_filing_year', 'publication_year', 'publication_month', 'publication_week_adj', 'publication_day']].apply(pd.to_numeric)
print(patent_set.dtypes)

patent_set['publication_date_adj'] = patent_set.apply(lambda x: datetime.date(x['publication_year'], x['publication_month'], x['publication_day']), axis=1)

# Average publication period after filling
patent_set['filing_to_publication_years'] = patent_set['publication_year'] - patent_set['first_filing_year']
print(round(patent_set['filing_to_publication_years'].mean(), ndigits=1))
bins_of_duration = pd.DataFrame(patent_set.groupby(['filing_to_publication_years']).count()['publication'])
print(bins_of_duration)
bins_of_duration.plot(kind='bar')
long_filing_patents = pd.DataFrame(patent_set[patent_set['filing_to_publication_years']==16])

bins_of_duration_per_year = pd.DataFrame(patent_set.groupby(['publication_year', 'filing_to_publication_years']).count()['publication'])
print(bins_of_duration_per_year)
#bins_of_duration_per_year.unstack().plot(kind='bar')

# Focus on the patents that have at least one Greek inventor in the list of inventors
# Check the presence of Greece in the inventor country
greek_inventor = pd.DataFrame(patent_set[patent_set['inventor_country'].str.contains("GR")==True])

# Number of patents that have at least one Greek inventor in the list
print(greek_inventor['publication_year'].min())
print(greek_inventor['publication_year'].max())
print(greek_inventor.shape[0])

# Plotting the number of patents per year
greek_patents_per_year = pd.DataFrame(greek_inventor.groupby(['publication_year']).count()['publication'])
print(greek_patents_per_year)
greek_patents_per_year.plot(kind='bar')

# Check the type of patent - CPC sections
# https://www.epo.org/searching-for-patents/helpful-resources/first-time-here/classification/cpc.html
cpc_classification = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'Y']
cpc_area = ['Human Necessities', 'Performing Operations-Transporting', 'Chemistry-Metallurgy', 'Textiles-Paper',
'Fixed Constructions', 'Mechanical Engineering (lighting, heating, weapons, blasting engines, pumps)',
'Physics', 'Electricity', 'General tagging of new technological developments']
cpc_code_match = list(zip(cpc_classification, cpc_area))
cpc_match = pd.DataFrame(cpc_code_match, columns=['cpc_classification', 'cpc_area'])

# Get the first character of the IPC column in order to determine the classification of the patent
print(greek_inventor.columns)
greek_inventor['cpc_classification'] = greek_inventor['ipc_full_level_invention_information'].str[:1]
# Match with the cpc_match, so as to get the cpc area
greek_inventor = greek_inventor.merge(cpc_match, on=['cpc_classification'], how='left')

# Fill the na values in place for better visualization
greek_inventor.fillna('Other', inplace=True)
# Count the publications per year and per type
greek_patents_per_type = pd.DataFrame(greek_inventor.groupby(['publication_year', 'cpc_area']).count()['publication']).reset_index()

fig, ax = plt.subplots()
for label, data in greek_patents_per_type.groupby('cpc_area'):
    data.plot(x='publication_year', y='publication', ax=ax, label=label)