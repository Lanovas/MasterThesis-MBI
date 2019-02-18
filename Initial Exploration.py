# Import the necessary packages
import os
import glob
import pandas as pd
import pymysql

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

# Create a database with the full patent data set

# Data exploration
print(patent_set.shape)



#pymysql.connect