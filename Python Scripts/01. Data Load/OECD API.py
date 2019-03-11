import pandasdmx as pdmx

oecd_connection = pdmx.Request('OECD')
root_url = "https://stats.oecd.org/restsdmx/sdmx.ashx/GetData/"
dataset_identifier = "GOV"
start_time = "2007"
end_time = "2018"

try:
    data_response = oecd_connection.data()
except:

except:

finally: