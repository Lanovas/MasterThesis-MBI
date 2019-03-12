import pandasdmx as pdmx

oecd_connection = pdmx.api.Request('OECD')
root_url = "https://stats.oecd.org/restsdmx/sdmx.ashx/GetData/"
dataset_identifier = "GOV"
start_time = "2007"
end_time = "2018"

try:
    data_response = oecd_connection.data(resource_id = dataset_identifier, key = 'all/all')
except UnicodeDecodeError:
    pass
except KeyError:
    pass
finally:
    oecd_dataset = data_response.data

    df = data_response.write(oecd_dataset.series, parse_time=False)