import pandasdmx as pdmx

# Organisation for Economic Cooperation and Development (OECD)
# root_url = "https://stats.oecd.org/restsdmx/sdmx.ashx/GetData/"
dataset_identifier = "RMW"

oecd_connection = pdmx.api.Request('OECD')

test = oecd_connection.get(resource_type='data', resource_id=dataset_identifier, params={'startPeriod': '1960'})
test2 = test.write()

test2 = list(test.data.series)
len(test2)
test2[5].key
period = (s for s in test.data.series)

type(test2)


test3 = test.write(test2.series, parse_time=False)
print(test2.dim_at_obs)


try:
    data_response = oecd_connection.data(resource_id = dataset_identifier, key = 'all/all')
except UnicodeDecodeError:
    pass
except KeyError:
    pass
finally:
    oecd_dataset = data_response.data

    df = data_response.write(oecd_dataset.series, parse_time=False)