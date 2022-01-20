import requests
import json


# Authentication parameters
headers = { 'AccountKey' : 'u5H3yfW+QkG06YbuaHSLIQ==', 'accept' : 'application/json'} #this is by default

# API parameters
uri = 'http://datamall2.mytransport.sg' #Resource URL
path = '/ltaodataservice/BusStops?$skip=5500'

# Build query string & specify type of API call
target = uri + path
print(target)

res = requests.get(target, headers=headers)
res_dict = json.loads(res.text)
value = res_dict.get("value")
print(len(value))
print(res_dict.keys())


