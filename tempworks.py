# Python3+ required #

import os, time, json, sys
from pymongo import pymongo, MongoClient, UpdateOne
import requests
import dns
from pprint import pprint

#Check Environment Variables Exist
#SID = TW sid, AUTH_TOKEN = tw auth_token, CONNECT_STRING = mongo URI, API_STRING = tw api uri
if "SID" and "AUTH_TOKEN" and "CONNECT_STRING" and "API_STRING" not in os.environ:
    print("Error: Set environment variables: SID, AUTH_TOKEN, CONNECT_STRING, API_STRING")
    sys.exit()
else:
    sid = os.getenv('SID')
    auth_token = os.getenv('AUTH_TOKEN')
    connect_string = os.getenv('CONNECT_STRING')
    api_string = os.getenv('API_STRING')


#Initialize Counter Variables:
skip_param = 0
items_returned = 1

#Intialize lists:
json_list = []
primaryBulkArr = []

#Process up to 20k records at a time, only while records are still being returned
while items_returned > 0:
    request_payload = {'take': '20000', 'skip': skip_param}
    tempworks_request = requests.get(api_string, auth=(sid, auth_token), params=request_payload)
    tempworks_response = tempworks_request.json()
    items_returned = int(tempworks_response['totalCount'])
    items = tempworks_response['data']
    old_skip_param = skip_param
    skip_param = skip_param + 20000
    if items:
        print("Processing %i - %i of %i total records" % (old_skip_param, skip_param, items_returned))
        json_list.append(items)
    time.sleep(2) #Pause 2 seconds for rate limiting


#MongoDB Operations
client = MongoClient(connect_string)
collection = client.partners.employees

for index, x in enumerate(json_list):
    splicer=json_list[index]
    for item in splicer:
        primaryBulkArr.append(
            UpdateOne({"employeeId": item['employeeId']}, {'$set': item}, upsert=True))

#Bulk Write Update
result = collection.bulk_write(primaryBulkArr)

#Operation Result Summary
pprint(result.bulk_api_result)
