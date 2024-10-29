import argparse
import logging
import requests
import pandas as pd
import math
import json
from config import PURE_BASE_URL, PURE_API_KEY, PURE_HEADERS, RIC_BASE_URL, ID_URI

def find_item_by_uuid(data, target_uuid):
    # items = data.get('items', [])

    for item in data:
        if item.get('uuid') == target_uuid:
            print('blabla')
            return item
    return None

batch = ['6b484d94-8263-4156-9fb4-d5f863b4ea56' , 'b40327c0-c00a-49df-acd6-7ea4f65447e0']
json_data = {'uuids': batch, 'size': len(batch), 'offset': 0}
url = PURE_BASE_URL + 'persons/search/'
response = requests.post(url, headers=PURE_HEADERS, json=json_data)

response_data = response.json()
batch_data = response_data.get('items', [])



for item in batch:
    data = find_item_by_uuid(batch_data, item)