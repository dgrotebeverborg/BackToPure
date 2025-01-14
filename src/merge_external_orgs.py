import time
import pandas as pd
import logging
from logging_config import setup_logging
import requests
import csv
import json
import argparse
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import urllib3
from config import PURE_BASE_URL, PURE_API_KEY, PURE_HEADERS, RIC_BASE_URL, ROR_ID_URI, ORCID_ID_URI, OPENALEX_HEADERS
import enrich_pure_external_orgs as org
logger = setup_logging('btp', level=logging.INFO)


headers = {
    'Accept': 'application/json',
    'api-key': PURE_API_KEY,
}
# Set up a single session for all requests
session = requests.Session()
retry_strategy = Retry(
    total=5,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["HEAD", "GET", "OPTIONS", "PUT", "POST"],
    backoff_factor=1
)
adapter = HTTPAdapter(max_retries=retry_strategy)
session.mount("https://", adapter)
# Disable only the single InsecureRequestWarning from urllib3 needed to use the InsecureRequestWarning
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Initialize an empty list to store rows
rors = []
def fetch_org_data(rors, batch_size):
    """
    Fetch person data from the Pure API in batches and combine the results.

    Parameters:
    - person_df: DataFrame containing the person data with 'PURE_UUID_PERS' column.
    - headers: Headers required for the API requests.
    - base_url: Base URL of the Pure API.
    - batch_size: Number of records to fetch per batch.

    Returns:
    - datatotal: List containing all the combined data from each API call.
    """
    # Extract the list of UUIDs from the DataFrame

    total_records = len(rors)
    datatotal = []  # List to collect all data from the API responses
    total_found = 0  # Counter to keep track of the total number of items found
    # Loop through the list in batches

    for offset in range(0, total_records, batch_size):
        batch = rors[offset:offset + batch_size]
        pipe_separated_rors = "|".join(batch)
        json_data = {'searchString': pipe_separated_rors, 'size': len(batch), 'offset': 0}
        url = PURE_BASE_URL + 'external-organizations/search/'

        try:
            response = requests.post(url, headers=PURE_HEADERS, json=json_data)
            response.raise_for_status()
            response_data = response.json()
            batch_data = response_data.get('items', [])
            datatotal.extend(batch_data)  # Append batch response to datatotal
            total_found += response_data['count']
            logger.info(f"Successfully found  {str(response_data['count'])}, for {batch_size} uuids")
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed for offset {offset}: {e}")

    logger.info(f"total ext orgs found in pure:  {str(total_found)}")
    with open('extorgs.json', "w") as f:
        json.dump(datatotal, f, indent=4)

    # Return the combined data
    return datatotal

# Open and read the CSV file
with open('output.csv', mode='r') as file:
    reader = csv.reader(file)
    for row in reader:
        if len(row) > 1:
            rors.append(row[1])  # Index 1 corresponds to the second column

datatotal = fetch_org_data(rors, 20)

# Extract the uuid and ROR IDs for each entry
result = []
for entry in datatotal:
    uuid = entry.get('uuid')
    ror_ids = [identifier['id'] for identifier in entry.get('identifiers', [])
               if identifier.get('type') and identifier['type']['term'].get('en_GB') == 'ROR ID']

    if uuid and ror_ids:
        for ror_id in ror_ids:
            result.append({"uuid": uuid, "ror": ror_id})
# Cluster UUIDs by ROR, ensuring uniqueness
clustered_result = {}
for item in result:
    ror = item['ror']
    uuid = item['uuid']
    if ror not in clustered_result:
        clustered_result[ror] = set()  # Use a set to ensure unique UUIDs
    clustered_result[ror].add(uuid)


# Save only RORs that have more than one UUID
final_result = {ror: list(uuids) for ror, uuids in clustered_result.items() if len(uuids) > 1}




# Save JSON to file
with open('exorgs.json', 'w') as file:
    json.dump(final_result, file, indent=4)  # indent=4 makes the file more readable

# print(datatotal)
# pure_orgsjsons = org.fetch_pure_extorgs(uuids)

