"""
================================================================================
Script Name: enrich_internal_persons_with_ids.py
Author: David Grote Beverborg

================================================================================

Overview:
----------
This script is part of the BackToPure project, an initiative that integrates data between the Ricgraph and Pure systems.
Specifically, this script enriches internal persons' records within the Pure system by ensuring that each person
has the appropriate identifiers associated with their profile, such as ORCID, ResearcherID, etc.

Execution:
-----------
This script is designed to be executed exclusively through the BackToPure web interface, rather than directly from the command line.
The BackToPure web application provides a user-friendly interface for selecting options, such as choosing a faculty and specifying
whether to run in test mode. When the user initiates the script from the website, the following occurs:
1. The web interface collects user input (e.g., faculty selection, test mode).
2. The Flask backend processes the input and executes this script with the appropriate parameters.
3. The script’s output, including logs and any updates made to the Pure system, is streamed back to the user through the web interface.

This approach allows for easy access and operation of the script without requiring direct interaction with the code, making it more
accessible to users who may not be familiar with Python or command-line operations.

Process:
---------
1. **Faculty Selection:**
   - The script begins by fetching a list of faculties from Ricgraph.
   - The user, via the web interface, selects a specific faculty or chooses all faculties for processing.

2. **Person Root Node Retrieval:**
   - For the selected faculty, the script retrieves all person-root nodes from Ricgraph.
   - It then fetches the person IDs associated with these root nodes.

3. **Enrichment Check:**
   - The script checks if each person already has the required identifiers in Pure.
   - If an identifier is missing, it prepares to update the person's profile in Pure with the missing information.

4. **Person Data Update:**
   - The script generates a JSON object representing the updated person data.
   - If the script is not running in test mode, it sends an update request to the Pure API to enrich the person's profile.

5. **Result Logging:**
   - Detailed logs are generated throughout the process to track the progress and any issues encountered.
   - The results, including the number of persons updated, are displayed on the web interface at the end of the script's execution.

Purpose within BackToPure and Ricgraph:
---------------------------------------
The BackToPure project aims to synchronize and enrich data between Ricgraph, a data storage and query system,
and Pure, a research information management system. This script plays a critical role in ensuring that internal
person profiles in Pure are accurate and up-to-date, which is essential for research reporting, collaboration,
and institutional data management.

By using data from Ricgraph, the script ensures that all relevant identifiers are associated with the correct
person in Pure. This enrichment process not only enhances data integrity but also supports better research visibility
and management within the university's research ecosystem.

Open Source and Licensing:
---------------------------
This script, along with the entire BackToPure project, is open-source software, meaning that anyone can view, modify,
and distribute the code. The project is licensed under the MIT License, which is a permissive free software license.
This allows for maximum freedom in how the software is used, including in proprietary software.

MIT License:
------------
Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation
files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy,
modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software
is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF
OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
"""

import argparse
import logging
import requests
import pandas as pd
import math
import json
from config import PURE_BASE_URL, PURE_API_KEY, PURE_HEADERS, RIC_BASE_URL, ID_URI, FACULTY_PREFIX
from logging_config import setup_logging

#Setup logger

logger = setup_logging('btp', level=logging.INFO)

def is_nan(value):
    """
       Checks if a given value is NaN (Not a Number).
    """
    return value is None or (isinstance(value, float) and math.isnan(value))

def fetch_personroots(faculty_key):

    try:
        params = {'key': faculty_key, 'max_nr_items': '0'}
        url = RIC_BASE_URL + 'get_all_personroot_nodes'
        response = requests.get(url, params=params)
        # print(response, faculty_key)
        response.raise_for_status()
        return response.json().get("results", [])
    except requests.RequestException as e:
        logger.error(f"Error fetching person-roots for faculty {faculty_key}: {e}")
        return []

def fetch_person_ids(persoonroot_key):
    try:
        params = {'key': persoonroot_key, 'category_want': 'person'}
        url = RIC_BASE_URL + 'get_all_neighbor_nodes'
        response = requests.get(url, params=params)
        response.raise_for_status()
        return response.json().get("results", [])
    except requests.RequestException as e:
        logger.error(f"Error fetching person IDs for person-root {persoonroot_key}: {e}")
        return []

def checkenrichement(persoonroot_key):
    try:
        params = {'key': persoonroot_key, 'source_system': 'pure uu', 'max_nr_items': 0}
        url = RIC_BASE_URL + 'person/enrich'
        response = requests.get(url, params=params)
        response.raise_for_status()
        return response.json().get("results", [])
    except requests.RequestException as e:
        logger.error(f"Error fetching person IDs for person-root {persoonroot_key}: {e}")
        return []

def select_persons(faculties, faculty_choice):
    persons = []
    count = 0

    for faculty in faculties:
        logger.info(f"Processing faculty: {faculty}")
        personroots = fetch_personroots(faculty)
        logger.info(f"Processing {str(len(personroots))} persons in ricgraph")

        for personroot in personroots:
            count += 1
            if count % 250 == 0:
                logger.debug(f"Processed {str(count)} persons in ricgraph")

            # enrich = checkenrichement(persoonroot_key)
            personids = fetch_person_ids(personroot['_key'])
            persons.extend([
                [personroot['_key'], personid['name'], personid['value']]
                for personid in personids
            ])

    df = pd.DataFrame(persons, columns=['person_id', 'id_name', 'id_value'])
    df_aggregated = df.groupby(['person_id', 'id_name'], as_index=False).agg(lambda x: ' | '.join(x))
    persondf = df_aggregated.pivot(index='person_id', columns='id_name', values='id_value').reset_index()

    # Ensure PURE_UUID_PERS is added or handled properly
    if 'PURE_UUID_PERS' not in persondf.columns:
        persondf['PURE_UUID_PERS'] = pd.NA
    logger.info(f"Processing faculty: {str(persondf.shape[0])}")
    file_path = "person_ids.xlsx"
    persondf.to_excel(file_path, index=False)
    return persondf

def update_person(new_ids, data, api_url):
    for new in new_ids:
        new_identifier = {
            'typeDiscriminator': 'ClassifiedId',
            'id': new['id'],
            'type': {'uri': new['uri']}
        }
        if 'identifiers' in data:
            data['identifiers'].append(new_identifier)
        else:
            data['identifiers'] = [new_identifier]
    response2 = requests.put(api_url, headers=PURE_HEADERS, json=data)


def check_new_ids(row, data):
    identifiers = data.get('identifiers', [])
    existing_ids = {entry.get('id') or entry.get('value'): entry for entry in identifiers}
    new_ids = []
    different_ids_values = []
    orcid = ''
    orcidchange = ''
    for key, value in row.items():

        if key == 'ORCID':
            orcid = value
        if key in ID_URI and not is_nan(value):
            id_type_uri = ID_URI[key]
            found = False
            for entry in identifiers:
                if entry.get('type', {}).get('uri') == id_type_uri:
                    found = True
                    if (entry.get('id') or entry.get('value')) != value:
                        different_ids_values.append({
                            'id': value,
                            'uri': id_type_uri,
                            'existing_id': entry.get('id') or entry.get('value')
                        })
            if not found:
                new_ids.append({'id': value, 'uri': id_type_uri})
    if 'orcid' not in data:
        if isinstance(orcid, float) and math.isnan(orcid):
            pass
        else:
            if orcid:
                data['orcid'] = orcid
                orcidchange = 'X'
    return new_ids, data, orcidchange, orcid

def find_item_by_uuid(data, target_uuid):

    for item in data:
        if item.get('uuid') == target_uuid:

            return item
    return None

def select_faculties(faculty_choice):

    params = {'value': FACULTY_PREFIX}
    url = f"{RIC_BASE_URL}organization/search"

    try:
        response = requests.get(url, params=params)
        response.raise_for_status()  # Raises an HTTPError if the HTTP request returned an unsuccessful status code
    except requests.RequestException as e:
        raise SystemExit(f"Error fetching faculties: {e}")

    try:
        data = response.json()
    except ValueError:
        raise SystemExit("Failed to decode JSON from response.")

    # Extract faculties or use the provided choice
    if faculty_choice.lower() == 'all':
        selected_faculties = [item.get('_key') for item in data.get("results", [])]
    else:
        selected_faculties = [faculty_choice]

    return selected_faculties


def fetch_person_data(person_df, batch_size):
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
    id_list = person_df['PURE_UUID_PERS'].tolist()
    total_records = len(id_list)
    datatotal = []  # List to collect all data from the API responses
    total_found = 0  # Counter to keep track of the total number of items found
    # Loop through the list in batches

    for offset in range(0, total_records, batch_size):
        batch = id_list[offset:offset + batch_size]
        json_data = {'uuids': batch, 'size': len(batch), 'offset': 0}
        url = PURE_BASE_URL + 'persons/search/'

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

    logger.info(f"total persons found in pure:  {str(total_found)}")
    with open('purepersons.json', "w") as f:
        json.dump(datatotal, f, indent=4)

    # Return the combined data
    return datatotal


def update_persons(person_df, datatotal, test_choice):
    count = 0
    succes = 0
    for index, row in person_df.iterrows():
        count += 1
        if count % 250 == 0:
            logger.info(f"Processed {str(count)} persons in pure")
        logger.debug(f"checking person:  {row['PURE_UUID_PERS']}")
        data = find_item_by_uuid(datatotal, row['PURE_UUID_PERS'])
        if data:
            new_ids, data, orcidchange, orcid = check_new_ids(row, data)
            if new_ids or orcidchange:
                succes += 1
                logger.debug(f"new ids: {new_ids} {orcid}")
                api_url = PURE_BASE_URL + 'persons/' + row['PURE_UUID_PERS']
                if test_choice == 'no':
                    update_person(new_ids, data, api_url)
        else:
            logger.debug(f"{row['PURE_UUID_PERS']} not found in pure")
    logger.info(f"total persons updated (if no test run):  {succes}")


def main(faculty_choice, test_choice):
    logger.info(f"Script enrich persons has started")
    logger.info(f"Test run =  {test_choice}")
    faculties = select_faculties(faculty_choice)
    person_df = select_persons(faculties, faculty_choice)

    if not person_df.empty:
       datatotal = fetch_person_data(person_df, 100)
       update_persons(person_df, datatotal, test_choice)
    logger.info(f"Script enrich persons has ended")



# ########################################################################
# MAIN
# ########################################################################
if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Enrich Internal Persons with IDs.')
    parser.add_argument('faculty_choice', type=str, nargs='?',
                        default='uu faculty: faculteit rebo|organization_name',
                        # default='all',
                        help='Faculty choice or "all"')
    parser.add_argument('test_choice', type=str, nargs='?', default='yes', help='Run in test mode ("yes" or "no")')

    args = parser.parse_args()

    main(args.faculty_choice, args.test_choice)


