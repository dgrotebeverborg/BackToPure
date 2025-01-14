# ########################################################################
# Script: enrich_pure_external_orgs.py
#
# Description:
# This script enriches external organization records in Pure using data from
# Ricgraph and OpenAlex. The main task is to ensure that external organizations
# in Pure have up-to-date information, such as ROR IDs and geographic data.
#
# The script includes:
# - Fetching research outputs and associated organizations.
# - Matching organizations between Pure and OpenAlex.
# - Updating external organizations in Pure with ROR IDs if missing.
#
# Important:
# This script relies on external APIs (Pure and OpenAlex) and should be run
# with necessary configurations in place.
# This script is meant to be invoked by the BackToPure web interface.
#
# Dependencies:
# - requests, pandas, json, logging, urllib3, etc.
#
# Author: David Grote Beverborg
# Created: 2024
#
# License:
# MIT License
#
# Copyright (c) 2024 David Grote Beverborg
# ########################################################################


import time
import csv
import pandas as pd
import logging
from logging_config import setup_logging
import requests
import enrich_pure_external_persons as enrich
import json
import argparse
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import urllib3
import os
from config import PURE_BASE_URL, PURE_API_KEY, PURE_HEADERS, RIC_BASE_URL, ROR_ID_URI, ORCID_ID_URI, OPENALEX_HEADERS

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

def match_organizations(pure_orgs, openalex_orgs, ):
    # Initialize the new list to store the organizations to update
    orgs_to_update = []

    # Loop over each organization in pure_orgs
    for pure_org in pure_orgs:
        pure_org_name = pure_org['name']  # Extract the name of the Pure organization

        # Loop over each organization in openalex_orgs
        for openalex_org in openalex_orgs:
            # Extract the display name and alternatives from the OpenAlex organization
            openalex_display_name = openalex_org['display_name']
            openalex_alternatives = openalex_org.get('display_name_alternatives', [])

            # Check if the Pure organization name matches the OpenAlex display name or any of its alternatives
            if pure_org_name == openalex_display_name or pure_org_name in openalex_alternatives:
                # If a match is found, create a dictionary with the required details
                matched_org = {
                    'uuid': pure_org['uuid'],  # Pure organization UUID
                    'openalex_id': openalex_org['openalex_id'],  # OpenAlex organization ID
                    "ror": openalex_org['ror'],
                    'geo': openalex_org['geo']  # Geographic information from OpenAlex
                }
                # Append the matched organization to the orgs_to_update list
                orgs_to_update.append(matched_org)
                break  # Exit the inner loop since a match is found for this Pure organization


    return orgs_to_update

def match_orgs_oa_pure(oa_article, pure_article, article_orgs, uuids, oa_ids):
    # Initialize a dictionary to store unique institutions
    oa_unique_institutions = {}
    # Iterate over the authorships to extract institutions
    if oa_article['authorships'] is not None:
        for authorship in oa_article['authorships']:
            institutions = authorship.get('institutions', [])
            for institution in institutions:
                inst_id = institution.get('id')
                display_name = institution.get('display_name')
                ror = institution.get('ror')

                oa_ids.add(ror)

                # Check if the institution is already added using its OpenAlex ID
                if inst_id and inst_id not in oa_unique_institutions:
                    oa_unique_institutions[inst_id] = {
                        'openalex_id': inst_id,
                        'display_name': display_name,
                        'ror': ror
                    }

    # Initialize a set to store unique external organization UUIDs
    external_organization_uuids = set()

    contributors = pure_article.get('contributors', [])

    if not contributors:
        logger.info("No contributors found in the item.")
    else:

        # Loop through each contributor
        for contributor in contributors:
            # Check for external organizations associated with the contributor
            external_orgs = contributor.get('externalOrganizations', [])
            for ext_org in external_orgs:
                uuid = ext_org.get('uuid')
                if uuid:
                    external_organization_uuids.add(uuid)

    # Extract from top-level 'externalOrganizations' section
    top_level_external_orgs = pure_article.get('externalOrganizations', [])
    for ext_org in top_level_external_orgs:
        uuid = ext_org.get('uuid')
        if uuid:
            external_organization_uuids.add(uuid)

    # Convert set to a list
    external_organization_uuids = list(external_organization_uuids)
    data_entry = {
        'doi': oa_article['doi'],
        'external_organization_uuids': external_organization_uuids,
        'unique_institutions': oa_unique_institutions
    }
    article_orgs.append(data_entry)
    uuids.update(external_organization_uuids)

    return article_orgs, uuids, oa_ids

def identifier_exists(identifiers, new_id, id_type_uri):

    for identifier in identifiers:
        if 'type' in identifier and identifier['type']['uri'] == id_type_uri and identifier['id'] == new_id:
            return True
    return False
def update_externalorg_pure(orgs, test_choice, update):
    # Set up retry strategy
    retry_strategy = Retry(
        total=5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["HEAD", "GET", "OPTIONS", "PUT"],
        backoff_factor=1
    )

    adapter = HTTPAdapter(max_retries=retry_strategy)

    # Directory to save the output files
    output_dir = "output/external_orgs"
    os.makedirs(output_dir, exist_ok=True)

    # Initialize a list to store rows for the DataFrame
    rows_to_update = []

    # Initialize a list to store JSON objects
    json_updates = []

    for row in orgs:
        session = requests.Session()
        session.mount("https://", adapter)

        headers = {
            'Accept': 'application/json',
            'api-key': PURE_API_KEY,
        }

        url = PURE_BASE_URL + 'external-organizations/' + row['uuid']
        response = session.get(url, headers=headers, verify=False)
        logging.debug(f"get org data {row['uuid']}. responsecode = {response.status_code}")
        data = response.json()  # Parse JSON response
        new_ror = None

        if row['ror']:
            new_ror = {
                "typeDiscriminator": "ClassifiedId",
                "id": row['ror'],
                "type": {
                    "uri": ROR_ID_URI,
                    "term": {
                        "en_GB": "ROR ID"
                    }
                }
            }

        if 'identifiers' not in data:
            data['identifiers'] = []

        # Check if the new ROR is already in the identifiers
        if new_ror and new_ror not in data['identifiers']:
            # Mark as "to be updated"
            rows_to_update.append({

                'to_be_updated': 'X',
                'updated': ' ',
                'uuid': row['uuid'],
                'ror': row['ror']
            })

            # Add the JSON to the big JSON list
            json_updates.append(data)

        session.close()

    # Save the DataFrame
    df = pd.DataFrame(rows_to_update)
    df.to_csv(os.path.join(output_dir, "external_orgs_to_update.csv"), index=False)

    # Save the big JSON file
    with open(os.path.join(output_dir, "external_orgs_updates.json"), 'w') as json_file:
        json.dump(json_updates, json_file, indent=4)
    # for row in orgs:
    #     session = requests.Session()
    #     session.mount("https://", adapter)
    #
    #     headers = {
    #         'Accept': 'application/json',
    #         'api-key': PURE_API_KEY,
    #     }
    #
    #     url = PURE_BASE_URL +  'external-organizations/' + row['uuid']
    #     response = session.get(url, headers=headers, verify=False)
    #     logging.debug(f"get org data {row['uuid']}. responsecode = {response.status_code}")
    #     data = response.json()  # Directly parse JSON response
    #     new_ror = None
    #     if row['ror']:
    #         new_ror = {
    #             "typeDiscriminator": "ClassifiedId",
    #             "id": row['ror'],
    #             "type": {
    #                 "uri": ROR_ID_URI,
    #                 "term": {
    #                     "en_GB": "ROR ID"
    #                 }
    #             }
    #         }
    #
    #     if 'identifiers' not in data:
    #         data['identifiers'] = []
    #     # Add the new ror if it does not already exist and the ID is not empty
    #     if new_ror:
    #         data['identifiers'].append(new_ror)
    #         logger.debug(f"update of uuid {row['uuid']}, ror, {new_ror}")
    #         update += 1
    #         if test_choice == 'no':
    #             response = session.put(url, headers=headers, json=data, verify=False)
    #             if response.status_code != 200:
    #                 logger.debug(f"Failed to update data for UUID {row['uuid']}: {response.text}")
    #             else:
    #                 logger.debug(f"Successfully updated data for UUID {row['uuid']}, ror, {new_ror}")
    #             session.close()


    return update



def select_faculties(faculty_choice, test_choice):
    logging.info(f"start fetching person-roots for {faculty_choice}")
    logging.info(f"Test run =  {test_choice}")
    params = {
        'value': 'uu faculty',
    }
    url = RIC_BASE_URL + 'organization/search'
    response = requests.get(url, params=params)
    data = response.json()
    if faculty_choice.lower() == 'all':
        selected_faculties = [item['_key'] for item in data["results"]]
    else:
        selected_faculties = [faculty_choice]

    return selected_faculties

def fetch_personroots(faculty_key):
    """Fetch person-root nodes for a given faculty."""
    try:
        params = {'key': faculty_key, 'max_nr_items': '0'}
        url = RIC_BASE_URL + 'get_all_personroot_nodes'
        response = requests.get(url, params=params)
        # response.raise_for_status()
        return response.json().get("results", [])
    except requests.RequestException as e:
        logging.error(f"Error fetching person-roots for faculty {faculty_key}: {e}")
        return []

def select_researchoutputs(persoonroot_key):
    """Fetch person IDs for a given person-ro    ot."""
    try:
        params = {'key': persoonroot_key, 'category_want': 'journal article'}
        url = RIC_BASE_URL + 'get_all_neighbor_nodes'
        response = requests.get(url, params=params)

        # response.raise_for_status()

        return response.json().get("results", [])

    except requests.RequestException as e:
        logging.error(f"Error fetching person IDs for person-root {persoonroot_key}: {e}")
        return []


def select_persons_researchoutput(selected_faculties):
    persons = []
    new_data = []

    for faculty in selected_faculties:
        logging.info(f"Processing faculty: {faculty}")
        personroots = fetch_personroots(faculty)
        for personroot in personroots:
            if not personroot['_key'] == None:
                personroot_key = personroot['_key']
                outputs = select_researchoutputs(personroot_key)
                for output in outputs:
                    doi = output["_key"].split("|")[0]

                    new_data.append(doi)
                    # print(doi)
                    # if 'Pure-uu' in output["_source"] and 'OpenAlex-uu' in output["_source"]:
                    #     new_data.append(doi)
                    #
                    # else:
                    #     print(output["_key"], 'not in both systems')


    num_elements = len(new_data)
    logging.info(f"total research output with external persons selected:  {num_elements}")
    return new_data


def mainproces(doi, pure, open_alex, article_orgs, uuids, oa_ids):
    logging.debug(f"start fetching organizations for {doi}")
    oa_article = enrich.get_ro_from_openalex(doi, open_alex)
    pure_article = enrich.get_ro_from_pure(doi, pure)
    if oa_article and pure_article:
        article_orgs, uuids, oa_ids = match_orgs_oa_pure(oa_article, pure_article, article_orgs, uuids, oa_ids)

    return article_orgs, uuids, oa_ids

# Function to chunk a list into smaller parts
def chunk_list(data, chunk_size):
    for i in range(0, len(data), chunk_size):
        yield data[i:i + chunk_size]

# Function to get institution data from OpenAlex API using a session
def fetch_openalex_rors(rors, chunk_size=20):
    rors = list(rors)
    ror_chunks = list(chunk_list(rors, chunk_size))
    all_results = []
    logger.info(f"start fetching organizations in open alex")
    with requests.Session() as session:
        for chunk in ror_chunks:
            ror_filter = "|".join(chunk)
            api_url = f"https://api.openalex.org/institutions?filter=ror:{ror_filter}"
            response = session.get(api_url)

            if response.status_code == 200:
                all_results.extend(response.json().get('results', []))
            else:
                print(f"Error: {response.status_code}")
    all_results = {"results": all_results}
    logger.info(f"end fetching organizations in open alex")
    return all_results


def fetch_pure_extorgs(uuids):
    logger.info(f"start fetching external orgs from pure")
    url = PURE_BASE_URL + 'external-organizations/search'

    # Function to split the list into batches of size n
    def split_into_batches(lst, n):
        for i in range(0, len(lst), n):
            yield lst[i:i + n]

    # Define batch size for testing
    batch_size = 10

    # Split the uuids into batches
    # logger.info(f"Total deduplicated items =  {str(len(deduplicated_dois))}")
    uuids =  list(uuids)
    batches = list(split_into_batches(uuids, batch_size))

    # Initialize an empty list to hold all the research outputs
    all_orgs = []
    total_dois = set()  # Initialize a set to hold all DOIs
    # Optional: set a delay between requests to avoid hitting rate limits
    request_delay = 0.1  # seconds
    total_items = 0
    # Loop over each batch and make a request
    for batch_index, batch in enumerate(batches):
        pipe_separated_dois = "|".join(batch)
        logger.info(
            f"Finding ext orgs for batch {batch_index + 1}/{len(batches)}, {batch_size} DOIs per batch.")
        json_data = {
            'size': 100,  # Set size to batch size
            'searchString': pipe_separated_dois,
        }
        try:
            # Make the API request using the pre-configured session
            response = session.post(
                url,
                headers=headers,
                json=json_data,
                timeout=100
            )
            response.raise_for_status()  # Raises an HTTPError for bad responses

            # Parse the response JSON
            data = response.json()
            returned_items = data.get('count')
            logger.debug(f"Total items found for batch {batch_index + 1}: {data.get('count', 0)}")
            total_items += returned_items
            orgs = data.get("items", [])  # Extract the list of items

            # Add the retrieved works to the list
            all_orgs.extend(orgs)
            logger.debug(f"Batch {batch_index + 1}/{len(batches)}: Retrieved {len(orgs)} items.")

        except requests.exceptions.RequestException as e:
            logger.error(f"Error occurred while processing batch {batch_index + 1}: {e}")
   # logger.info(f"Total matching research outputs found: {len(pureworks['results'])}")
        # Optional: Add a delay between requests to avoid hitting rate limits
        time.sleep(request_delay)

    # Combine all works into one JSON object
    orgs = {"results": all_orgs}
    logger.info(f"Total matching external orgs found: {str(total_items)}")

    return orgs


def get_ext_orgdata_pure(external_organization_uuids, pure_org_data):
    # List to store the results
    def get_result_by_uuid(data, target_uuid):
        # Iterate through all results
        for result in data.get('results', []):
            # Check if the uuid matches the target uuid
            if result.get('uuid') == target_uuid:
                return result
        return None
    organization_details = []

    for uuid in external_organization_uuids:
        data = get_result_by_uuid(pure_org_data, uuid)


        if data:


            # Extract the required information
            org_name = data.get('name', {}).get('en_GB', '')  # English name
            org_uuid = data.get('uuid', '')  # UUID

            # Extract identifiers (name and id)
            identifiers = []

            for identifier in data.get('identifiers', []):
                id_name = identifier.get('type', {}).get('term', {}).get(
                    'en_GB') if 'type' in identifier else identifier.get('idSource')
                id_value = identifier.get('id') or identifier.get('value')

                if id_name and id_value:
                    identifiers.append({'name': id_name, 'id': id_value})

            # Append the extracted information to the results list
            organization_details.append({
                'uuid': org_uuid,
                'name': org_name,
                'identifiers': identifiers
            })

        else:
            print(f"Failed to retrieve data for UUID {uuid}.")
    return organization_details


def get_ext_orgdata_openalex(oa_unique_institutions, oa_orgsjsons):
    organization_details = []

    def get_result_by_id(data, target_uuid):
        # Iterate through all results
        for result in data.get('results', []):
            # Check if the uuid matches the target uuid
            if result.get('id') == target_uuid:
                return result
        return None


    for institute in oa_unique_institutions:

        data = get_result_by_id(oa_orgsjsons, institute)


        if data:

            # Extract the required fields
            openalex_id = data['ids'].get('openalex')
            ror = data['ids'].get('ror')
            display_name = data.get('display_name')
            display_name_alternatives = data.get('display_name_alternatives', [])
            geo = data.get('geo', {})

            # Create a list with the extracted information
            extracted_info = {
                "openalex_id": openalex_id,
                "ror": ror,
                "display_name": display_name,
                "display_name_alternatives": display_name_alternatives,
                "geo": geo
            }

            # Append the extracted information to the results list
            organization_details.append(extracted_info)

    return organization_details


def main(faculty_choice, test_choice):
    logger.info("Script to update external organisations in pure from ricgraph has started")

    faculties = select_faculties(faculty_choice, test_choice)
    researchoutputs = select_persons_researchoutput(faculties)
    purejsons = enrich.fetch_pure_researchoutputs(researchoutputs)
    openalexjsons = enrich.fetch_openalex_works(researchoutputs)

    update = 0
    article_orgs = []
    # Initialize sets for unique UUIDs and unique institutions
    uuids = set()
    oa_ids = set()
    for doi in researchoutputs:

        article_orgs, uuids, oa_ids = mainproces(doi, purejsons, openalexjsons, article_orgs, uuids, oa_ids)

    pure_orgsjsons = fetch_pure_extorgs(uuids)

    openalex_orgjsons = fetch_openalex_rors(oa_ids)
    all_orgs_to_update = []
    for article in article_orgs:
         pure_org_details = get_ext_orgdata_pure(article['external_organization_uuids'], pure_orgsjsons)
         oa_org_details = get_ext_orgdata_openalex(article['unique_institutions'], openalex_orgjsons)
         orgs_to_update = match_organizations(pure_org_details, oa_org_details, )
         all_orgs_to_update.extend(orgs_to_update)
         # for org in orgs_to_update:
         #
         #     # Ensure each item in orgs_to_update is a dictionary with 'uuid' and 'ror' keys
         #     if isinstance(org, dict) and 'uuid' in org and 'ror' in org:
         #         rorsuiids.append((org['uuid'], org['ror']))
         # update = update_externalorg_pure(orgs_to_update, test_choice, update)
    logger.info(f"nr of ext orgs that can be  updated: {len(all_orgs_to_update)}")
    # unique_rorsuiids = list(set(rorsuiids))
    # with open('output.csv', mode='w', newline='') as file:
    #     writer = csv.writer(file)
    #     writer.writerows(unique_rorsuiids)


# ########################################################################
# MAIN
# ########################################################################
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Update external persons from Ricgraph')
    parser.add_argument('faculty_choice', type=str, nargs='?',
                        default='uu faculty: faculteit rebo|organization_name',
                        # default='uu faculty: information & technology services|organization_name',
                        help='Faculty choice or "all"')
    parser.add_argument('test_choice', type=str, nargs='?', default='no', help='Run in test mode ("yes" or "no")')

    args = parser.parse_args()

    main(args.faculty_choice, args.test_choice)
