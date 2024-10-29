# ########################################################################
#
# Datacite utilities - function modules for getting datasets from datacite
#
# ########################################################################
#
# MIT License
#
# Copyright (c) 2024 David Grote Beverborg
# ########################################################################

import requests
import json
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import logging
from logging_config import setup_logging
logger = setup_logging('dataset', level=logging.INFO)
def get_first_affiliation_name(affiliations):
    if isinstance(affiliations, list) and affiliations:
        # Assume each item in the list is a dictionary with a 'name' key
        first_affiliation = affiliations[0]
        if isinstance(first_affiliation, dict):
            return first_affiliation.get('name', 'None')
        elif isinstance(first_affiliation, str):
            return first_affiliation  # Assuming the string itself is the name
    # Default case if 'affiliations' is not list-like or is empty
    return 'None'

def fetch_data_for_doi(doi):
    """Fetch and parse data for a single DOI."""
    response = requests.get(f'https://api.datacite.org/dois/{doi}')
    if response.status_code == 200:
        data = response.json()['data']['attributes']

        return parse_datacite_response(data, doi)
    else:
        logger.info(f"Failed to fetch data for DOI: {doi}")
        return None

def parse_datacite_response(data, doi):
    """Parse the response from DataCite API and return structured data."""

    title = data['titles'][0]['title']
    persons = []

    for creator in data['creators']:

        if not 'givenName' in creator and not 'familyName' in creator:

            if 'name' in creator:
                # Split the name into parts
                name_parts = creator['name'].split(",")
                if len(name_parts) != 2:
                    name_parts = creator['name'].split(" ")
                creator['givenName'] = name_parts[0]
                creator['familyName'] = " ".join(name_parts[1:]) if len(name_parts) > 1 else ""

        if 'givenName' in creator and  'familyName' in creator:
            affiliations = creator.get('affiliation', [])
            first_affiliation_name = get_first_affiliation_name(affiliations)
            person_ids = {
                ni.get('nameIdentifierScheme'): ni.get('nameIdentifier')
                for ni in creator.get('nameIdentifiers', [])
                if ni.get('nameIdentifier')  # Ensure only valid entries are included
            }
            creator_info = {
                # 'name': creator['name'],
                'first_name': creator['givenName'],
                'last_name': creator['familyName'],
                'type': 'creator',
                'affiliations': first_affiliation_name,
                'person_ids': person_ids

            }
            persons.append(creator_info)

    subjects = [subject['subject'] for subject in data.get('subjects', [])]
    descriptions = data.get('descriptions', [])
    description = descriptions[0]['description'] if descriptions else 'No description available'

    return {
        'title': title,
        'description': description,
        'persons': persons,
        'publisher': data['publisher'],
        'doi': doi,
        'publication_year': data['publicationYear'],
        'created': str(datetime.strptime(data['created'], '%Y-%m-%dT%H:%M:%S.%fZ')),
        'subjects': subjects
    }

def get_df_from_datacite(datasets):
    """Fetch data for multiple DOIs and return a DataFrame."""
    with ThreadPoolExecutor(max_workers=10) as executor:
        results = list(executor.map(fetch_data_for_doi, datasets))

    # Filter out None results in case of failed fetches
    valid_results = [result for result in results if result]

    # """Fetch data for multiple DOIs and return a DataFrame."""
    # results = [fetch_data_for_doi(doi) for doi in datasets]
    #
    # # Filter out None results in case of failed fetches
    # valid_results = [result for result in results if result is not None]


    df = pd.DataFrame(valid_results)
    logger.info("datasets found in open alex: " + str(df.shape[0]))
    file_path = "datasets.xlsx"
    logger.info("downloaded datasets in: " + file_path)
    # Save the dataframe to an Excel file
    df.to_excel(file_path, index=False)
    return df

def main():
    return
    # # Usage example, assuming 'datasets' is a list of DOI strings
    # datasets = ['10.6084/M9.FIGSHARE.21829182', '10.5061/dryad.tn70pf1', 'DOI3']
    # df = get_df_from_datacite2(datasets)
    # print(df)

if __name__ == '__main__':
    main()
