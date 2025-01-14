# ########################################################################
# Script: update_researchoutput_from_ricgraph.py
#
# Description:
# This script retrieves research output DOIs from Ricgraph and compares them
# with the Pure system. If a research output is missing in Pure, metadata
# is retrieved and formatted for import.
#
# The script includes:
# - Fetching a list of faculties and selecting research outputs by faculty.
# - Fetching research outputs associated with person nodes.
# - Formatting and sending metadata to Pure.
# - Supports interactive choice for test runs before updating Pure.
#
# Steps:
# 1. Retrieve a list of faculties.
# 2. Fetch persons and their research outputs by faculty.
# 3. Compare outputs between Ricgraph and Pure.
# 4. Create a JSON for output that can be imported into Pure.
#
# Author: David Grote Beverborg
# Created: April 2024
#
# License:
# MIT License
#
# Copyright (c) 2024 David Grote Beverborg
# ########################################################################


import logging
import argparse
import openalex_utils
import requests
import os
import pure_researchoutputs as pure
from logging_config import setup_logging
from config import PURE_BASE_URL, PURE_API_KEY, PURE_HEADERS, RIC_BASE_URL, OPENALEX_HEADERS, OPENALEX_BASE_URL
import enrich_pure_external_persons as oa
from datetime import datetime
datetimetoday = datetime.now().strftime('%Y%m%d')
# steps:
# - get list of faculties
# - user choose faculty (or all)
# - get persons from faculty
# - get researchoutput from persons
# - check pure for presence of these researchoutput
# - make json for output(later will be imported in pure)
# Set logging level to INFO for this script

logger = setup_logging('btp', level=logging.INFO)

def print_faculty_list(faculty_list):
    for idx, faculty in enumerate(faculty_list, start=1):
        print(f"{idx}. {faculty['value']}")
    print("all. All Faculties")

def fetch_personroots(faculty_key):
    """Fetch person-root nodes for a given faculty."""
    try:
        params = {'key': faculty_key, 'max_nr_items': '0'}
        url = RIC_BASE_URL + 'get_all_personroot_nodes'
        response = requests.get(url, params=params)
        response.raise_for_status()
        return response.json().get("results", [])
    except requests.RequestException as e:
        logging.error(f"Error fetching person-roots for faculty {faculty_key}: {e}")
        return []

def select_faculties(faculty_choice):

    logger.info("Script to update researchoutput in pure from ricgraph has started")
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



def select_persons_researchoutput(selected_faculties):
    persons = []

    new_data = []
    duplicates = []
    all_data = []
    for faculty in selected_faculties:

        logger.info(f"Processing faculty: {faculty}")

        import requests

        params = {
            'key': faculty,
            'category_want': 'journal article',
            'source_system': 'uu pure',
            'max_nr_items': '0',
        }

        response = requests.get('http://127.0.0.1:3030/api/organization/enrich', params=params)

        outputs =  response.json().get("results", [])
        for output in outputs:
            doi = output["value"]
            all_data.append(doi)
            new_data.append(doi)
            # if 'Pure-uu' not in output["_source"]:
            #     new_data.append(doi)
            # else:
            #     duplicates.append(doi)


        personroots = fetch_personroots(faculty)

        for personroot in personroots:
            if not personroot['_key'] == None:
                personroot_key = personroot['_key']
                outputs = select_researchoutputs(personroot_key)
                for output in outputs:
                    doi = output["_key"].split("|")[0]
                    all_data.append(doi)
                    if 'Pure-uu' not in output["_source"]:
                        new_data.append(doi)
                    else:
                        duplicates.append(doi)

    all_data = list(set(all_data))

    logger.info(f"research output selected in ricgraph, not in pure:  {len(new_data)}")
    return new_data, duplicates, all_data

def select_researchoutputs(persoonroot_key):
    """Fetch person IDs for a given person-ro    ot."""
    try:
        params = {'key': persoonroot_key, 'category_want': 'journal article'}
        url = RIC_BASE_URL + 'get_all_neighbor_nodes'
        response = requests.get(url, params=params)

        return response.json().get("results", [])

    except requests.RequestException as e:
        logging.error(f"Error fetching person IDs for person-root {persoonroot_key}: {e}")
        return []


def test_or_not(researchoutputs, duplicates, all_data):
    number_of_researchoutput = len(researchoutputs)
    print(f"{len(all_data)} are in ricgraph")
    print(f"{number_of_researchoutput} are not in pure but are in ricgraph")
    print(f"{len(duplicates)} are in pure AND are in ricgraph")
    print("Would you like to do a test run? (publications will not be inserted in pure, but the script will check if all needed info is there")
    choice = input("enter yes or no ")
    return choice


def back_to_pure(all_openalex_data):
    if all_openalex_data:
        df, errors = openalex_utils.transform_openalex_to_df(all_openalex_data)

        if 'journal_issn' in df.columns:
            df = df.dropna(subset=['journal_issn'])
        num_rows = df.shape[0]

        pure.df_to_pure(df)



def main(faculty_choice):

    faculties = select_faculties(faculty_choice)
    researchoutputs, duplicates, all_data = select_persons_researchoutput(faculties)

    if researchoutputs:
        all_openalex_data = oa.fetch_openalex_works(researchoutputs)
        back_to_pure(all_openalex_data)
    logger.info("Script part 1 to import research output in pure from ricgraph has ended")
    logger.info("Please look at the update file and uncheck items you do not want to be imported, then proceed to import them in pure via *Apply Update to Pure*")

# ########################################################################
# MAIN
# ########################################################################

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Import Datasets from Ricgraph')
    parser.add_argument('faculty_choice', type=str, nargs='?',
                        default='uu faculty: information & technology services|organization_name',
                        # default='uu faculty: faculteit diergeneeskunde|organization_name',
                        help='Faculty choice or "all"')
    parser.add_argument('test_choice', type=str, nargs='?', default='yes', help='Run in test mode ("yes" or "no")')

    args = parser.parse_args()
    main(args.faculty_choice)
