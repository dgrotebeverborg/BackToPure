import configparser
import logging
import requests
import datacite_utils
import pure_datasets as puda
import sys
import argparse
from config import PURE_BASE_URL, PURE_API_KEY, PURE_HEADERS, RIC_BASE_URL, OPENALEX_HEADERS, OPENALEX_BASE_URL
from logging_config import setup_logging

logger = setup_logging('dataset', level=logging.INFO)
import json
# steps:
# - get list of faculties
# - user choose faculty (or all)
# - get persons from faculty
# - get datasets from persons
# - check pure for presence of these datasets
# - create datasets in pure


def print_faculty_list(faculty_list):
    for idx, faculty in enumerate(faculty_list, start=1):
        print(f"{idx}. {faculty['value']}")
    print("all. All Faculties")

def fetch_personroots(faculty_key):
    """Fetch person-root nodes for a given faculty."""
    try:
        params = {'key': faculty_key, 'max_nr_items': '9999'}
        response = requests.get('http://127.0.0.1:3030/api/get_all_personroot_nodes', params=params)

        return response.json().get("results", [])
    except requests.RequestException as e:
        logging.error(f"Error fetching person-roots for faculty {faculty_key}: {e}")
        return []

def select_faculties(faculty_choice):
    # Set logging level to INFO for this script
    logger = setup_logging('dataset', level=logging.INFO)
    logger.info("Script to update datasets in pure from ricgraph has started")
    params = {
        'value': 'uu faculty',
    }
    response = requests.get('http://127.0.0.1:3030/api/organization/search', params=params)
    data = response.json()

    if faculty_choice.lower() == 'all':
        selected_faculties = [item['_key'] for item in data["results"]]
    else:
        selected_faculties = [faculty_choice]

    return selected_faculties

def select_persons_datasets(faculties, faculty_choice):
    persons = []
    if faculty_choice == 'all':

        params = {
            'category': 'data set',
            'max_nr_items': '0',
        }
        data = []
        response = requests.get('http://127.0.0.1:3030/api/advanced_search', params=params)
        datasets = response.json().get("results", [])
        for set in datasets:
            doi = set["_key"].split("|")[0]
            data.append(doi)
    else:
        for faculty in faculties:
            logger.info(f"Processing faculty: {faculty}")

            personroots = fetch_personroots(faculty)
            data = []
            for persoonroot in personroots:
                if not persoonroot['_key'] == None:
                    persoonroot_key = persoonroot['_key']
                    datasets = select_datasets(persoonroot_key)
                    for set in datasets:
                        doi = set["_key"].split("|")[0]
                        data.append(doi)
    logger.info("datasets found in ricgraph: " + str(len(data)))
    return data


def select_datasets(persoonroot_key):

    """Fetch IDs for a given person-ro    ot."""
    try:
        params = {'key': persoonroot_key, 'category_want': 'data set'}
        url = RIC_BASE_URL + 'get_all_neighbor_nodes'
        response = requests.get(url, params=params)

        return response.json().get("results", [])
    except requests.RequestException as e:
        logging.error(f"Error fetching person IDs for person-root {persoonroot_key}: {e}")
        return []
def test_or_not(datasets):
    number_of_datasets = len(datasets)
    print(f"{number_of_datasets} are not in pure but are in ricgraph")
    print("Would you like to do a test run? (datsets will not be inserted in pure, but the script will check if all needed info is there")
    choice = input("enter yes or no ")
    return choice


def df_to_pure(df, created, ignored, test_choice):
    for _, row in df.iterrows():
        already_in_pure = puda.find_dataset(None, row['doi'])
        if already_in_pure:
            logger.warning(f"dataset with doi: {row['doi']}, already in pure")
            ignored += 1
        else:
            contributors_details = puda.get_contributors_details(row['persons'], row['created'], test_choice)

            if contributors_details is not None:
                row['parsed_contributors'] = puda.format_contributors(contributors_details)
                row['parsed_organizations'], row['managing_org'] = puda.format_organizations_from_contributors(
                    contributors_details)

                if test_choice == 'no':
                    dataset_json = puda.construct_dataset_json(row)
                    uuid_ds = puda.create_dataset(dataset_json)
                    if not uuid_ds == 'error':
                       created += 1
                    else:
                        ignored += 1
                        logger.debug('error creating dataset: ' + row['title'])
                else:
                    created += 1
            else:
                ignored += 1

    return created, ignored


def main(faculty_choice, test_choice):

    faculties = select_faculties(faculty_choice)
    datasets = select_persons_datasets(faculties, faculty_choice)

    df = datacite_utils.get_df_from_datacite(datasets)


    created = 0
    ignored = 0

    created, ignored = df_to_pure(df, created, ignored, test_choice)

    logger.info(f"Process completed. created datasets: {created}, skipped: {ignored}")

# ########################################################################
# MAIN
# ########################################################################

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Import Datasets from Ricgraph')
    parser.add_argument('faculty_choice', type=str, nargs='?',
                        default='all',
                        help='Faculty choice or "all"')
    parser.add_argument('test_choice', type=str, nargs='?', default='no', help='Run in test mode ("yes" or "no")')

    args = parser.parse_args()

    main(args.faculty_choice, args.test_choice)
