# ########################################################################
# Script: yoda_utils.py
#
# Description:
# This script provides **main functions** for reading dataset exports from Yoda,
# parsing JSON data, and converting it into pandas DataFrames. It is intended to
# be imported as a module and should not be run standalone.
#
# Functions include:
# - Loading JSON files safely and parsing metadata.
# - Extracting contributor and creator information.
# - Formatting dataset metadata and dates.
#
# Important:
# This script is a utility module for Yoda data processing and should be used
# as part of other scripts.
#
# Dependencies:
# - json, pandas, logging, configparser, datetime, etc.
#
# Author: David Grote Beverborg
# Created: 2024
#
# License:
# MIT License
#
# Copyright (c) 2024 David Grote Beverborg
# ########################################################################


import json
import pandas as pd
import os
import logging
from datetime import datetime
from pathlib import Path
import configparser
# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
yoda_utils_dir = os.path.dirname(os.path.abspath(__file__))
file_path = os.path.join(yoda_utils_dir, 'other_files', 'test2.json')


def load_config():
    """Loads the configuration from the config.ini file."""
    config_path = Path(__file__).resolve().parent.parent / 'config.ini'
    if not config_path.exists():
        raise FileNotFoundError(f"The configuration file {config_path} does not exist.")
    config = configparser.ConfigParser()
    config.read(config_path)
    return config

def safe_load_json(filename):
    """ Safely load JSON data from a file. """
    try:
        with open(filename, 'r') as file:
            return json.load(file)
    except FileNotFoundError:
        logging.error(f"The file {filename} does not exist.")
        return None
    except json.JSONDecodeError:
        logging.error(f"The file {filename} contains invalid JSON.")
        return None

def parse_date(date_str):
    """
    Parse date string and return a dictionary of year, month, day if valid, else None.
    Handles both date-only and datetime strings.
    """
    date_formats = ["%Y-%m-%d", "%Y-%m-%dT%H:%M:%S"]

    for format in date_formats:
        try:
            parsed_date = datetime.strptime(date_str, format)
            return {
                'publication_year': parsed_date.year,
                'publication_month': parsed_date.month,
                'publication_day': parsed_date.day
            }
        except ValueError:
            continue
    logging.error(f"{date_str} is not a valid date format.")


def parse_person_data(contributor):
    """ Extract and format person-related data from contributor info. """
    name = contributor.get('Name', {})
    full_name = f"{name.get('Given_Name', 'Unknown')} {name.get('Family_Name', 'Unknown')}"

    affiliations = contributor.get('Affiliation', [])
    first_affiliation_name = affiliations[0] if affiliations else 'None'

    person_ids = [
        {
            'id': identifier.get('Name_Identifier_Scheme', 'N/A'),
            'value': identifier.get('Name_Identifier', 'N/A')
        }
        for identifier in contributor.get('Person_Identifier', [])
        if identifier
    ]

    print(f"Parsing contributor: {full_name}")
    print(f"Affiliation: {first_affiliation_name}")
    print(f"Person IDs: {person_ids}")

    return {
        'name': full_name,
        'person_ids': person_ids,
        'affiliation': first_affiliation_name,
        'type': 'contributor'
    }

def get_df_from_yoda(filename):
    """
    Load a JSON file and transform its content into a pandas DataFrame.
    """
    data = safe_load_json(filename)
    if data is None:
        return pd.DataFrame()  # Return an empty DataFrame if JSON data couldn't be loaded

    all_datasets_aggregated = []

    for dataset_path, dataset_info in data.items():
        metadata = dataset_info.get('metadata', {})
        doi = dataset_info.get('doi', None)
        title = metadata.get('Title', 'N/A')
        description = metadata.get('Description', 'N/A')
        access = metadata.get('Data_Access_Restriction', 'N/A')

        start_date = metadata.get('Collected', {}).get('Start_Date', '')
        if start_date:
            date_info = parse_date(start_date) or {}
        else:
            mod_date = dataset_info.get('modified', None)
            date_info = parse_date(mod_date) or {}

        # Parsing contributors and creators
        persons = [parse_person_data(contributor) for contributor in dataset_info.get('contributors', [])]
        persons += [parse_person_data(creator) for creator in
                    metadata.get('Creator', [])]  # Adjusted to use 'metadata'

        all_datasets_aggregated.append({
            'doi': doi,
            'title': title,
            'description': description,
            'publisher': 'default',
            'date': date_info,
            'publication_year': date_info['publication_year'],
            'publication_month': date_info['publication_month'],
            'publication_day': date_info['publication_day'],
            'persons': persons
        })

    return pd.DataFrame(all_datasets_aggregated)

# config = load_config()
if __name__ == "__main__":

    df = get_df_from_yoda(file_path)
    logging.info(f"DataFrame loaded with {len(df)} entries")

