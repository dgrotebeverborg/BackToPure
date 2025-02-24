# ########################################################################
#
# Pure Persons - function modules for the CRUD api persons of pure
#
# ########################################################################
#
# MIT License
#
# Copyright (c) 2024 David Grote Beverborg
# ########################################################################

import pandas as pd
import json
import requests
from datetime import datetime, time
import configparser
import os
import logging
from config import PURE_BASE_URL, PURE_API_KEY, PURE_HEADERS, OPENALEXEX_ID_URI, ORCID_ID_URI, OPENALEX_BASE_URL
from logging_config import setup_logging
from dateutil import parser
from pathlib import Path

logger = setup_logging('btp', level=logging.INFO)

headers = {
    "Content-Type": "application/json",
    "accept": "application/json",
    "api-key": PURE_API_KEY
}

def parse_date(date_string):

    try:
        return parser.parse(date_string)
    except (ValueError, TypeError):

        today_date = datetime.now().date()
        today_date = datetime.combine(today_date, time())
        return today_date

def extract_orcid(orcid_full_url):
    # Split the string by '/' and return the last part

    return orcid_full_url.split('/')[-1]


# Function to construct person_detail from API response data
def construct_person_detail(data, ref_date=None):
    """
       Constructs a dictionary of person details from API response data.
       (Note that the header of the api-call contain the apikey that is loaded in the top of this script)

       Parameters:
       - data (dict): The JSON data returned from the API.
       - ref_date (datetime, optional): A reference date used to filter associations.
         Only associations active on this date are included. If None, all associations are included.

       Returns:
       - dict: A dictionary containing the person's UUID, first name, last name,
               and a list of associations with their UUIDs and active dates.
       """

    associations = data.get('staffOrganizationAssociations', [])

    associationsUUIDs = []
    for assoc in associations:

        assoc_start_date = assoc.get('period', {}).get('startDate')
        assoc_end_date = assoc.get('period', {}).get('endDate', '9999-12-31')

        # Convert string dates to datetime objects for comparison
        assoc_start_datetime = datetime.strptime(assoc_start_date, "%Y-%m-%d") if assoc_start_date else None
        assoc_end_datetime = datetime.strptime(assoc_end_date, "%Y-%m-%d") if assoc_end_date else None

        # Check if the association falls within the ref_date, if provided
        if ref_date:

            if assoc_start_datetime <= ref_date <= assoc_end_datetime:
                associationsUUIDs.append({
                    "uuid": assoc.get('organization', {}).get('uuid'),
                    "startDate": assoc_start_date,
                    "endDate": assoc_end_date
                })
        else:
            associationsUUIDs.append({
                "uuid": assoc.get('organization', {}).get('uuid'),
                "startDate": assoc_start_date,
                "endDate": assoc_end_date
            })

    return {
        "uuid": data.get('uuid'),
        "firstName": data.get('name', {}).get('firstName'),
        "lastName": data.get('name', {}).get('lastName'),
        "associationsUUIDs": associationsUUIDs,
    }

def find_person(contributor, person_ids, date, type):
    """
    Searches for and retrieves detailed information about a person from an API.

    Parameters:
    - name (str): The name of the person to be searched. if none => the module will not try to find person on name
    - person_ids (dict): A dictionary of identifiers for the person (e.g., UUID, other IDs).
    - date (str): A date string used for filtering data. if None => all association ids will be collected
    - apikey (str): API key for authentication with the API.
      (Note that the header of the api-call contain the apikey that is loaded in the top of this script)


    Returns:
    - dict: A dictionary containing detailed information about the person if a unique match is found.
    - str: A message indicating no unique person was found if no match or multiple matches are found.
    """
    ref_date = None

    name = contributor['name']
    if date:
        # ref_date = datetime.strptime(date, "%Y-%m-%d")

        ref_date = parse_date(date)


    person_detail = None
    if person_ids and 'uuid' in person_ids:

        uuid = person_ids['uuid']
        api_url = PURE_BASE_URL + 'persons/' + uuid

        response = requests.get(api_url, headers=headers)
        if response.status_code == 200:
            data = response.json()
            person_detail = construct_person_detail(data, ref_date)
            if person_detail:
                logger.debug(f"Person found with UUID: {person_ids['uuid']}")
                return person_detail

    if person_ids:
        for id_type, id_value in person_ids.items():
                    if id_type.lower() == 'orcid':
                        id_value = extract_orcid(id_value)
                    data = {"searchString": id_value}
                    json_data = json.dumps(data)
                    api_url = PURE_BASE_URL + 'persons/search/'
                    try:
                        response = requests.post(api_url, headers=headers, data=json_data)
                        if response.status_code == 200:
                            data = response.json()
                            items = data.get('items', [])

                            if items:
                                if len(items) == 1:
                                    item = items[0]
                                    person_detail = construct_person_detail(item, ref_date)
                                    person_detail['type'] = type
                                    logger.debug(f"Person found with {id_type}: {id_value}")
                                    return person_detail
                                else:
                                    logger.debug(f"Multiple or no persons found for {id_type}, {id_value}")
                        else:
                            logger.debug(f"Error searching for {id_type}: {response.status_code} - {response.text}")
                    except requests.RequestException as e:
                        logger.debug(f"An error occurred while searching for {id_type}: {e}")

    if not person_detail and name is not None:
        data = {"searchString": name}
        json_data = json.dumps(data)
        api_url = PURE_BASE_URL + 'persons/search/'
        try:
            response = requests.post(api_url, headers=headers, data=json_data)
            if response.status_code == 200:
                data = response.json()
                items = data.get('items', [])

                if items:
                    if len(items) == 1:
                        item = items[0]
                        person_detail = construct_person_detail(item, ref_date)
                        logger.debug(f"Person {person_detail['firstName']} {person_detail['lastName']} found for name: {name}")
                        person_detail['type'] = type
                        return person_detail
                    else:
                        logger.debug(f"Multiple persons found for name: {name}")

                        for item in items:
                            if 'names' in item:
                                for name_entry in item["names"]:
                                    if name_entry["type"]["uri"] == "/dk/atira/pure/person/names/knownas":
                                        first_name = name_entry["name"].get("firstName", "N/A")
                                        last_name = name_entry["name"].get("lastName", "N/A")

                                        if contributor['first_name']== first_name and contributor['last_name']== last_name:
                                            person_detail = construct_person_detail(item, ref_date)
                                            logger.debug(
                                                f"Person {person_detail['firstName']} {person_detail['lastName']} found for name: {name}")

                else:
                    logger.debug(f"no persons found for name: {name}")

            else:
                logger.debug(f"Error searching for {name}: {response.status_code} - {response.text}")

        except requests.RequestException as e:
            logger.debug(f"An error occurred while searching for {name}: {e}")


    return person_detail




def get_active_associations(person_details, ref_date_str):
    """
    Updates person details to include only active associations based on a reference date.

    Parameters:
    - person_details (dict): A dictionary containing person's details including associations.
    - ref_date_str (str): The reference date in string format (e.g., 'YYYY-MM-DD').

    Returns:
    - dict: Updated person_details with only active associations for the given reference date.
    """
    if not person_details or not ref_date_str:
        logger.warning(f"no person_details or date for get_active_associations")
        return

    active_associations = []
    ref_date = None
    if ref_date_str:

        ref_date = parse_date(ref_date_str)

    if 'associationsUUIDs' in person_details:
        for association in person_details['associationsUUIDs']:

            start_date_str = association.get('startDate')
            end_date_str = association.get('endDate', '9999-12-31')

            start_date = datetime.strptime(start_date_str, "%Y-%m-%d") if start_date_str else None
            end_date = datetime.strptime(end_date_str, "%Y-%m-%d") if end_date_str else None

            if start_date and end_date and start_date <= ref_date <= end_date:
                active_associations.append(association)

        # Update the person_details with only active associations
    person_details['associationsUUIDs'] = active_associations
    return person_details

def find_external_person(person_ids):
    orcid = None
    openalex = None
    first_uuid = None
    for id_type, id_value in person_ids.items():

        if id_type.lower() == 'orcid':
            id_value = extract_orcid(id_value)
            orcid = id_value

        if id_type.lower() == 'openalex':
            id_value = id_value.replace("https://openalex.org/", "")
            openalex = id_value

        data = {"searchString": id_value}
        json_data = json.dumps(data)
        api_url = PURE_BASE_URL + 'external-persons/search/'
        try:
            response = requests.post(api_url, headers=PURE_HEADERS, data=json_data)
            if response.status_code == 200:
                data = response.json()
                items = data.get('items', [])

                if items:
                    if len(items) == 1:

                        # Extract the UUID of the first item in the "items" list
                        first_uuid = data['items'][0]['uuid']
                        logger.debug(f"Person found with {id_type}: {id_value}")


                    else:
                        logger.debug(f"Multiple or no persons found for {id_type}, {id_value}")
            else:
                logger.error(f"Error searching for {id_type}: {response.status_code} - {response.text}")
        except requests.RequestException as e:
            logger.error(f"An error occurred while searching for {id_type}: {e}")

    return first_uuid, orcid, openalex

def create_external_person(first_name, last_name, orcid, openalex):
    """
    Creates an external person in the Pure system.

    :param first_name, last_name:  first and last names.
    :return: UUID of the newly created external person.
    """
    api_url = PURE_BASE_URL + 'external-persons/'


    data = {"name": {"firstName": first_name, "lastName": last_name}}
    if openalex:
        openalexid = {
            "typeDiscriminator": "ClassifiedId",
            "id": openalex,
            "type": {
                "uri": OPENALEXEX_ID_URI,
                "term": {
                    "en_GB": "Open Alex id"
                }
            }
        }

    if orcid:
        orcidid = {
            "typeDiscriminator": "ClassifiedId",
            "id": orcid,
            "type": {
                "uri": ORCID_ID_URI,
                "term": {
                    "en_GB": "ORCID"
                }
            }
        }
    if orcid or openalex:
        data['identifiers'] = []
        if orcid:
            data['identifiers'].append(orcidid)
        if openalex:
            data['identifiers'].append(openalexid)

    json_data = json.dumps(data)

    try:
        response = requests.put(api_url, headers=PURE_HEADERS, data=json_data)

        if response.status_code in [200, 201]:
            external_person = response.json()
            return external_person.get('uuid')
        else:
            logger.error(f"Error creating external person: {response.status_code} - {response.text}")
    except requests.RequestException as e:
        logger.error(f"An error occurred while creating external person: {e}")

    return None

def find_extenal_orgs(affiliations):
    # 'affiliations': {'OpenAlex': 'https://openalex.org/I193662353', 'ROR': 'https://ror.org/04pp8hn57'}}

    ror = affiliations.get('ROR')

    first_uuid = None
    if ror:
        data = {"searchString": ror}
        json_data = json.dumps(data)
        api_url = PURE_BASE_URL + 'external-organizations/search/'
        try:
            response = requests.post(api_url, headers=PURE_HEADERS, data=json_data)
            if response.status_code == 200:
                data = response.json()
                items = data.get('items', [])

                if items:
                    if len(items) == 1:

                        # Extract the UUID of the first item in the "items" list
                        first_uuid = data['items'][0]['uuid']
                        logger.debug(f"Person found with {ror}")

                    else:
                        logger.debug(f"Multiple or no orgs found for {ror}")
            else:
                logger.debug(f"Error searching for {ror}")
        except requests.RequestException as e:
            logger.debug(f"An error occurred while searching for {ror}: {e}")

    return first_uuid