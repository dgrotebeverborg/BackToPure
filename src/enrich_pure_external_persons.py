"""
Script Name: enrich_pure_external_persons.py

Summary:
---------
This script is designed to update external person records in the Pure research information system by integrating data from ricgraph/OpenAlex. The main goal is to enrich the existing data with additional identifiers such as ORCID and OpenAlex IDs, and ensure the data in Pure is as complete and accurate as possible.

The script performs the following steps:

1. **Initial Setup**:
   - Imports necessary libraries and sets up configurations such as logging, session handling for HTTP requests, and API keys.

2. **Selecting Faculties and Research Outputs**:
   - Selects a set of faculties based on the user's choice and identifies the research outputs associated with those faculties from ricgraph. This defines the scope of data to be processed.

3. **Fetching Data from Pure and OpenAlex**:
   - Retrieves research output data from both Pure and OpenAlex systems, ensuring a comprehensive dataset from both sources, containing information about publications and their authors.

4. **Matching Authors**:
   - Processes the retrieved research outputs to identify authors and matches individuals across the two datasets (Pure and OpenAlex) based on name and identifier information.
   - Handles inconsistencies in naming conventions and ensures that corresponding records from Pure and OpenAlex are linked.

5. **Retrieving and Updating Person Records**:
   - Retrieves detailed records of external persons from Pure after matching.
   - Updates these records with additional identifiers, such as ORCID and OpenAlex IDs, when they are not already present.
   - Uses the Pure API to ensure that all records are enriched with as much available data as possible.

6. **Logging and Error Handling**:
   - Uses logging to track progress, successes, and errors during the matching and updating process. Handles errors gracefully to continue processing as much data as possible.

7. **Test Mode**:
   - Includes a test mode option that allows users to run the script without making actual updates to Pure, useful for validating changes before applying them.

The purpose of the script is to integrate and update information about external researchers in Pure by leveraging data from OpenAlex, helping keep Pure's data accurate and comprehensive, especially concerning author identifiers.
"""

import re
import time
import pandas as pd
import logging
from logging_config import setup_logging
import requests
import json
import argparse
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import urllib3
from config import PURE_BASE_URL, PURE_API_KEY, EMAIL, RIC_BASE_URL, OPENALEXEX_ID_URI, ORCID_ID_URI, OPENALEX_HEADERS

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

def extract_orcid_id(orcid):
    # Check if the ORCID is in URL format
    if orcid and orcid.startswith('https://orcid.org/'):
        # Extract just the ID part
        return orcid.split('/')[-1]
    elif orcid:
        # Return the ORCID as is, assuming it's already in the correct format
        return orcid
    else:
        # Return an empty string if orcid is None
        return ''

def extract_openalex_id(openalex):
    # Check if the openalex is in URL format
    if openalex and openalex.startswith('https://openalex.org/'):
        # Extract just the ID part
        return openalex.split('/')[-1]
    elif openalex:
        # Return the openalex as is, assuming it's already in the correct format
        return openalex
    else:
        # Return an empty string if orcid is None
        return ''

def get_ro_from_openalex(item, openalexworks):
    doi = 'https://doi.org/' + item
    url = 'https://api.openalex.org/works/' + doi
    # try:
    #     response = session.get(url, headers=OPENALEX_HEADERS)
    #     if response.status_code == 200:
    #         data = response.json()  # Directly parse JSON response
    #         return data
    # except:
    #     pass

    for work in openalexworks.get("results", []):

        if work.get("doi") == doi:

            return work
    return None


def get_ro_from_pure(target_doi, pureworks):
    """
    Retrieves the first research output from the Pure API results that matches the provided DOI.

    Parameters:
    pureworks (dict): The combined JSON object containing all research outputs.
    target_doi (str): The DOI of the research output to retrieve.

    Returns:
    dict: The first research output that corresponds to the provided DOI. Returns None if none are found.
    """

    # Normalize the target DOI to ensure matching works correctly
    normalized_doi = target_doi.replace("https://doi.org/", "").lower()

    for work in pureworks.get("results", []):
        # Check in 'electronicVersions' for the DOI
        if 'electronicVersions' in work:
            for version in work['electronicVersions']:
                if 'doi' in version:
                    # Normalize the DOI in the data
                    normalized_version_doi = version['doi'].replace("https://doi.org/", "").lower()
                    if normalized_doi == normalized_version_doi:
                        return work  # Return the first matching work

        # Check in 'additionalLinks' for the DOI if not already found
        if 'additionalLinks' in work:
            for link in work['additionalLinks']:
                if 'url' in link:
                    # Normalize the DOI in the link
                    normalized_link_doi = link['url'].replace("https://doi.org/", "").lower()
                    if normalized_doi == normalized_link_doi:
                        return work  # Return the first matching work

    return None  # Return None if no match is found


def check_name_match(alex_name, pure_authors):
    # Check for exact full name match
    if alex_name in pure_authors:
        return pure_authors[alex_name]

    # Split the alex_name into first and last names
    alex_parts = alex_name.split(' ')
    if len(alex_parts) < 2:
        return None  # Not enough parts to compare

    alex_first_name, alex_last_name = alex_parts[0], alex_parts[-1]

    for pure_name in pure_authors:
        pure_parts = pure_name.split(' ')
        if len(pure_parts) < 2:
            continue  # Skip if the name format is not as expected

        pure_first_name, pure_last_name = pure_parts[0], pure_parts[-1]

        # Check if last names match and if first letter of first names match
        if alex_last_name == pure_last_name and alex_first_name and pure_first_name and alex_first_name[0] == \
                pure_first_name[0]:
            return pure_authors[pure_name]
    return None
def match_persons_oa_pure(oa_article, pure_article):
    # Extract authors from the alex1.json dataset
    # Extract authors from the alex1.json dataset with ORCID if available
    alex_authors = {}

    for author in oa_article.get('authorships', []):
        author_info = author.get('author')
        if author_info and isinstance(author_info, dict):
            display_name = author_info.get('display_name')
            if display_name:
                alex_authors[display_name] = {
                    'alex_id': author_info.get('id'),
                    'orcid': author_info.get('orcid', None)
                }

    # Extract authors from the pure1.json dataset
    # Correcting the extraction of UUIDs for contributors and ensuring Pure_UUID is not a list

    pure_authors = {}  # Initialize the dictionary if not already initialized

    for contributor in pure_article.get('contributors', []):
        if 'externalPerson' in contributor:  # Check for externalPerson first
            # Extract UUID
            uuid = contributor['externalPerson'].get('uuid', "")

            # Extract first and last names
            name_info = contributor.get('name', {})
            first_name = name_info.get('firstName', "")
            last_name = name_info.get('lastName', "")

            if first_name or last_name:
                name = f"{first_name} {last_name}".strip()
                # Store in pure_authors dictionary
                pure_authors[name] = uuid
            else:
                logger.info(f"Missing first or last name for contributor: {contributor}")
        # else:
        #     logger.info(f"Contributor is not an external person: {contributor}")

    # Find common authors based on names and create the list with names, all IDs, and ORCID if available
    common_authors_list = []
    for name, ids in alex_authors.items():

        pure_uuid = check_name_match(name, pure_authors)
        if pure_uuid and name:

            common_authors_list.append({
                'Name': name,
                'Alex_ID': extract_openalex_id(ids['alex_id']),
                'Pure_UUID': pure_uuid,
                'ORCID': extract_orcid_id(ids['orcid'])
            })




    # Display the common authors

    # output_path = "common_authors.xlsx"
    # common_authors_df.to_excel(output_path, index=False)
    return common_authors_list

def identifier_exists(identifiers, new_id, id_type_uri):

    for identifier in identifiers:
        if 'type' in identifier and identifier['type']['uri'] == id_type_uri and identifier['id'] == new_id:
            return True
    return False
def update_externalpersons_pure(persons, matched_personsjson, test_choice):
    def get_person_by_uuid(matched_personsjson, target_uuid):
        """
        Retrieves a specific person from the matched_personsjson list based on the provided uuid.

        Parameters:
        matched_personsjson (list): The list of persons from which to retrieve the specific person.
        target_uuid (str): The UUID of the person to retrieve.

        Returns:
        dict: The dictionary of the person that matches the provided uuid. Returns None if no match is found.
        """
        for person in matched_personsjson:
            if person['uuid'] == target_uuid:
                return person
        return None

    logger.info(f"start updating external persons from pure")
    ro, matched_persons, updated_persons, already_ids = 0, 0, 0, 0
    for row in persons:
        uuid = row['Pure_UUID']
        matched_person = get_person_by_uuid(matched_personsjson, uuid)

        if matched_person is None:
            logger.debug(f"Matched person not found for UUID {uuid}")
            continue

        # Initialize identifiers if not already present
        if 'identifiers' not in matched_person:
            matched_person['identifiers'] = []

        new_openalexid = None
        new_orcid = None

        # Create new ORCID object if available
        if row['ORCID']:
            new_orcid = {
                "typeDiscriminator": "ClassifiedId",
                "id": row['ORCID'],
                "type": {
                    "uri": ORCID_ID_URI,
                    "term": {
                        "en_GB": "ORCID"
                    }
                }
            }

        # Create new OpenAlex ID object if available
        if row['Alex_ID']:
            new_openalexid = {
                "typeDiscriminator": "ClassifiedId",
                "id": row['Alex_ID'],
                "type": {
                    "uri": OPENALEXEX_ID_URI,
                    "term": {
                        "en_GB": "Open Alex id"
                    }
                }
            }

        # Check if the new ORCID and OpenAlex ID already exist
        orcid_exists = (
            new_orcid and
            identifier_exists(matched_person['identifiers'], new_orcid['id'], ORCID_ID_URI)
        )

        openalexid_exists = (
            new_openalexid and
            identifier_exists(matched_person['identifiers'], new_openalexid['id'], OPENALEXEX_ID_URI)
        )

        # Add new identifiers if they don't already exist
        identifiers_updated = False
        if new_orcid and not orcid_exists:
            matched_person['identifiers'].append(new_orcid)
            identifiers_updated = True

        if new_openalexid and not openalexid_exists:
            matched_person['identifiers'].append(new_openalexid)
            identifiers_updated = True

        # Update person data in Pure if identifiers were added
        if identifiers_updated:
            updated_persons += 1
            if test_choice == 'no':
                url = PURE_BASE_URL + 'external-persons/' + uuid
                try:
                    response = session.put(url, headers=headers, json=matched_person, verify=False)
                    if response.status_code != 200:
                        logger.debug(f"Failed to update data for UUID {uuid}: {response.text}")
                    else:
                        logger.debug(f"Successfully updated data for UUID {uuid} with ORCID {new_orcid}, OpenAlex {new_openalexid}")

                except Exception as e:
                    logger.error(f"Error updating UUID {uuid}: {e}")
                time.sleep(0.1)  # Adjust the sleep time based on rate limits
            else:

                logger.debug(f"Test mode: would update UUID {uuid} with ORCID {new_orcid}, OpenAlex {new_openalexid}")
        else:
            already_ids += 1

    logger.info(f"end updating external persons from pure")
    logger.info(f"total external persons updated: {updated_persons}")
    logger.info(f"total external persons not updated (already has ids, or no ids found): {already_ids}")




def get_external_persons_data(persons):
    """
    Retrieves the data for all external persons based on their UUIDs using the POST /external-persons/search endpoint.

    Parameters:
    persons (DataFrame): The DataFrame containing the persons with their UUIDs.

    Returns:
    list: A list of JSON objects, each representing an external person.
    """
    logger.info(f"start fetching external persons from pure")
    all_person_data = []
    page_size = 500 # Adjust based on Pure API's max page size

    def split_into_batches(lst, n):
        for i in range(0, len(lst), n):
            yield lst[i:i + n]

    # Split the UUIDs into batches
    uids = [person['Pure_UUID'] for person in persons]
    batches = list(split_into_batches(uids, page_size))
    session = requests.Session()
    count = 0
    for batch in batches:
        count += 1
        logger.debug(f"start fetching external persons for batch {count}")
        json_body = {
            "uuids": batch,
            "size": page_size,
            "offset": 0  # Start with offset 0
        }

        try:
            response = session.post(
                PURE_BASE_URL + 'external-persons/search',
                json=json_body,
                headers=headers,
                verify=False
            )
            time.sleep(0.5)
            response.raise_for_status()
            data = response.json()
            persons_data = data.get("items", [])
            all_person_data.extend(persons_data)

        except requests.exceptions.RequestException as e:
            logger.error(f"An error occurred while processing batch: {batch}\nError: {e}")
    logger.info(f"end fetching pure")
    logger.info(f"Matching external persons found: {len(all_person_data)}")
    return all_person_data

def select_faculties(faculty_choice):
    logger.info(f"start fetching person-roots for {faculty_choice}")

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

        response.raise_for_status()
        return response.json().get("results", [])
    except requests.RequestException as e:
        logger.error(f"Error fetching person-roots for faculty {faculty_key}: {e}")
        return []

def select_researchoutputs(persoonroot_key):
    """Fetch person IDs for a given person-ro    ot."""
    # categories = CATEGORIES
    categories =  ['journal article']
    all_results = []

    for categorie in categories:
        logger.debug(f"fetching {categorie}")
        try:
            params = {'key': persoonroot_key, 'category_want': categorie}
            url = RIC_BASE_URL + 'get_all_neighbor_nodes'
            response = requests.get(url, params=params)

            # response.raise_for_status()
            results = response.json().get("results", [])
            all_results.extend(results)
        except requests.RequestException as e:
            logger.error(f"Error fetching person IDs for person-root {persoonroot_key}: {e}")

    return all_results



def select_persons_researchoutput(selected_faculties):
    all_data = []
    for faculty in selected_faculties:
        logger.info(f"Processing faculty: {faculty}")
        personroots = fetch_personroots(faculty)
        logger.info(f"Processing internal persons: {len(personroots)}")
        new_data = []
        for personroot in personroots:

            if not personroot['_key'] == None:
                personroot_key = personroot['_key']

                outputs = select_researchoutputs(personroot_key)
                for output in outputs:
                    source = output.get("_source", {})
                    if 'Pure-uu' in source and 'OpenAlex-uu' in source:
                        # doi = 'doi.org/' + output["_key"].split("|")[0]
                        doi = output["_key"].split("|")[0]
                        new_data.append(doi)
                    # else:
                        # logger.info(f"{output['_key']} not in both systems")

        all_data.extend(new_data)
    logger.info(f"total pubs found in Ricgraph: {len(all_data)}")
    return all_data


def match_persons(doi, openalexjsons, purejsons):
    persons = []
    oa_article = get_ro_from_openalex(doi, openalexjsons)
    pure_article = get_ro_from_pure(doi, purejsons)

    if oa_article and pure_article:
        persons = match_persons_oa_pure(oa_article, pure_article)
        # updated_persons, already_ids = update_externalpersons_pure(persons, test_choice, updated_persons, already_ids)

    if persons:
        return persons

def fetch_pure_researchoutputs(dois):
    """
    Fetches research outputs from the Pure API for a given list of DOIs and returns a combined JSON object.

    Parameters:
    api_key (str): The API key for accessing the Pure API.
    dois (list): List of DOIs.

    Returns:
    dict: Combined JSON object containing all research outputs.
    """
    logger.info(f"start fetching research outputs from pure")
    url = PURE_BASE_URL + 'research-outputs/search'
    # Function to split the list into batches of size n
    def split_into_batches(lst, n):
        for i in range(0, len(lst), n):
            yield lst[i:i + n]

    # Define batch size for testing
    batch_size = 10

    # Split the DOIs into batches

    deduplicated_dois = list(set(dois))

    # logger.info(f"Total deduplicated items =  {str(len(deduplicated_dois))}")
    batches = list(split_into_batches(deduplicated_dois, batch_size))

    # Initialize an empty list to hold all the research outputs
    all_works = []
    total_dois = set()  # Initialize a set to hold all DOIs
    # Optional: set a delay between requests to avoid hitting rate limits
    request_delay = 0.1  # seconds
    total_items = 0
    # Loop over each batch and make a request
    for batch_index, batch in enumerate(batches):
        pipe_separated_dois = "|".join(batch)
        logger.debug(
            f"Finding research outputs for batch {batch_index + 1}/{len(batches)}, {batch_size} DOIs per batch.")
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
                timeout= 100
            )
            response.raise_for_status()  # Raises an HTTPError for bad responses

            # Parse the response JSON
            data = response.json()
            returned_items = data.get('count')
            logger.debug(f"Total items found for batch {batch_index + 1}: {data.get('count', 0)}")
            total_items += returned_items
            works = data.get("items", [])  # Extract the list of items
            # Create a set of returned DOIs from the API response, normalized to lowercase
            returned_dois = set()
            for item in works:
                for version in item.get("electronicVersions", []):
                    if "doi" in version:
                        returned_dois.add(version["doi"].lower())  # Normalize to lowercase

            total_dois.update(returned_dois)

            # Add the retrieved works to the list
            all_works.extend(works)
            logger.debug(f"Batch {batch_index + 1}/{len(batches)}: Retrieved {len(works)} items.")

        except requests.exceptions.RequestException as e:
            logger.error(f"Error occurred while processing batch {batch_index + 1}: {e}")

        # Optional: Add a delay between requests to avoid hitting rate limits
        time.sleep(request_delay)

    # Combine all works into one JSON object
    pureworks = {"results": all_works}
    # logger.info(f"Total matching research outputs found: {len(pureworks['results'])}")
    logger.info(f"Total matching research outputs found: {str(total_items)}")


    # Save the JSON data to a file
    json_filename = 'research_outputs.json'  # Name of the file to save the JSON data
    try:
        with open(json_filename, 'w', encoding='utf-8') as json_file:
            json.dump(pureworks, json_file, ensure_ascii=False, indent=4)
        logger.info(f"JSON data successfully saved to {json_filename}")
    except IOError as e:
        logger.error(f"Error saving JSON data to file: {e}")
    logger.info(f"end fetching pure")

    # Return or use pureworks as needed
    return pureworks

def fetch_openalex_works(dois):
    """
    Fetches works from OpenAlex API for a given list of DOIs and returns a combined JSON object.

    Parameters:
    dois (list): List of DOIs.

    Returns:
    dict: Combined JSON object containing all works.
    """
    openalexworks = {}
    # Function to split the list into batches of size n
    def split_into_batches(lst, n):
        for i in range(0, len(lst), n):
            yield lst[i:i + n]

    # Regex to match valid DOI format
    doi_pattern = re.compile(r'^10\.\d{4,9}/[-._;()/:A-Z0-9]+$', re.IGNORECASE)

    # Filter valid DOIs
    dois = [doi for doi in dois if doi_pattern.match(doi)]

    logger.info(f"start fetching open alex")

    from tenacity import retry, stop_after_attempt, wait_exponential

    # Split the DOIs into batches of 40 (consistent with the code)
    batches = list(split_into_batches(dois, 40))

    # Initialize an empty list to hold all the works
    all_works = []

    # Define a retry decorator for making requests
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    def fetch_batch(url):
        response = session.get(url)
        response.raise_for_status()
        return response.json()

    # Loop over each batch and make a request
    for batch in batches:
        pipe_separated_dois = "|".join(batch)
        url = f"https://api.openalex.org/works?filter=doi:{pipe_separated_dois}&per-page=50&mailto={EMAIL}"

        try:
            response_data = fetch_batch(url)
            works = response_data.get("results", [])
            all_works.extend(works)

            # Check if there are more pages of results
            while 'next' in response_data.get('meta', {}):
                next_url = response_data['meta']['next']
                response_data = fetch_batch(next_url)
                works = response_data.get("results", [])
                all_works.extend(works)

        except requests.exceptions.RequestException as e:
            logger.error(f"An error occurred while processing batch starting with DOI: {batch[0]}\nError: {e}")

    # Combine all works into one JSON object
    openalexworks = {"results": all_works} if all_works else {}

    # Log the total number of works fetched
    logger.info(f"Total number of works fetched: {len(all_works)}")

    logger.info(f"End fetching from OpenAlex")
    return openalexworks

def match_all_persons(researchoutputs, openalexjsons, purejsons):
    all_persons = []
    for doi in researchoutputs:

        persons = match_persons(doi, openalexjsons, purejsons)
        if persons:
            all_persons = all_persons + persons

    logger.info(f"total persons matched: {len(all_persons)}")
    return all_persons

def main(faculty_choice, test_choice):
    logger.info("Script to update external persons in pure from ricgraph has started")
    logger.info(f"Test run =  {test_choice}")
    faculties = select_faculties(faculty_choice)

    researchoutputs = select_persons_researchoutput(faculties)
    purejsons = fetch_pure_researchoutputs(researchoutputs)
    openalexjsons = fetch_openalex_works(researchoutputs)
    all_persons = match_all_persons(researchoutputs, openalexjsons, purejsons)
    matched_personsjson = get_external_persons_data(all_persons)
    update_externalpersons_pure(all_persons, matched_personsjson, test_choice)
    logger.info(f"Script has ended")

# ########################################################################
# MAIN
# ########################################################################
if __name__ == '__main__':


    parser = argparse.ArgumentParser(description='Update external persons from Ricgraph')
    parser.add_argument('faculty_choice', type=str, nargs='?',
                        # default='uu faculty: information & technology services|organization_name',
                        default='uu faculty: faculteit geowetenschappen|organization_name',
                        help='Faculty choice or "all"')
    parser.add_argument('test_choice', type=str, nargs='?', default='yes', help='Run in test mode ("yes" or "no")')
    args = parser.parse_args()
    main(args.faculty_choice, args.test_choice)
