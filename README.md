# Project Documentation

## Overview

This project consists of several Python scripts designed to handle and process data from various research information management systems and datasets, including Pure, YODA, OpenAlex, and DataCite. The scripts facilitate tasks such as fetching, parsing, and formatting data, making it easier to integrate and utilize information from these diverse sources. All these sources are combine in Ricgraph. BackToPure uses Ricgraph to update/enrich pure
all the scripts can be used via a webinterface. just start BackToPure.py and this will generate a website from which the scripts can be launched

## Prerequisites
- **Pure**: You need access to the Pure research information management system.
- **CRUD API**: Ensure you have access to the CRUD API provided by Pure. the apikey should be in the config.ini
- **API User Rights**: The API user must have all the necessary rights to access and modify data in Pure.
- **RicGraph**: you  must have access to a Ricgraph instance and the url for the api calls

## Usage
1. clone this repo on your machine
2. install the dependencies (in requirements.txt)
3. configure the crud api from pure and use the api-key in the config ini
4. add the following to the config.ini:
    - url for ricgraph api
    - email address for open alex api
    - FacultyPrefix = {the faculty prefix from you ricgraph instance
    - fill in the defaults from your pure instance in the config.ini (among others the uuid of the organization of the university and publisher)



## Scripts
1. Enrich Internal Persons with IDs
2. Enrich External Persons with IDs
3. Import Research Outputs
4. Import Datasets
5. Enrich External Organisations
