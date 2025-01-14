import configparser
import os

config_path = os.path.join(os.path.dirname(__file__), 'config.ini')

if not os.path.exists(config_path):
    raise FileNotFoundError(f"The configuration file {config_path} does not exist.")

config = configparser.ConfigParser()
config.read(config_path)

PURE_BASE_URL = config['PURE-API']['BaseURL']
PURE_API_KEY = config['PURE-API']['APIKey']
RIC_BASE_URL = config['RICGRAPH-API']['BaseURL']
FACULTY_PREFIX = config['RICGRAPH-API']['FacultyPrefix']
OPENALEX_BASE_URL = config['OPENALEX_PURE']['BaseURL']
EMAIL = config['OPENALEX_PURE']['email']
OPENALEX_ID_URI = config['ID_URI']['OPENALEX']
OPENALEXEX_ID_URI = config['ID_URI']['OPENALEXEX']

ORCID_ID_URI = config['ID_URI']['ORCIDEXT']
ROR_ID_URI = config['ID_URI']['ROR_ID_URI']
ID_URI = config['ID_URI']
TYPE_URI = config['URI']
CATEGORIES = config['RICGRAPH-API']['rescat']

DEFAULTS = config['DEFAULTS']


PURE_HEADERS = {
    "Content-Type": "application/json",
    "accept": "application/json",
    "api-key": PURE_API_KEY
}

OPENALEX_HEADERS = {'Accept': 'application/json',
                    'User-Agent': EMAIL
                    }