import os

BASE_DIR = os.path.join(os.getenv('DATA'), 'ram')

PREPPED_DATA_DIR = os.path.join(BASE_DIR, 'prepped_data')

IMPLEMENTATION_DATA_DIR = os.path.join(BASE_DIR, 'implementation')

SIMULATIONS_DATA_DIR = os.path.join(BASE_DIR, 'simulations')

COMBO_SEARCH_OUTPUT_DIR = os.path.join(BASE_DIR, 'combo_search')

ERN_PEAD_DIR = os.path.join(os.getenv('DATA'), 'ram', 'data', 'temp_ern_pead')

GCP_STORAGE_BUCKET_NAME = 'ram_data'

GCP_CLOUD_IMPLEMENTATION = True
