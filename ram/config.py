import os

BASE_DIR = os.path.join(os.getenv('DATA'), 'ram')

PREPPED_DATA_DIR = os.path.join(BASE_DIR, 'prepped_data')

SIMULATION_OUTPUT_DIR = os.path.join(BASE_DIR, 'simulations')

ERN_PEAD_DIR = os.path.join(os.getenv('DATA'), 'ram', 'data', 'temp_ern_pead')

SKLEARN_NJOBS = 2

GCP_STORAGE_BUCKET_NAME = 'ram_data'
