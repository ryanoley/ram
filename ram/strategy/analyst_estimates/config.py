import os
import sys

SKLEARN_NJOBS = 2

GCP_CLOUD_IMPLEMENTATION = False

BASE_DIR = os.path.join(os.getenv('DATA'), 'ram')

PREPPED_DATA_DIR = os.path.join(BASE_DIR, 'prepped_data')

IMPLEMENTATION_DATA_DIR = os.path.join(BASE_DIR, 'implementation')

SIMULATIONS_DATA_DIR = os.path.join(BASE_DIR, 'simulations')

POSITION_SHEET_DIR = os.path.join(BASE_DIR, 'position_sheet')

MODEL_SELECTION_OUTPUT_DIR = os.path.join(BASE_DIR, 'model_selection')

ERN_PEAD_DIR = os.path.join(os.getenv('DATA'), 'ram', 'data', 'temp_ern_pead')

GCP_STORAGE_BUCKET_NAME = 'ram_data'
