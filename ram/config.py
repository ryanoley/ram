import os
import sys

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
try:
    from ram import cloud
    GCP_CLOUD_IMPLEMENTATION = cloud.FLAG
except:
    print("\n[Error] - Is this a GOOGLE cloud implementation?")
    user_input = raw_input("Enter 'y' if yes: ")
    path = os.path.join(os.getenv('GITHUB'), 'ram', 'ram', 'cloud.py')
    if user_input == 'y':
        f = open(path, 'w')
        f.write('FLAG = True')
        f.close()
    else:
        f = open(path, 'w')
        f.write('FLAG = False')
        f.close()
    print("Re-install python package before running")
    sys.exit()

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

BASE_DIR = os.path.join(os.getenv('DATA'), 'pydata')

PREPPED_DATA_DIR = os.path.join(BASE_DIR, 'prepped_data')

IMPLEMENTATION_DATA_DIR = os.path.join(BASE_DIR, 'implementation')

SIMULATIONS_DATA_DIR = os.path.join(BASE_DIR, 'simulations')

POSITION_SHEET_DIR = os.path.join(BASE_DIR, 'position_sheet')

MODEL_SELECTION_OUTPUT_DIR = os.path.join(BASE_DIR, 'model_selection')

ERN_PEAD_DIR = os.path.join(os.getenv('DATA'), 'ram', 'data', 'temp_ern_pead')

GCP_STORAGE_BUCKET_NAME = 'ram_data'
