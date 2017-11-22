import os

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
        GCP_CLOUD_IMPLEMENTATION = True
    else:
        f = open(path, 'w')
        f.write('FLAG = False')
        f.close()
        GCP_CLOUD_IMPLEMENTATION = False

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

BASE_DIR = os.path.join(os.getenv('DATA'), 'ram')

PREPPED_DATA_DIR = os.path.join(BASE_DIR, 'prepped_data')

IMPLEMENTATION_DATA_DIR = os.path.join(BASE_DIR, 'implementation')

SIMULATIONS_DATA_DIR = os.path.join(BASE_DIR, 'simulations')

COMBO_SEARCH_OUTPUT_DIR = os.path.join(BASE_DIR, 'combo_search')

ERN_PEAD_DIR = os.path.join(os.getenv('DATA'), 'ram', 'data', 'temp_ern_pead')

GCP_STORAGE_BUCKET_NAME = 'ram_data'
