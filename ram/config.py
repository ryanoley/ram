import os

BASE_DIR = os.path.join(os.getenv('DATA'), 'ram')

PREPPED_DATA_DIR = os.path.join(BASE_DIR, 'prepped_data')

SIMULATION_OUTPUT_DIR = os.path.join(BASE_DIR, 'simulations')
