import os
from sklearn.externals import joblib


def import_models(directory, file_names):
    models = {}
    for file_name in file_names:
        file_path = os.path.join(directory, file_name)
        models[file_name] = joblib.load(file_path)
    return models
