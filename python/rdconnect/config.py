import json


def readConfig(config_path):
    with open(config_path) as data_file:
        data = json.load(data_file)
    return data