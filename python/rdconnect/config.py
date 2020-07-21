import json

def readFilesList(list_path):
    paths = []
    with open(list_path) as files_paths:
        paths = files_paths.read().splitlines()
        print(paths)
    return paths

def readConfig(config_path):
    print(">>>>> config_path: {}".format(config_path))
    with open(config_path) as data_file:
        data = json.load(data_file)
    return data
