import os
import pickle


def read_file(input_file_path):
    with open(input_file_path, 'r') as input_file:
        return input_file.readlines()


def make_dir(path):
    if not os.path.exists(path):
        os.makedirs(path, exist_ok=True)


def write_to_pickle_file(path, data):
    with open(path, 'wb') as handle:
        pickle.dump(data, handle, protocol=pickle.HIGHEST_PROTOCOL)


def get_dir_same_as_file(path):
    dir_path = os.path.dirname(path)
    new_dir_name = os.path.splitext(os.path.basename(path))[0]
    dir_path = f'{dir_path}/{new_dir_name}'
    return dir_path
