from collections import defaultdict
import sys
import os

sys.path.append(os.path.dirname(os.path.realpath(__file__)) + "/util")
from file_util import read_file, make_dir, get_dir_same_as_file

""" 
Preprocessor class to do following:
1) Set number of mappers
2) Split the file across multiple mappers so that each mapper only processes one chunk of the file
"""


class Preprocessor:
    def __init__(self, num_mappers):
        self.NUM_OF_MAPPERS = num_mappers

    def split_input_file_for_mappers(self, input_file_path):
        """
            Split the input files to different mappers.
            If file contains 8 lines and there are 2 mappers,
            first mapper would have 1-4 lines and second mapper would have 5-8 lines.
        """
        data = read_file(input_file_path)
        num_lines = len(data)
        # dir_path = os.path.dirname(input_file_path)
        dir_path = get_dir_same_as_file(input_file_path)
        # new_dir_name = os.path.splitext(os.path.basename(input_file_path))[0]
        # dir_path = f'{dir_path}/{new_dir_name}'
        make_dir(dir_path)
        chunk_per_mapper = int(num_lines / self.NUM_OF_MAPPERS)
        if chunk_per_mapper < 1:
            print("Number of lines are less than number of mappers\n")
            chunk_per_mapper = 1
            self.NUM_OF_MAPPERS = num_lines
        idx = 0
        input_file_paths = []
        for mapper_id in range(self.NUM_OF_MAPPERS):
            end = idx + chunk_per_mapper
            if mapper_id == self.NUM_OF_MAPPERS - 1:
                end = num_lines
            data_per_mapper = data[idx: end]
            idx = idx + chunk_per_mapper
            file_name_per_mapper = f'{dir_path}/{mapper_id}.txt'
            input_file_paths.append(file_name_per_mapper)
            for line in data_per_mapper:
                with open(file_name_per_mapper, 'a') as wrt:
                    wrt.write(line)
        return input_file_paths
