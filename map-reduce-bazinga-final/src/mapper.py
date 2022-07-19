from collections import defaultdict
import sys
import os
import hashlib
sys.path.append(os.path.dirname(os.path.realpath(__file__)) + "/util")
from file_util import read_file, make_dir, write_to_pickle_file

""" 
Mapper class to do following:
1) Load input file for the mapper
2) For each line in the input file, call udf map function
3) Concatenate list of key value pairs returned by udf map function in key_value_list 
   #key_value_list format [{k1:v1}, {k1:v2}, {k2:v3}, {k2:v4}]
4) Each K of <K, V> in key_value_list is being assigned to a reducer using a simple hash.
   reducer_to_key_value_list stores all key value pairs for each reducer. 
   format: {r1: [{k1:v1}, {k2:v3}, {k2:v3}], r2: [{k3:v1}, {k3:v2}, {k4:v3}, {k4:v3}]}
5) While storing, create r directories for each hashed reducer.
   Each directory contains output files from all mappers for a that particular reducer.
   Each file (corresponding to mapper) contains the key value pairs traversed by that particular mapper.
"""


class Mapper:
    def __init__(self, output_dir, num_reducers, mapper_id):
        self.OUTPUT_DIR = output_dir
        self.NUM_OF_REDUCERS = num_reducers
        self.MAPPER_ID = mapper_id

        # format: [{k1:v1}, {k1:v2}, {k2:v3}, {k2:v4}]
        self.key_value_list = []

        # reducer_to_key_value_list format: {r1: [{k1:v1}, {k2:v3}, {k2:v3}], r2: [{k3:v1}, {k3:v2}, {k4:v3}, {k4:v3}]}
        self.reducer_to_key_value_list = defaultdict(list)

        # format: [r1, r2] ->  keys are getting hashed to a list of these reducer ids
        self.hashed_reducer_ids = set()

    def execute(self, input_file_path, udf, return_dict, simulate_failure=False):
        """
        Calls udf mapper for each line in input data file.
        Stores the list of key-value pairs generated.
        Shuffles data for input to reducers
        Stores the result in the directory
        if simulate_failure = true, the execution of the program is not completed and returned
        """
        data = read_file(input_file_path)
        for line_num, line in enumerate(data):
            key_value_list_per_line = udf(line_num, line.rstrip('\n'))
            self.append_key_value_list(key_value_list_per_line)
        self.shuffle_intermediate_pairs()

        if simulate_failure:
            return_dict[self.MAPPER_ID] = -1
        else:
            self.write_list_to_file()
            return_dict[self.MAPPER_ID] = self.hashed_reducer_ids

    def append_key_value_list(self, key_value_list_per_line):
        """
            Concatenate key value pairs from all lines
        """
        self.key_value_list += key_value_list_per_line

    def shuffle_intermediate_pairs(self):
        """
        shuffles data for input to reducers by hashing on key. each key is hashed for a particular reducer and
        written in a reducer specific directory
        Example: {reducer_id_1: [{singla: 1}, {singla: 1}], reducer_id_2: [{sarda,1}, {golla, 1}]
        """
        for key_val_pair in self.key_value_list:
            for intermediate_key, intermediate_val in key_val_pair.items():
                if type(intermediate_key) is int:
                    hashed_reducer_id = int(hashlib.sha512(intermediate_key.to_bytes(16, 'little', signed=False)).hexdigest(),
                                            16) % self.NUM_OF_REDUCERS
                else:
                    hashed_reducer_id = int(hashlib.sha512(intermediate_key.encode('utf-8')).hexdigest(), 16) % self.NUM_OF_REDUCERS
                self.hashed_reducer_ids.add(hashed_reducer_id)
                self.reducer_to_key_value_list[hashed_reducer_id].append(key_val_pair)

    def write_list_to_file(self):
        """
            Store shuffled data for each reducer into a separate directory.
        """
        make_dir(f'{self.OUTPUT_DIR}/')
        for reducer_id in self.reducer_to_key_value_list:
            reducer_dir = self.OUTPUT_DIR + '/' + str(reducer_id) + '/'
            make_dir(reducer_dir)
            intermediate_path = reducer_dir + str(self.MAPPER_ID) + '.pickle'
            write_to_pickle_file(intermediate_path, self.reducer_to_key_value_list[reducer_id])
