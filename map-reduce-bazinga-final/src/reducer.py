import pickle
import os
import time
from file_util import read_file, make_dir

"""
Reducer class to do the following:
1) Load intermediate file for reducer.
2) Read data from all files in the respective reducer directory.
3) Create a key, list of values dictionary from the entire input data
#key_value_list format [{k1:v1,v2,v3}, {k2:v1,v2,v3}, {k3:v3}]
4) For each key in dictionary, call udf reduce function to return key value (result) pair.
5) Add each result to final output array. Write this data to output file for each reducer.
"""


class Reducer:

    def __init__(self, intermediate_path, output_path, reducer_id):
        self.REDUCER_ID = reducer_id
        self.input_path = intermediate_path
        self.final_output_path = output_path

    """ 
        create a dictionary with key and list of values from input data 
    """
    def __set_key_value(self, input_data):
        self.dict = {}
        for l in input_data:
            for d in l:
                for k, v in d.items():
                    if k in self.dict:
                        self.dict[k] += [v]
                    else:
                        self.dict[k] = [v]

    """ 
        read data from all files in the reducer directory
    """
    def __set_input_data(self, input_path):
        input_data = []
        if isinstance(input_path, str):
            if os.path.isdir(input_path):
                input_files = [f'{input_path}/{f}' for f in os.listdir(input_path)]
                for filename in input_files:
                    with open(filename, "rb") as input_file:
                        input_data.append(pickle.load(input_file))

            else:
                raise ValueError("Input path is not a directory")

        return input_data

    """ 
        writes output for each reducer in output file 
    """
    def _write_final_output(self, final_output):

        new_folder = f'{self.final_output_path}/'
        make_dir(new_folder)

        filename = str(self.REDUCER_ID) + '-' + str(int(time.time()))
        reducer_output_file = open(f"{self.final_output_path}/{filename}", 'a+')
        for output_key, output_value in final_output.items():
            output_line = f"{output_key}" + f": {output_value}" + "\n"
            reducer_output_file.write(output_line)
        reducer_output_file.close()

    """
        function to call udf
        call udf reduce and append each key's key, value output
        In case of reducer failure, the status is not updated to done and returned
    """
    def execute(self, udf, return_dict=None, simulate_failure=False):
        """if simulate_failure = true, does not complete the execution of the program and returns"""
        input_data = self.__set_input_data(self.input_path)
        self.__set_key_value(input_data)
        final_output = {}
        for key in self.dict:
            k, v = udf(key, self.dict[key])
            final_output[k] = v

        if simulate_failure:
            return

        return_dict[self.REDUCER_ID] = "Done"
        self._write_final_output(final_output)
