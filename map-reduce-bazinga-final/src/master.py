import shutil
import multiprocessing
import os
import sys
import time

from mapper import Mapper
from reducer import Reducer
from preprocessor import Preprocessor

sys.path.append(os.path.dirname(os.path.realpath(__file__)) + "/util")
from file_util import get_dir_same_as_file


class Master:
    """Master of the Map-Reduce system"""

    # Milestone 1: number of mappers and reducers is 1 and only one file being read
    def __init__(self, input_file_path, output_dir, n=1):
        self.ORIGINAL_INPUT_FILE = input_file_path  # will become input_files once multi-threading is implemented
        self.OUTPUT_DIR = output_dir
        self.TMP_DIR = f"{output_dir}/tmp"
        self.NUM_OF_MAPPERS = n
        self.NUM_OF_REDUCERS = n
        preprocessor = Preprocessor(n)
        self.INPUT_FILE_PATHS = preprocessor.split_input_file_for_mappers(input_file_path)
        self.processes = []

    def execute(self, udf_map_fn, udf_reduce_fn, faults):
        """executes the map-reduce workflow
        faults: to simulate faults in the system. Indicates if fault will be created, if yes, then whether to fail mapper or a
        reducer and the Id of mapper/reducer worker to kill
        executes mapper function to return active hashed reducer Ids
        executes reducer function to produce final output files"""
        create_mapper_fault = faults["createMapperFault"]
        create_reducer_fault = faults["createReducerFault"]
        worker_id = faults["worker_id"]

        hashed_reducer_ids = self.call_mappers(udf_map_fn, create_mapper_fault, worker_id)
        self.call_reducers(hashed_reducer_ids, udf_reduce_fn, create_reducer_fault, worker_id)
        self.delete_tmp_dir()

    def call_mappers(self, udf_map_fn, create_fault=False, worker_id=-1):
        """calls the map fn of the library. returns the set of reducer ids hashed by mapper"""
        hashed_reducer_ids = set()  # reducer ids returned by the mapper as mapper hashes keys to different reducers.
        # in multi-threading system, multiple mappers might hash to same reducer id. Master needs to convert list to
        # set as list may contain same reducer id multiple times.

        manager = multiprocessing.Manager()
        return_dict = manager.dict()
        for mapper_id in range(self.NUM_OF_MAPPERS):
            mapper = Mapper(self.TMP_DIR, self.NUM_OF_REDUCERS, mapper_id)
            create_fault_for_mapper = create_fault and worker_id == mapper_id
            process = multiprocessing.Process(target=mapper.execute, args=(self.INPUT_FILE_PATHS[mapper_id], udf_map_fn,
                                                                           return_dict, create_fault_for_mapper))
            self.processes.append(process)
            process.start()

        for proc in self.processes:
            proc.join()

        #For mapper fault, restart the killed worker and execute mapper
        for mapper_id, val in return_dict.items():
            if val == -1:
                mapper = Mapper(self.TMP_DIR, self.NUM_OF_REDUCERS, mapper_id)
                mapper.execute(self.INPUT_FILE_PATHS[mapper_id], udf_map_fn, return_dict)
                val = return_dict[mapper_id]
            hashed_reducer_ids.update(val)
        return hashed_reducer_ids

    def call_reducers(self, hashed_reducer_ids, udf_reduce_fn, create_fault=False, worker_id=-1):
        """
        calls the reduce fn of the library for each reducer id
        Using multiprocessing, execute reducers. In case of reducer fault the status is not marked done, restart the reducer worker.
        """

        processes = []
        manager = multiprocessing.Manager()
        return_dict = manager.dict()
        for reducer_id in hashed_reducer_ids:
            create_fault_for_reducer = create_fault and worker_id == reducer_id
            reducer_input_files = f'{self.TMP_DIR}/{reducer_id}'  # directory of all files hashed to this reducer
            reducer = Reducer(reducer_input_files, self.OUTPUT_DIR, reducer_id)
            process = multiprocessing.Process(target=reducer.execute,
                                              args=(udf_reduce_fn, return_dict, create_fault_for_reducer))
            processes.append(process)
            process.start()

        for proc in processes:
            proc.join()

        for reducer_id in hashed_reducer_ids:
            if reducer_id not in return_dict.keys() or return_dict[reducer_id] != "Done":
                reducer_input_files = f'{self.TMP_DIR}/{reducer_id}'  # directory of all files hashed to this reducer
                reducer = Reducer(reducer_input_files, self.OUTPUT_DIR, reducer_id)
                reducer.execute(udf_reduce_fn, return_dict)

    def delete_tmp_dir(self):
        """delete all temporary files created"""
        shutil.rmtree(self.TMP_DIR)
        shutil.rmtree(get_dir_same_as_file(self.ORIGINAL_INPUT_FILE))

