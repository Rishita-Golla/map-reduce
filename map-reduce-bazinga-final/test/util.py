import os
from random import randint


def read_conf(filename):
    """
        reads data from config file. data: input_file_path, output_dir, n = #mappers = #reducers,
    """
    with open(filename, "r") as test_config_file:
        line = test_config_file.readline()  # assuming config has ony 1 line
        config = line.split(",")
        input_file_path = config[0].strip()
        output_dir = config[1].strip()
        n = int(config[2].strip())
        return input_file_path, output_dir, n


def read_output_files(output_dir):
    """
    Returns the combined output of all reducer outputs
    """
    final_output = []
    if os.path.isdir(output_dir):
        files = [f'{output_dir}/{f}' for f in os.listdir(output_dir)]
        for filename in files:
            with open(filename, "r") as file:
                final_output += file.readlines()
    else:
        print("cannot read files")
    return final_output


def get_faults(num_workers):
    """ to simulate faults in the system. Tells if fault will be created, if yes, then whether to fail mapper or a
    reducer and which mapper id/reducer to kill"""
    mapper_fault = bool(randint(0, 1))
    reducer_fault = not mapper_fault
    faulty_worker_id = randint(0, num_workers - 1)
    faults = {"createMapperFault": mapper_fault, "createReducerFault": reducer_fault, "worker_id": faulty_worker_id}
    return faults


def get_no_fault():
    faults = {"createMapperFault": False, "createReducerFault": False, "worker_id": -1}
    return faults


def verify_result(output_mapreduce, output_seq):
    fail = 0
    for key in output_seq:
        if key not in output_mapreduce.keys():
            print("key not found in mr keys " + key)
            fail = 1
        elif output_mapreduce[key] != output_seq[key]:
            print("key " + key + " : " + str(output_mapreduce[key]) + ", " + str(output_seq[key]))
            fail = 1
            break
    if fail:
        print("oops! Test case FAILED!")
    else:
        print("Hurray! Test case PASSED!")
