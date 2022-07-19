import sys
import os
import shutil
import time

sys.path.append(os.path.dirname(os.path.realpath(__file__)) + "/../src")
from master import Master
from util import read_conf, read_output_files, get_faults, verify_result, get_no_fault

sys.path.append(os.path.dirname(os.path.realpath(__file__)) + "/../src/util")
from file_util import read_file

"""
    The input file contains the amount spent by a customer on various orders
    The resulting output file contains the customer id as key and sum of all the purchases for a given customer as the value
"""
def map_func(line_num, line):
    id_value_list = []
    line = line.split(',')
    id_value_list.append({line[0]: line[1]})

    return id_value_list


def reduce_func(cust_id, values):
    total = 0
    for value in values:
        total += int(value)

    return cust_id, total


if __name__ == '__main__':
    print("----------STARTING SUM PER CUSTOMER TEST------------")

    def get_sum_per_cust_seq(input_file):
        data = read_file(input_file)
        total_per_cust = {}
        for index, line in enumerate(data):
            line = line.rstrip('\n')
            cust_id, shop_value = line.split(",")

            if cust_id not in total_per_cust:
                total_per_cust[cust_id] = int(shop_value)
            else:
                total_per_cust[cust_id] += int(shop_value)

        return total_per_cust

    def run_map_reduce(input_file, output_dir, n, fault):
        master = Master(input_file, output_dir, n)
        if fault:
            master.execute(map_func, reduce_func, get_faults(n))
        else:
            master.execute(map_func, reduce_func, get_no_fault())
        return output_dir


    def get_mapreduce_result(output_dir):
        output = read_output_files(output_dir)
        total_per_cust = {}

        for line in output:
            line = line.rstrip('\n')
            cust_id, total = line.split(':')
            total_per_cust[cust_id] = int(total)

        return total_per_cust


    try:
        input_file, output_dir, n = read_conf("resources/config_shop_per_cust.txt")
        if os.path.exists(output_dir):
            shutil.rmtree(output_dir)
        output_seq = get_sum_per_cust_seq(input_file)

        print("------------------Running Map Reduce for single process (without fault)------------------")
        mr_output_dir = run_map_reduce(input_file, output_dir + '1', 1, False)
        output_mapreduce = get_mapreduce_result(mr_output_dir)
        verify_result(output_mapreduce, output_seq)
        time.sleep(3)
        print("------------------Running Map Reduce for multiple processes (without fault)------------------")
        mr_output_dir = run_map_reduce(input_file, output_dir + '2', n, False)
        output_mapreduce = get_mapreduce_result(mr_output_dir)
        verify_result(output_mapreduce, output_seq)
        time.sleep(3)
        print("------------------Running Map Reduce for multiple processes (with fault)------------------")
        mr_output_dir = run_map_reduce(input_file, output_dir + '3', n, True)
        output_mapreduce = get_mapreduce_result(mr_output_dir)
        verify_result(output_mapreduce, output_seq)

    except ValueError as e:
        print(e)
