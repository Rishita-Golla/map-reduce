import shutil
import sys
import os
import time

sys.path.append(os.path.dirname(os.path.realpath(__file__)) + "/../src")

from master import Master
from util import read_conf, read_output_files, get_faults, get_no_fault, verify_result

sys.path.append(os.path.dirname(os.path.realpath(__file__)) + "/../src/util")
from file_util import read_file


def map_func(line_num, line):
    """
        User defined mapper function
    """
    word_list = line.split(' ')
    words = []
    for word in word_list:
        words.append({word: 1})
    return words


def reduce_func(word, freq_list):
    """
        User defined reducer function
    """
    count = 0
    for freq in freq_list:
        count += freq
    return word, count


if __name__ == '__main__':
    print("----------STARTING WORD COUNT TEST------------")

    def run_map_reduce(input_file, output_dir, n, fault):
        """
            Execute Map Reduce by calling master
        """
        master = Master(input_file, output_dir, n)
        if fault:
            master.execute(map_func, reduce_func, get_faults(n))
        else:
            master.execute(map_func, reduce_func, get_no_fault())
        return output_dir


    def get_mapreduce_result(output_dir):
        """
             Read from output files generated by reducers
         """
        output = read_output_files(output_dir)
        word_count_mapreduce = {}
        for line in output:
            word, count = line.rsplit(':', 1)
            word_count_mapreduce[word] = int(count)
        return word_count_mapreduce


    def get_sequential_result(file):
        """
            Get output using sequential loops to verify result
        """
        data = read_file(file)
        word_count_seq = {}
        for index, line in enumerate(data):
            line = line.rstrip('\n')
            word_list = line.split(' ')
            for word in word_list:
                if word not in word_count_seq:
                    word_count_seq[word] = 0
                word_count_seq[word] += 1
        return word_count_seq

    try:
        input_file, output_dir, n = read_conf("resources/config_word_count.txt")
        if os.path.exists(output_dir):
            shutil.rmtree(output_dir)
        output_seq = get_sequential_result(input_file)
        print("------------------Running Map Reduce for single process (without fault)------------------")
        mr_output_dir = run_map_reduce(input_file, output_dir+'1', 1, False)
        output_mapreduce = get_mapreduce_result(mr_output_dir)
        verify_result(output_mapreduce, output_seq)
        time.sleep(3)
        print("------------------Running Map Reduce for multiple processes (without fault)------------------")
        mr_output_dir = run_map_reduce(input_file, output_dir+'2', n, False)
        output_mapreduce = get_mapreduce_result(mr_output_dir)
        verify_result(output_mapreduce, output_seq)
        time.sleep(3)
        print("------------------Running Map Reduce for multiple processes (with fault)------------------")
        mr_output_dir = run_map_reduce(input_file, output_dir+'3', n, True)
        output_mapreduce = get_mapreduce_result(mr_output_dir)
        verify_result(output_mapreduce, output_seq)
    except ValueError as e:
        print(e)