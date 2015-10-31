# author: Cheng Peng
# brief : implements basic Apriori Algorithm, outputs Ck, Lk, and etc.

# -----------------------------------------------------------------------------
# import modules

from itertools import combinations
from collections import defaultdict
import argparse
import glob
import time
import os
import json

# -----------------------------------------------------------------------------
# Apriori algorithm methods


def write_candidates(directory, file_out, candidates, computation_time):
    ## generate files that contain the number of candidates, computation time and candidates collection

    number_candidates = len(candidates)
    file_o = "{0}.txt".format(file_out)
    dst_path = os.path.join(directory, file_o)

    data = dict ((index, tuple(k)) for index, k in enumerate(candidates))

    with open(dst_path, "w") as fo:
        fo.write("The number of generated candidates:" + str(number_candidates) + "\n" + "computation time: " + str(computation_time) + "\n" + json.dumps(data))


def JoinSet(itemset, length):
    ## generate possible candidates by self-joining
    
    return set([i.union(j) for i in itemset for j in itemset if len(i.union(j)) == length])


def SupportCounter(itemSet, transactionList, minSupport, freqSet):
    ## generates frequent candidates
    
    
    c_itemSet = set()  # stores Ci itemsets
    l_itemSet = set()  # stores Li itemsets
    localSet = defaultdict(int)
    
    # ----- computation time for Ci -----
    start_time1 =time.time()
    for item in itemSet:
        for transaction in transactionList:
            if item.issubset(transaction):
                freqSet[item] += 1
                localSet[item] += 1
                c_itemSet.add(item)
    computation_time1 = time.time() - start_time1

    # ----- computation time for Li -----
    start_time2 =time.time()
    for item, count in localSet.items():
        if count >= minSupport:  # basically count the frequency of itemset
            l_itemSet.add(item)
    computation_time2 = time.time() - start_time2

    return c_itemSet, l_itemSet, computation_time1, computation_time2  ## return Ck, Lk, and computation time


def TransactionInstance(transaction_generator):
    ## materialize transaction generator
    
    transactionList = list()  ## transaction list that stores all the transactions
    itemSet = set()  ## a set that stores individual item

    for transaction in transaction_generator:
        transactionList.append(transaction)
        for item in transaction:
            itemSet.add(frozenset([item]))

    return itemSet, transactionList


def TransactionGenerator(file_name):
    ## read dataset into a generator

    with open(file_name, 'r') as dataset:
        head = dataset.readline()
        for line in dataset:
            transactions = frozenset(line.strip().split(' '))  ## make each transaction immutable
            yield transactions


def write_candidates(directory, file_name, prefix, candidates, computation_time):
    ## write candidates to a text file
    
    number_candidates = len(candidates)
    data = dict ((index, tuple(k)) for index, k in enumerate(candidates))
    sec_to_nanosec = computation_time * 10 ** 9
    
    name = os.path.basename(file_name)
    name_wo_extension = os.path.splitext(name)[0]
    file_out = name_wo_extension + "_" + prefix
    file_o = "{0}.txt".format(file_out)
    dst_path = os.path.join(directory, file_o)
    
    with open(dst_path, "w") as fo:
        fo.write("The number of generated candidates:" + str(number_candidates) + "\n" + "computation time in nanosecond: " + str(sec_to_nanosec) + "\n" + json.dumps(data))


def write_summary_files(directory, file_name, prefix, table):
    ## write a dictionary to text file

    name = os.path.basename(file_name)
    name_wo_extension = os.path.splitext(name)[0]
    file_out = name_wo_extension + "_" + prefix
    file_o = "{0}.txt".format(file_out)
    dst_path = os.path.join(directory, file_o)

    with open(dst_path, "w") as fo:
        fo.write(json.dumps(table))


def candidates_summary(directory, file_name, all_freq, all_infreq, counts_table, minS, countC, countL, countT):
    ## generates files that contains frequent and infrequent candidates with support counts

    frequents_table = defaultdict(int)
    for level, frequents in all_freq.items():
        for f_candidate in frequents:
            if f_candidate in counts_table.keys():
                frequents_table[str(f_candidate)] = counts_table[f_candidate]

    infrequents_table = defaultdict(int)
    for level, in_frequents in all_infreq.items():
        for i_candidate in in_frequents:
            if i_candidate in counts_table.keys():
                infrequents_table[str(i_candidate)] = counts_table[i_candidate]

    summary_items = dict()
    countT = countC + countL
    summary_items['The minSupport value'] = minS
    summary_items['Total CandidatesGenerator computation time in nanosecond'] = countC * 10 ** 9
    summary_items['Total SupportCoutner computation time in nanosecond'] = countL * 10 ** 9
    summary_items['Total computation time in nanosecond'] = countT * 10 ** 9
    summary_items['Total number of frequent itemsets'] = len(frequents_table)
    summary_items['Total number of infrequent itemsets'] = len(infrequents_table)

    # ----- write data -----

    Fprefix = "Frequent"
    Ifprefix = "Infrequent"
    Suprefix = "Summary"

    write_summary_files(directory, file_name, Fprefix, frequents_table)
    write_summary_files(directory, file_name, Ifprefix, infrequents_table)
    write_summary_files(directory, file_name, Suprefix, summary_items)


def Apriori(dst_directory, file_name, min_sup, candidates_dict):
    ## implement basic algorithm
    
    Cprefix = "C"
    Lprefix = "L"
    
    tC = 0   ## time counters
    tL = 0
    
    index = 1
    transactionList = list()       ## stores sets of transactions
    all_freq_candidates = dict()      ## stores all frequent candidates at each Lk
    all_infreq_candidates = dict()       ## stores all in-frequent candidates at each Ck
    
        
    transactions_generator = TransactionGenerator(file_name)
    candidates_set, transaction_list = TransactionInstance(transactions_generator)  ## n-itemsets, transactions db
        
    # ----- write data -----
        
    c_prefix = Cprefix + str(index)
    l_prefix = Lprefix + str(index)
        
    candidates, freq_candidates, c_time, l_time = SupportCounter(candidates_set, transaction_list, min_sup, candidates_dict)
    write_candidates(dst_directory, file_name, c_prefix, candidates, c_time)
    write_candidates(dst_directory, file_name, l_prefix, freq_candidates, l_time)
    all_infreq_candidates[index] = candidates
    tC += c_time
    tL += l_time
    
    index += 1
    while(freq_candidates != set([])):
            
        all_freq_candidates[index-1] = freq_candidates
        candidates_set = JoinSet(freq_candidates, index)
        candidates, freq_candidates, c_time, l_time = SupportCounter(candidates_set, transaction_list, min_sup, candidates_dict)
        c_prefix = Cprefix + str(index)
        l_prefix = Lprefix + str(index)
        write_candidates(dst_directory, file_name, c_prefix, candidates, c_time)
        write_candidates(dst_directory, file_name, l_prefix, freq_candidates, l_time)
        all_infreq_candidates[index] = candidates
        
        index += 1
        tC += c_time
        tL += c_time

    return all_freq_candidates, all_infreq_candidates, tC, tL


def main():
    
    # ----- parse arguments -----
    
    parser = argparse.ArgumentParser(description='Implement Apriori Algorithm')
    parser.add_argument('minSupport', help="Require a min support")
    parser.add_argument('dataLocation', help="Require a dataset location")
    parser.add_argument('dstDirectory', help="Require an output directory")
    
    args = parser.parse_args()
    min_sup = int(args.minSupport)
    data_loc = args.dataLocation
    dst_directory = str(args.dstDirectory)
    
    # ----- make a directory -----
    
    if not os.path.exists(dst_directory):
        os.makedirs(dst_directory)
    
    # ----- get dataset files -----
    
    file_list = glob.glob(os.path.join(data_loc,'*.txt'))

    # ----- implement Apriori -----

    for file_name in file_list:
        
        totalComputation_time = 0
        candidates_dict = defaultdict(int)    ## global dictionary that stores all candidates with support counts
        all_freq_candidates, all_infreq_candidates, totalCandidates_time, totalSupport_time = Apriori(dst_directory, file_name, min_sup, candidates_dict)
        
        # ----- write summary -----
        candidates_summary(dst_directory, file_name, all_freq_candidates, all_infreq_candidates, candidates_dict, min_sup, totalCandidates_time, totalSupport_time, totalComputation_time)


# -----------------------------------------------------------------------------
# run script

if __name__ == '__main__':
    main()




