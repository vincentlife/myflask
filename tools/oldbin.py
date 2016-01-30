#! /usr/bin/env python

__author__ = 'dell'

import json
import time
from operator import itemgetter

binOffsets = [512+64+8+1, 64+8+1, 8+1, 1, 0]
binFirstShift = 17
binNextShift = 3
file = 'test_reads1.txt'
fwp = open('results.txt', 'w')


def intial_bin(start, end):
    startBin = start - 1
    endBin = end-1
    startBin >>= binFirstShift
    endBin >>= binFirstShift
    for i in range(len(binOffsets)):
        if startBin == endBin:
            return binOffsets[i] + startBin
        startBin >>= binNextShift
        endBin >>= binNextShift


def assign_bin_reads():
    fp = open(file, 'r')
    #lines = fp.readlines()
    chr_dict = {}
    for line in fp.xreadlines():
        line = line.strip('\n')
        pos_list = line.split('\t')
        bin = intial_bin(int(pos_list[1]), int(pos_list[2]))
        if pos_list[0] not in chr_dict:
            chr_dict[pos_list[0]] = {}
        if bin not in chr_dict[pos_list[0]]:
            chr_dict[pos_list[0]][bin] = []
        chr_dict[pos_list[0]][bin].append([pos_list[1], pos_list[2], pos_list])
    return json.dumps(chr_dict)


def one_bin(start, end):
    startBin = start
    endBin = end - 1
    startBin >>= binFirstShift
    endBin >>= binFirstShift
    bin_list = []
    for i in range(len(binOffsets)):
        bin_list.extend([binOffsets[i] + startBin, binOffsets[i] + endBin])
        startBin >>= binNextShift
        endBin >>= binNextShift
    bin_set = set(bin_list)
    bin_list = list(bin_set)
    return bin_list


def query_reads(chr, start, end, bin_dict):
    i = 0
    begin = time.time()
    bin_list = one_bin(start, end)
    result = []
    if chr in bin_dict:
        chr_dict = bin_dict[chr]
        for bin in bin_list:
            if str(bin) in chr_dict:
                for items in chr_dict[str(bin)]:
                    if int(items[0]) <= end and int(items[1]) >= start:
                        i += 1
                        result.append([chr, int(items[0]), int(items[1])])
                        # fwp.write(items[2])
                        # fwp.write('\n')
    total = time.time() - begin
    print total
    print i
    #fwp.close()
    return result


def query_reads2(chr, start, end):
    fp = open(file, 'r')
    lines = fp.readlines()
    fp.close()
    count = 0
    for line in lines:
        line = line.strip('\n')
        list1 = line.split('\t')
        if chr == list1[0] and int(list1[1]) <= end and int(list1[2]) >= start:
            count += 1
    print count

def show_reads(result):
    begin = time.time()
    result = sorted(result, key=itemgetter(1, 2))
    show_dict = {}
    show_dict[0] = []
    show_dict[0].append(result[0])
    length = len(result)
    for i in range(1, length):
        for key in show_dict:
            if show_dict[key][-1][2] < result[i][1]:
                show_dict[key].append(result[i])
                break
            if key+1 not in show_dict:
                show_dict[key+1] = []
                show_dict[key+1].append(result[i])
                break
    total = time.time() - begin
    #print show_dict
    print total
    for key in show_dict:
        for i in range(len(show_dict[key])):
            length = int(show_dict[key][i][2]) - int(show_dict[key][i][1]) -1
            fwp.write(str(show_dict[key][i][1]))
            for j in range(length):
                fwp.write('*')
            fwp.write(str(show_dict[key][i][2]))
            if i<len(show_dict[key])-1:
                length2 = int(show_dict[key][i+1][1]) - int(show_dict[key][i][2]) + 1
                for k in range(length2):
                    fwp.write('_')
        fwp.write('\n')
    return show_dict


def count_reads(show_dict):
    count = 0
    for key in show_dict:
        for item in show_dict[key]:
            count += 1
    print count


def test_chr_dict():
    chr_dict = {}
    chr_dict['chr1']={}
    bin_dict = {}
    bin_dict['585'] = []
    length = 128*1024
    for i in range(length):
        i = i
        end = i + 199
        bin_dict['585'].append([i, end, ['chr1', i, end]])
    for i in range(length):
        i = i
        end = i + 199
        bin_dict['585'].append([i, end, ['chr1', i, end]])
    for i in range(length):
        i = i * 2
        end = i + 200
        bin_dict['585'].append([i, end, ['chr1', i, end]])
    for i in range(length):
        i = i * 2
        end = i + 200
        bin_dict['585'].append([i, end, ['chr1', i, end]])
    for i in range(length):
        i = i + 1
        end = i + 200
        bin_dict['585'].append([i, end, ['chr1', i, end]])
    chr_dict['chr1'] = bin_dict
    return chr_dict

if __name__ == '__main__':
    bin_dict = json.loads(assign_bin_reads())
    #bin_dict = test_chr_dict()
    begin = time.time()
    result = query_reads('chr1', 1000, 2000, bin_dict)
    query_reads2('chr1', 1000, 2000)
    show_dict = show_reads(result)
    #print time.time() - begin
    count_reads(show_dict)



