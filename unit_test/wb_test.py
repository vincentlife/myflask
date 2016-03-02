# -*- coding: utf-8 -*-
# Create Date 2015/12/9
__author__ = 'wubo'
import time,re

# 处理 cigar 部分
binOffsets = [512+64+8+1, 64+8+1, 8+1, 1, 0]
binFirstShift = 17
binNextShift = 3


def intial_bin(start, end):
    startBin = start - 1
    endBin = end - 1
    startBin >>= binFirstShift
    endBin >>= binFirstShift
    for i in range(len(binOffsets)):
        if startBin == endBin:
            return binOffsets[i] + startBin
        startBin >>= binNextShift
        endBin >>= binNextShift


def get_length(cigar):
    length = 0
    cigar_list = re.findall(r'\d+\w', cigar)
    for item in cigar_list:
        if 'M' in item or 'N' in item or 'H' in item or 'D' in item:
            length += int(re.compile(r'(\d+)\w').search(item).group(1))
    return length


def get_cigar(cigar, seq):
    cigar_list = re.findall(r'\d+\w', cigar)
    new_cigar_list = []
    seq_p = 0
    for item in cigar_list:
        name = item[-1].upper()
        if name == 'S' or name == 'I':
            sl = int(item[0:-1])
            new_cigar_list.append(str(sl)+"I"+seq[seq_p:seq_p+sl])
            seq_p += sl

        if name == 'H' or name == 'D' or name == 'N' or name == 'P':
            new_cigar_list.append(item[0:-1]+"D")

        if name == 'M':
            ml = int(item[0:-1])
            c = 0
            for m in seq[seq_p:seq_p+ml]:
                if m != '=':
                    new_cigar_list.append(str(c)+"M")
                    new_cigar_list.append("1X"+m)
                    c = 0
                else:
                    c += 1
            if c > 0:
                new_cigar_list.append(str(c)+"M")
            seq_p += ml

    return "".join(new_cigar_list)


def handle_sam(filepath, handle_file):
    fwp = open(handle_file, 'w')
    with open(filepath) as file:
        for line in file:
            if line[0] == '@':
                continue
            line = line.strip('\n')
            line_list = line.split('\t')
            qname = line_list[0]
            chr = line_list[2]
            start = line_list[3]
            mate = line_list[9]
            md = line_list[12]
            #print (line_list[1],bin(line_list[1])
            flag = str(bin(int(line_list[1])))
            strand = int(flag[-5]) + int(flag[-6])
            cigar = line_list[5]
            end = get_length(cigar) + int(start) - 1
            bin_no = intial_bin(int(start), end)
            cigar_str = get_cigar(cigar, mate)
            # print cigar,cigar_str
            tmp_list=[str(bin_no), qname, chr, line_list[1], str(strand), cigar_str, start, str(end)]
            fwp.write('\t'.join(tmp_list)+'\n')


def test(filepath):
    with open(filepath) as file:
        for line in file:
            if line[0] == '@':
                continue
            line = line.strip('\n')
            line_list = line.split('\t')
            cigar = line_list[5]
            mate = line_list[9]
            md = line_list[12]
            if 'D' in cigar or 'N' in cigar:
                print cigar
                print mate
                print md

if __name__ == '__main__':
    sam_file = "C:/Users/guhongjie/Desktop/tt.sam"
    handle_file = "C:/Users/guhongjie/Desktop/out.sam"
    # sam_file = "3908.md.sam"
    # handle_file = "3908.h.sam"
    st = time.time()
    handle_sam(sam_file, handle_file)
    print time.time()-st