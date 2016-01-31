# -*- coding: utf-8 -*-
# Create Date 2016/1/30 0030
__author__ = 'wubo'
from ots2 import *
import re

ENDPOINT = "http://samplechr.cn-beijing.ots.aliyuncs.com/"
ACCESSID = "QFmrMPB18qNx9KYc"
ACCESSKEY = "IuAdh4qL9noDf0UnMOO977HSgZSc0E"
INSTANCENAME = "samplechr"
TABLE_NAME = "sample_chr_info"

ots_client = OTSClient(ENDPOINT, ACCESSID, ACCESSKEY, INSTANCENAME)


def alter_cigar(original_cigar, seq, ref):
    # 比对序列指针
    seq_p = 0
    # 参考序列指针
    ref_p = 0
    # 新的cigar
    new_cigar = ""
    # 将原cigar拆分
    unit_list = re.findall(r'[0-9]+[MIDNS]', original_cigar)
    for unit in unit_list:
        type_flag = unit[-1]
        offset = int(unit[0:-1])
        # 若为M则查看有无错配情况 错配记为X
        if type_flag == "M":
            t = 0
            flag = True
            x_seq = ""
            for i in range(0,offset):
                # 若当前位置正确 则判断上个位置是否正确
                if seq[seq_p+t] == ref[ref_p+t]:
                    if flag: t+=1
                    else:
                        new_cigar = new_cigar + str(t) + "X" + x_seq
                        x_seq = ""; t = 1; flag = False
                else:
                    if flag:
                        new_cigar = new_cigar + str(t) + "M"
                        seq_p += t; ref_p += t;
                        x_seq += seq[seq_p+t]
                    else: t+=1

        elif type_flag == "I":
            new_cigar = new_cigar + unit + seq[seq_p:seq_p+offset]
            seq_p += offset
        elif type_flag == "D":
            ref_p += offset


def sam2table(file_path, sample_no):
    with open(file_path,"r") as file:
        for line in file:
            pass
    file = open("d:/wzh_test/bin_wzh.sam","r")
    lines = file.readlines()
    # 行数
    row_nos = len(lines)
    i = 0
    for item in lines:
        i += 1
        line_list = item.split('\t')
        row_no = i
        bin_no = line_list[0]
        qname = line_list[1]
        flag = line_list[2]
        rname = line_list[3]
        pos = line_list[4]
        mapq = line_list[5]
        cigar = line_list[6]
        rnext = line_list[7]
        pnext = line_list[8]
        tlen = line_list[9]
        seq = line_list[10]
        qual = line_list[11]

        chr_no = rname

        # print row_no

        if chr_no == 'X':
            chr_no = '23'
        elif chr_no == 'Y':
            chr_no = '24'
        elif chr_no == 'MT':
            chr_no = '25'
        elif chr_no == '*':
            chr_no = '26'

        if len(chr_no) == 1:
            chr_no = '0'+ chr_no
        sample_no_chr = int(str(sample_no)+chr_no)
        bin_no = int(bin_no)

        try:
            primary_key = {u"sample_no_chr":sample_no_chr,u"bin_no":bin_no,u'qname':qname,'flag':flag}
            # print primary_key
            attribute_columns = {'rname':rname,'pos':pos, 'mapq':mapq, 'cigar':cigar, 'rnext':rnext,
                                 'pnext':pnext, 'tlen':tlen, 'seq':seq, 'qual':qual,'row_no':row_no,
                                 'attribute_qname':qname,'attribute_flag':flag}
            condition = Condition('EXPECT_NOT_EXIST')
            consumed = ots_client.put_row(TABLE_NAME, condition, primary_key, attribute_columns)
            print u'成功插入数据，消耗的写CapacityUnit为：%s' % consumed.write
        except Exception,e:
            print str(e)

def sam2table2():
    file = open(r"C:\Users\code\Desktop\tmp.sam","r")
    lines = file.readlines()
    # 行数
    row_nos = len(lines)
    i = 0
    for item in lines:
        i += 1
        line_list = item.split('\t')
        row_no = i
        bin_no = line_list[0]
        qname = line_list[1]
        rname = line_list[2]
        flag = line_list[3]
        strand = line_list[4]
        cigar = line_list[5]
        start = line_list[6]
        end = line_list[7]
        seq = line_list[8]

        # pos = line_list[4]
        # mapq = line_list[5]
        # cigar = line_list[6]
        # rnext = line_list[7]
        # pnext = line_list[8]
        # tlen = line_list[9]
        # seq = line_list[10]
        # qual = line_list[11]

        chr_no = rname

        # print row_no

        if chr_no == 'X':
            chr_no = '23'
        elif chr_no == 'Y':
            chr_no = '24'
        elif chr_no == 'MT':
            chr_no = '25'
        elif chr_no == '*':
            chr_no = '26'
        sample_no = 7
        if len(chr_no) == 1:
            chr_no = '0'+ chr_no
        sample_no_chr = int(str(sample_no)+chr_no)
        bin_no = int(bin_no)
        try:
            primary_key = {u"sample_no_chr":sample_no_chr,u"bin_no":bin_no,u'qname':qname,'flag':flag}
            # print primary_key
            attribute_columns = {'rname': rname,'start': start, 'cigar':cigar,
                                 'end': end, 'seq': seq, 'row_no':row_no,'strand': strand,
                                 'attribute_qname':qname,'attribute_flag':flag}
            condition = Condition('EXPECT_NOT_EXIST')
            consumed = ots_client.put_row(TABLE_NAME, condition, primary_key, attribute_columns)
            print u'成功插入数据，消耗的写CapacityUnit为：%s' % consumed.write
        except Exception,e:
            print str(e)


        # try:
        #     primary_key = {u"sample_no_chr":sample_no_chr,u"bin_no":bin_no,u'qname':qname,'flag':flag}
        #     # print primary_key
        #     attribute_columns = {'rname':rname,'pos':pos, 'mapq':mapq, 'cigar':cigar, 'rnext':rnext,
        #                          'pnext':pnext, 'tlen':tlen, 'seq':seq, 'qual':qual,'row_no':row_no,
        #                          'attribute_qname':qname,'attribute_flag':flag}
        #     condition = Condition('EXPECT_NOT_EXIST')
        #     consumed = ots_client.put_row(TABLE_NAME, condition, primary_key, attribute_columns)
        #     print u'成功插入数据，消耗的写CapacityUnit为：%s' % consumed.write
        # except Exception,e:
        #     print str(e)

if __name__ == "__main__":
    # file_path = ""
    # alter_cigar("3M2D2I3M","ATC","AAC")
    sam2table2()
    

