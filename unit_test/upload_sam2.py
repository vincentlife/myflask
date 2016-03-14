# -*- coding: utf-8 -*-
# !/usr/bin/python
# Create Date 2016/3/2 0002
__author__ = 'wubo'
import multiprocessing,datetime
from ots2 import *
# 内网数据
OTS_ENDPOINT = "http://samplechr.cn-beijing.ots-internal.aliyuncs.com/"
# OTS_ENDPOINT = "http://samplechr.cn-beijing.ots.aliyuncs.com/"
OSS_ENDPOINT = "oss-cn-shenzhen-internal.aliyuncs.com"
ACCESSID = "QFmrMPB18qNx9KYc"
ACCESSKEY = "IuAdh4qL9noDf0UnMOO977HSgZSc0E"
INSTANCENAME = "samplechr"
TABLE_NAME = "sample_chr_info"


# 特殊染色体对应数字
chr_no_swich = {
    "X":'23',
    "Y":'24',
    "MT":'25',
    "*": '26'
}

# 上传数据的进程
def upload_proc(file_path,start_line,end_line,sample_no,id):
    ots_client = OTSClient(OTS_ENDPOINT, ACCESSID, ACCESSKEY, INSTANCENAME)
    print "process %d start" % id
    with open(file_path) as file:
        c = 0
        i = 0
        pk_set = set()
        item_list = []
        for line in file:
            # 定位到开始位置
            if c < start_line:
                c += 1
                continue
            if c >= end_line:
                c += 1
                break
            line_list = line.split('\t')
            row_no = i
            # read对应的bin_no
            bin_no = line_list[0]
            # read名称
            qname = line_list[1]
            # 参考染色体号 1~22 X Y M
            rname = line_list[2]
            # 标识位
            flag = line_list[3]
            # strand 0 负链 1 正链
            strand = line_list[4]
            # cigar 主要数据
            cigar = line_list[5]
            # read 起始位置
            start = line_list[6]
            end = line_list[7]
            chr = chr_no_swich.get(rname, str(rname).zfill(2))
            sample_no_chr = int(str(sample_no)+chr)
            # ignore 为忽视主键是否存在 EXPECT时重复则抛出条件错误ConditionError
            # condition = Condition('EXPECT_NOT_EXIST')
            condition = Condition('IGNORE')
            primary_key = {u"sample_no_chr": sample_no_chr, u"bin_no": int(bin_no), u'qname': qname, u'flag': flag}
            if (sample_no_chr,int(bin_no),qname,flag) not in pk_set:
                pk_set.add((sample_no_chr,int(bin_no),qname,flag))
                attribute_columns = {'rname': rname,'start': start, 'cigar':cigar,
                                 'end': end, 'row_no':row_no,'strand': strand,
                                 'attribute_qname':qname,'attribute_flag':flag}
                put_row_item = PutRowItem(condition, primary_key, attribute_columns)
                item_list.append(put_row_item)
                i += 1
            if i > 169:
                table_item1 = {'table_name': TABLE_NAME, 'put': item_list, 'update':[], 'delete':[]}
                batch_list = [table_item1]
                batch_write_response = ots_client.batch_write_row(batch_list)
                print "process %d insert" % id
                pk_set.clear()
                item_list = []
                i = 0
            c += 1
    print datetime.datetime.now()


def sam2ts(filepath,sample_no):
    # 统计文件行数
    l = 0
    with open(filepath) as file:
        for line in file:
            l += 1
    # l = 98818961
    # 等分为20
    step = l/20
    start_line = 0
    end_line = start_line + step
    for i in range(19):
        multiprocessing.Process(name="worker%d"% i,target=upload_proc,args=[filepath,start_line,end_line,sample_no,i]).start()
        start_line = end_line
        end_line = start_line + step
    multiprocessing.Process(name="worker19",target=upload_proc,args=[filepath,start_line,l+1,sample_no,19]).start()


if __name__ == "__main__":
    print datetime.datetime.now()
    handle_file = "3908.sam"
    sample_no = 3908
    sam2ts(handle_file, sample_no)

