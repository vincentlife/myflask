#! /usr/bin/env python
# coding: utf-8
# author wubo
# 2016/2/25
from ots2 import *
import json,re,threading,oss2,ots2,subprocess
from itertools import islice
OTS_ENDPOINT = "http://samplechr.cn-beijing.ots-internal.aliyuncs.com/"
# OTS_ENDPOINT = "http://samplechr.cn-beijing.ots.aliyuncs.com/"
OSS_ENDPOINT = "oss-cn-shenzhen-internal.aliyuncs.com"
ACCESSID = "QFmrMPB18qNx9KYc"
ACCESSKEY = "IuAdh4qL9noDf0UnMOO977HSgZSc0E"
INSTANCENAME = "samplechr"
TABLE_NAME = "sample_chr_info"

ots_client = OTSClient(OTS_ENDPOINT, ACCESSID, ACCESSKEY, INSTANCENAME)

def insert2table(TableName, item_list):
    def add_batch_write_item(batch_list, table_name, operation, item):
        for table_item in batch_list:
            if table_item.get('table_name') == table_name:
                operation_item = table_item.get(operation)
                if not operation_item:
                    table_item[operation] = [item]
                else:
                    operation_item.append(item)
                return
        # not found
        table_item = {'table_name': table_name, operation: [item]}
        batch_list.append(table_item)
    table_item1 = {'table_name': TableName, 'put': item_list, 'update':[], 'delete':[]}
    batch_list = [table_item1]
    batch_write_response = ots_client.batch_write_row(batch_list)
    # 每一行操作都是独立的，需要分别判断是否成功。对于失败子操作进行重试。
    retry_count = 0
    operation_list = ['put', 'update', 'delete']
    while retry_count < 3:
        failed_batch_list = []
        for i in range(len(batch_write_response)):
            table_item = batch_write_response[i]
            for operation in operation_list:
                operation_item = table_item.get(operation)
                if not operation_item:
                    continue
                # print u'操作：%s' % operation
                for j in range(len(operation_item)):
                    row_item = operation_item[j]
                    # print u'操作是否成功：%s' % row_item.is_ok
                    if not row_item.is_ok:
                        print "error %s" % row_item.error_code
                        # print u'错误码：%s' % row_item.error_code
                        # print u'错误信息：%s' % row_item.error_message
                        add_batch_write_item(failed_batch_list, batch_list[i]['table_name'], operation, batch_list[i][operation][j])
                    # else:
                        # print "success CapacityUnit: %s" % row_item.consumed.write
                        # print u'本次操作消耗的写CapacityUnit为：%s' % row_item.consumed.write

        if not failed_batch_list:
            break
        retry_count += 1
        batch_list = failed_batch_list
        batch_write_response = ots_client.batch_write_row(batch_list)
        # print batch_write_response


def sam2table(filepath,sample_no):
    chr_no_swich = {
        "X":'23',
        "Y":'24',
        "MT":'25',
        "*": '26'
    }
    pk_set = set()
    with open(filepath, "r") as file:
        i = 0
        item_list = []
        for line in file:
            line_list = line.split('\t')
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
            chr = chr_no_swich.get(rname, str(rname).zfill(2))
            sample_no_chr = int(str(sample_no)+chr)
            # condition = Condition('EXPECT_NOT_EXIST')
            condition = Condition('IGNORE')
            primary_key = {u"sample_no_chr": sample_no_chr, u"bin_no": int(bin_no), u'qname': qname, u'flag': flag}
            if (sample_no_chr,int(bin_no),qname,flag) not in pk_set:
                pk_set.add((sample_no_chr,int(bin_no),qname,flag))
                attribute_columns = {'rname': rname,'start': start, 'cigar':cigar,
                                 'end': end, 'seq': seq, 'row_no':row_no,'strand': strand,
                                 'attribute_qname':qname,'attribute_flag':flag}
                put_row_item = PutRowItem(condition, primary_key, attribute_columns)
                item_list.append(put_row_item)
                i += 1
            if i > 169:
                insert2table(TABLE_NAME, item_list)
                pk_set.clear()
                item_list = []
                i = 0

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


def ref_dict(chr_no):

    list1 = []
    chr_dict = {}
    filename = '/data/ref/chr' + str(chr_no)
    with open(filename,'r') as file:
        for line in file:
            line = line.strip('\n')
            if '>' in line:
                continue
            list1.append(line)
    str1 = ''.join(list1)
    chr_dict[str(chr_no)] = str1
        #print str1
    return chr_dict


def get_cigar(mate, template, cigar):
    cigar_list = re.findall(r'\d+\w', cigar)
    cigar_len = re.findall(r'\d+', cigar)
    cigar_str = ''
    s22 = 0
    s2 = 0
    #print template
    #print mate
    for i in range(len(cigar_list)):
        if 'M' in cigar_list[i].upper():
            s1 = s2
            s11 = s22
            s2 = s1 + int(cigar_len[i])
            s22 = s11 + int(cigar_len[i])
            map_seq = mate[s1:s2]
            map_template = template[s11:s22]
            m = 0
            x = 0
            xx = []
            #print map_template
            #print map_seq
            #print cigar
            for j in range(len(map_seq)):
                if map_seq[j].upper() == map_template[j].upper():
                    if x != 0:
                        cigar_str += str(x) + 'X' + ''.join(xx)
                        x = 0
                    xx = []
                    m += 1
                else:
                    if m != 0:
                        cigar_str += str(m) + 'M'
                        m = 0
                    x += 1
                    xx.append(map_seq[j])
            if m != 0:
                cigar_str += str(m) + 'M'
            if x != 0:
                cigar_str += str(x) + 'X' + ''.join(xx)
        elif 'S' in cigar_list[i].upper() or 'I' in cigar_list[i].upper():
            s1 = s2
            s2 = s1 + int(cigar_len[i])
            cigar_str += mate[s1:s2]
        elif 'N' in cigar_list[i].upper() or 'H' in cigar_list[i].upper() or 'D' in cigar_list[i].upper():
            cigar_str += cigar_len[i] + re.compile(r'\d+(\w)').search(cigar_list[i].upper()).group(1)
            s22 += int(cigar_len[i])
    return cigar_str


def handle_sam(file_path, outfile):
    fwp = open(outfile, 'a')
    chrlist = [1,2,3,4,5,6,7,8,9,10,11,12,'M',13,14,15,16,17,18,19,20,21,22,'X','Y']
    for item in chrlist:
        chr_dict = ref_dict(item)
        with open(file_path, 'r') as sam_file:
            for line in sam_file:
                line = line.strip('\n')
                line_list = line.split('\t')
                qname = line_list[0]
                start = line_list[3]
                mate = line_list[9]
                #print (line_list[1],bin(line_list[1])
                flag = str(bin(int(line_list[1])))
                cigar = line_list[5]
                chr = line_list[2]
                end = get_length(cigar) + int(start) - 1
                bin_no = intial_bin(int(start), end)
                if chr in chr_dict:
                    template = chr_dict[chr][int(start)-1:end]
                #i += 1
                #if i == 85:
                #	break
                    cigar_str = get_cigar(mate, template, cigar)
                #print cigar_str
                    strand = int(flag[-5]) + int(flag[-6])
                    tmp_list = []
                    fwp.write(str(bin_no))
                    tmp_list.extend([qname, line_list[2], line_list[1], strand, cigar_str, start, end, mate])
                    for item in tmp_list:
                        fwp.write('\t')
                        fwp.write(str(item))
                    fwp.write('\n')
    fwp.close()


def handle_thread(account, sample_no, task_id):
    auth = oss2.Auth(ACCESSID, ACCESSKEY)
    bucket = oss2.Bucket(auth,OSS_ENDPOINT, 'jingyun-shenzhen')
    # remote_file = "jingyun-output/{account}/{sample_no}/{sample_no}.dedup.bam".format(account=account,sample_no=sample_no)
    remote_file = "jingyun-output/budechao/3894_R1.target.bam"
    local_file = "{sample_no}.dedup.bam".format(sample_no=sample_no)
    sam_file = "{sample_no}.dedup.sam".format(sample_no=sample_no)
    handle_file = "{sample_no}.sam".format(sample_no=sample_no)
    bucket.get_object_to_file(remote_file, local_file)
    print "download success"
    subprocess.Popen(['samtools', 'view', local_file, '>', sam_file]).communicate()
    handle_sam(sam_file, handle_file)
    print "handle success"
    sam2table(handle_file, sample_no)
    print "insert success"
    subprocess.Popen(['rm', '-f', local_file]).communicate()
    subprocess.Popen(['rm', '-f', sam_file]).communicate()
    subprocess.Popen(['rm', '-f', handle_file]).communicate()
    print "delete success"


if __name__ == '__main__':
    account = 'geneac'
    sample_no = 3908
    # sample_no = 4
    task_id = 1
    # threading.Thread(target=handle_thread, args=(account, sample_no, task_id)).start()
    handle_file = "{sample_no}.sam".format(sample_no=sample_no)
    # describe_response = ots_client.describe_table('myTable')
    # print u'表的名称: %s' % describe_response.table_meta.table_name
    # handle_file = "C:/Users/code/Desktop/tmp.sam"
    sam2table(handle_file, sample_no)
    print "insert success"
    # sam2table(handle_file, 6)