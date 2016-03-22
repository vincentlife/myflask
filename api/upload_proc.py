# -*- coding: utf-8 -*-
# !/usr/bin/python
# Create Date 2016/3/15 0015
__author__ = 'wubo'

from jy_ots import OTSClient
import sys,requests,json
OTS_ENDPOINT = "http://samplechr.cn-beijing.ots.aliyuncs.com/"
ACCESSID = "QFmrMPB18qNx9KYc"
ACCESSKEY = "IuAdh4qL9noDf0UnMOO977HSgZSc0E"
INSTANCENAME = "samplechr"
TABLE_NAME = "sample_chr_info"
REF_TABLE = "ref_seq"

gather_url = "http://127.0.0.1:7000/pgb/gather"


def upload_pb(read_file, sample_no, checksum, pno):
    ots_client = OTSClient(OTS_ENDPOINT, ACCESSID, ACCESSKEY, INSTANCENAME)
    # 定位到开始位置
    read_file = open(read_file, "rb")
    read_num = 1000000
    left_content = ""
    request_freq = 0
    content = read_file.read(read_num)
    while len(content) > 5:
        lines = (left_content + content).split("\r\n")
        for index in range(len(lines) - 1):
            line = lines[index]
            if len(line) <= 2:
                continue
            try:
                # batch_write_response, decode_time = ots_client.batch_write_row(line)
                ots_client.batch_write_row(line)
                request_freq += 1
            except Exception as e:
                print e.message
                print("error_________________________________")
                print(index)
                print(request_freq)
        left_content = lines[-1]
        content = read_file.read(read_num)
    requests.get(gather_url+"/%d/" % sample_no ,data=json.dumps({"checksum":checksum,"pno":pno,"request_freq":request_freq}))

if __name__ == "__main__":
    filepath = sys.argv[1]
    sample_no = sys.argv[2]
    checksum = sys.argv[3]
    pno = sys.argv[4]
    # filepath = r"C:\Users\guhongjie\Desktop\pgb1.pb"
    # sample_no = 3908
    # checksum = 3
    # pno = 2
    upload_pb(filepath,sample_no,checksum,pno)