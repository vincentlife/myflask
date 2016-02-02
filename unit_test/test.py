# -*- coding: utf-8 -*-
# Create Date 2015/12/9
__author__ = 'wubo'
import requests
import json
import re
# if __name__ == "__main__":
#     print requests.get("http://127.0.0.1:3534/hello/").text
#     request_data = json.dumps({"range":(19254500,19255700),"sample_no": 7, "chr":13})
#     response = requests.get("http://127.0.0.1:3534/api/pgb/", data=request_data)
#     print response.text
if __name__ == '__main__':
    outfile = open(r"C:\Users\guhongjie\Desktop\result_variant_function.xls","w")
    with open(r"C:\Users\guhongjie\Desktop\variant_function.xls") as infile:
        for line in infile:
            item_list = line.strip("\n").split('\t')
            s = item_list[1]

            if '(' in s:
                units = s.split(',')
                MIN= 100000000000000000000
                temp = ""
                for unit in units:
                    l = re.findall(r"[+-]\d+[ATCG]", unit)
                    if len(l) != 0:
                        numstr = l[0]
                        if int(numstr[1:-1]) < MIN:
                            MIN = int(numstr[1:-1])
                            temp = unit
                item_list[1] = temp
            else:
                item_list[1] = ""
            outfile.write("\t".join(item_list)+"\n")