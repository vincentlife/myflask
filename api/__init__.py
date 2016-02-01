# -*- coding: utf-8 -*-
# Create Date 2015/12/15
__author__ = 'wubo'
from flask import Flask, request

app = Flask(__name__)

@app.route('/hello/', methods=["GET"])
def hello():
    return "HELLO"

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=3534)

