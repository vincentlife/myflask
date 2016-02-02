# -*- coding: utf-8 -*-
# Create Date 2015/12/15
__author__ = 'wubo'
from flask import Flask, request

app = Flask(__name__)

from api.api_pgb import pgb_api
app.register_blueprint(pgb_api, url_prefix='/api')

@app.route('/hello/', methods=["GET"])
def hello():
    return "HELLO"

