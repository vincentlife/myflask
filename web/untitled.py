# coding: utf-8
from flask import Flask, render_template
from tools.Mysql_db import DB
import json
app = Flask(__name__)
db = DB()


@app.route('/')
def hello_world():
    sel_SQL = "SELECT disease_name,text FROM tmp LIMIT 1;"
    db.execute(sel_SQL)
    pam = {}
    res = db.fetchone()
    pam["en_text"] = res[0]
    pam["zh_text"] = res[1]
    return render_template("hello.html", dic=pam)


@app.route('/api/')
def rev_text():
    return json.dumps({"se":"fe"})

if __name__ == '__main__':
    app.run()
