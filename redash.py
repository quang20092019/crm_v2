from flask import Blueprint
import os
from flask import request, jsonify,send_file,send_from_directory
import json

import requests
from requests.auth import HTTPDigestAuth
import pandas as pd
from pandas import json_normalize
import pymysql
from flask_jsonpify import jsonpify
from datetime import datetime
redash = Blueprint('redash', __name__)

@redash.route('/durationvbn', methods=['GET'])
def durationvbn():
    try :
        db = pymysql.connect(host="127.0.0.1",    # your host, usually localhost
                     user="syncdbvos",         # your username
                     passwd="KJksjooii8998899",  # your password
                     db="vos_cdr",
                     port=3306
                     )
        startdate = request.args.get('startdate')
        enddate = request.args.get('enddate')
        brandname = request.args.get('brandname')
        brandname =str(brandname).replace(",","','")
        sql= "select a.id,a.customername 'Customer', a.callergatewayid 'brandname', a.feetime as Charge_Duration, a.Tin_OTP, a.Date, b.feetime'Total' from (select 'id',customername,callergatewayid,sum(feetime)/60 'feetime',sum(Tin_OTP)'Tin_OTP',left(starttime,10) 'Date'from giamsatnew_vbn where callergatewayid in ('"+brandname+"') and starttime >= '"+str(startdate)+"' AND starttime < '"+str(enddate)+"' group by callergatewayid, left(starttime, 10)) a  left join (select 'id',customername,callergatewayid,sum(feetime)/60 'feetime',sum(Tin_OTP)'Tin_OTP',left(starttime,10) 'Date' from giamsatnew_vbn where callergatewayid in ('"+brandname+"') and starttime >= '"+str(startdate)+"' AND starttime < '"+str(enddate)+"') b on a.id = b.id"
        print(sql)
        df = pd.read_sql(sql,db)
        json_records = df.to_json(orient ='records')
        data = []
        data = json.loads(json_records)
        context = {'data': data,'code': 'OK'}
        db.close()
        return context
    except :
        db.close()  
        return jsonify({'Lỗi': 'Quá nhiều lỗi'})

@redash.route('/listbrandname', methods=['GET'])
def listbrandname():
    try :
        db = pymysql.connect(host="127.0.0.1",    # your host, usually localhost
                     user="syncdbvos",         # your username
                     passwd="KJksjooii8998899",  # your password
                     db="vos_cdr",
                     port=3306
                     )
        # startdate = request.args.get('startdate')
        # enddate = request.args.get('enddate')
        # brandname = request.args.get('brandname')
        # brandname =str(brandname).replace(",","','")
        sql= "select distinct(callergatewayid) from giamsatnew_vbn"
        print(sql)
        df = pd.read_sql(sql,db)
        json_records = df.to_json(orient ='records')
        data = []
        data = json.loads(json_records)
        context = {'data': data,'code': 'OK'}
        db.close()
        return context
    except :
        db.close()  
        return jsonify({'Lỗi': 'Quá nhiều lỗi'})