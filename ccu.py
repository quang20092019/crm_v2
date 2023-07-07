from flask import Blueprint
from flask import request, jsonify
import json

import requests
from requests.auth import HTTPDigestAuth
import pandas as pd
from pandas import json_normalize
import pymysql
from flask_jsonpify import jsonpify
from datetime import datetime,date, timedelta
import datetime
import logging
session=datetime.datetime.now()
ccu = Blueprint('ccu', __name__)
@ccu.route('/totalccu', methods=['GET'])
def totalccu():
    logging.info(str(session) +"|totalccu")
    try :
        db = pymysql.connect(host="172.17.0.1",    # your host, usually localhost
                     user="leeoncrm",         # your username
                     passwd="41XmKsO3NBgHPwv",  # your password
                     db="leeon_crm",
                     port=3306
                     )
        sql ="select a.ip,b.ccu_mapping'ccu' from  vos a join (select sum(ccu)'ccu_mapping',vosid from mapping group by vosid) b on a.id = b.vosid"
        df = pd.read_sql(sql,db)
        json_records = df.to_json(orient ='records')
        data = []
        data = json.loads(json_records)
        context = {'data': data,'code': 'OK'}
        db.close()
        logging.info(str(session) +"|response|Thành công")
        return context
    except Exception as e:
        db.close()
        logging.error(str(session) +"|response|" +str(e))
        return jsonify({'NOTOK': str(e)}),400
@ccu.route('/timeline_ccu', methods=['GET'])
def timeline_ccu():
    logging.info(str(session) +"|timeline_ccu")
    try :
        db = pymysql.connect(host="172.17.0.1",    # your host, usually localhost
                     user="leeoncrm",         # your username
                     passwd="41XmKsO3NBgHPwv",  # your password
                     db="leeon_crm",
                     port=3306
                     )
        starttime = request.args.get('starttime')
        print(starttime)
        endtime = request.args.get('endtime')
        print(endtime)
        if str(starttime) ==str(endtime) :
            endtime = str(endtime)+" 23:59:59"
        sql ="select b.ip,sum(ccu)'ccu',a.createdtime,a.id from mapping_ccu a, vos b where a.vosid=b.id and a.createdtime >= CURDATE() group by a.createdtime,b.ip"
        logging.info(str(session) +"|query|"+str(sql))
        df = pd.read_sql(sql,db)
        df['createdtime'] = df['createdtime'].astype('str')
        json_records = df.to_json(orient ='records')
        data = []
        data = json.loads(json_records)
        context = {'data': data,'code': 'OK'}
        db.close()
        logging.info(str(session) +"|response|Thành công")
        return context
    except Exception as e:
        db.close()
        logging.error(str(session) +"|response|" +str(e))
        return jsonify({'NOTOK': str(e)}),400
@ccu.route('/timeline_ccu_customer', methods=['GET'])
def timeline_ccu_customer():
    now = datetime.datetime.now()
    session = str(now).replace(" ","_").replace(":","_").replace(".","_").replace("-","_")
    logging.info(str(session) +"|timeline_ccu_customer")
    try :
        db = pymysql.connect(host="172.17.0.1",    # your host, usually localhost
                     user="leeoncrm",         # your username
                     passwd="41XmKsO3NBgHPwv",  # your password
                     db="leeon_crm",
                     port=3306
                     )
        nickname = request.args.get('nickname')
        vos = request.args.get('vos')
        sql_vos = "select id from vos where name = '"+str(vos)+"' limit 1"
        df_vos = pd.read_sql(sql_vos,db)
        # if df_vos.empty:
        #     sql = "select '"+str(nickname)+"' `nickname`,sum(ccu)'ccu',createdtime from mapping_ccu WHERE name IN (SELECT name FROM mapping WHERE customer_id IN( SELECT customer_id FROM ACCOUNT WHERE name = '"+str(nickname)+"' AND type = 0)) and createdtime >= CURDATE() group by createdtime"
        # else :
        vos_id = df_vos.iloc[0]['id']
        sql = "select '"+str(nickname)+"' `nickname`,sum(ccu)'ccu',createdtime from mapping_ccu WHERE createdtime >= CURDATE() and customer_id IN (SELECT customer_id FROM account WHERE name ='"+str(nickname)+"') group by createdtime"
        logging.info(session + "| sql | " +sql)
        df = pd.read_sql(sql,db)
        df['createdtime'] = df['createdtime'].astype('str')
        json_records = df.to_json(orient ='records')
        data = []
        data = json.loads(json_records)
        context = {'data': data,'code': 'OK'}
        db.close()
        logging.info(str(session) +"|response|Thành công")
        return context
    except Exception as e:
        db.close()
        logging.error(str(session) +"|response|" +str(e))
        return jsonify({'NOTOK': str(e)}),400

@ccu.route('/get_nickname_from_vosid', methods=['GET'])
def get_nickname_from_vosid():
    logging.info(str(session) +"|timeline_ccu_customer")
    try :
        db = pymysql.connect(host="172.17.0.1",    # your host, usually localhost
                     user="leeoncrm",         # your username
                     passwd="41XmKsO3NBgHPwv",  # your password
                     db="leeon_crm",
                     port=3306
                     )
        vos = request.args.get('vos')
        sql = "SELECT distinct(name)'NAME' FROM ACCOUNT WHERE vosid = (SELECT id FROM vos WHERE NAME = '"+str(vos)+"')"
        df = pd.read_sql(sql,db)
        json_records = df.to_json(orient ='records')
        data = []
        data = json.loads(json_records)
        context = {'data': data,'code': 'OK'}
        db.close()
        logging.info(str(session) +"|response|Thành công")
        return context
    except Exception as e:
        db.close()
        logging.error(str(session) +"|response|" +str(e))
        return jsonify({'NOTOK': str(e)}),400
