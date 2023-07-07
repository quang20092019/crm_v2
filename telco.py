from flask import Blueprint
from flask import request, jsonify
import json

import requests
from requests.auth import HTTPDigestAuth
import pandas as pd
from pandas import json_normalize
import pymysql
from flask_jsonpify import jsonpify
from datetime import datetime
import datetime
import logging
telco = Blueprint('telco', __name__)
@telco.route('/listtelco', methods=['GET'])
def listtelco():
    try :
        db = pymysql.connect(host="172.17.0.1",    # your host, usually localhost
                     user="leeoncrm",         # your username
                     passwd="41XmKsO3NBgHPwv",  # your password
                     db="leeon_crm",
                     port=3306
                     )
        logging.info("test")
        sql ="SELECT createdtime,id, name FROM telco order by createdtime desc"
        df = pd.read_sql(sql,db)
        df['createdtime'] = df['createdtime'].astype('str')
        json_records = df.to_json(orient ='records')
        data = []
        data = json.loads(json_records)
        context = {'data': data,'code': 'OK'}
        db.close() 
        return context
    except :
        db.close()  
        return jsonify({'Lỗi': 'Quá nhiều lỗi'})
@telco.route('/listnhamang', methods=['GET'])
def listnhamang():
    try :
        db = pymysql.connect(host="172.17.0.1",    # your host, usually localhost
                     user="leeoncrm",         # your username
                     passwd="41XmKsO3NBgHPwv",  # your password
                     db="leeon_crm",
                     port=3306
                     )
        logging.info("test")
        sql ="SELECT id,telcoName'name' FROM service_prefix where status =1"
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
@telco.route('/insertelco', methods=['POST'])
def inserttelco():
    try :
        db = pymysql.connect(host="172.17.0.1",    # your host, usually localhost
                     user="leeoncrm",         # your username
                     passwd="41XmKsO3NBgHPwv",  # your password
                     db="leeon_crm",
                     port=3306
                     )
        record = json.loads(request.data)
        telco = record["telco"]
        now = datetime.now()
        current_time = now.strftime("%Y-%m-%d %H:%M:%S")
        sqlcheck= "select count(*) from telco where name ='"+str(telco)+"'"
        df = pd.read_sql(sqlcheck,db)
        sl = df.iloc[0,0]
        if int(sl) > 0 :
            return jsonify({'OK': 'telco đã tồn tại'})
        else :
            sql="insert into telco(name,createdtime,updatedtime) value ('"+str(telco)+"','"+str(current_time)+"','"+str(current_time)+"')"
            cursor = db.cursor()
            cursor.execute(sql)
            db.commit()
            return jsonify({'OK': 'OK'})
        db.close()     
    except :
        db.close()  
        return jsonify({'Lỗi': 'Quá nhiều lỗi'})
@telco.route('/deletetelco', methods=['DELETE'])
def deteletelco():
    try :
        db = pymysql.connect(host="172.17.0.1",    # your host, usually localhost
                     user="leeoncrm",         # your username
                     passwd="41XmKsO3NBgHPwv",  # your password
                     db="leeon_crm",
                     port=3306
                     )
        record = json.loads(request.data)
        id = record["id"]
        sqlcheck= "select count(*) from telco where id ='"+str(id)+"'"
        df = pd.read_sql(sqlcheck,db)
        sl = df.iloc[0,0]
        if int(sl) > 0 :
            sql="delete from telco where id = '"+str(id)+"'"
            cursor = db.cursor()
            cursor.execute(sql)
            db.commit()
            return jsonify({'OK': 'OK'})
        else :
            return jsonify({'OK': 'id không tồn tại'})
        db.close()     
    except :
        db.close()  
        return jsonify({'Lỗi': 'Quá nhiều lỗi'})
@telco.route('/updatetelco', methods=['POST'])
def updatetelco():
    try :
        db = pymysql.connect(host="172.17.0.1",    # your host, usually localhost
                     user="leeoncrm",         # your username
                     passwd="41XmKsO3NBgHPwv",  # your password
                     db="leeon_crm",
                     port=3306
                     )
        record = json.loads(request.data)
        id = record["id"]
        telco = record["telco"]
        now = datetime.now()
        current_time = now.strftime("%Y-%m-%d %H:%M:%S")
        sqlcheck= "select count(*) from telco where id ='"+str(id)+"'"
        df = pd.read_sql(sqlcheck,db)
        sl = df.iloc[0,0]
        if int(sl) > 0 :
            sql="update telco set name = '"+str(telco)+"',updatedtime='"+str(current_time)+"' where id ='"+str(id)+"'"
            cursor = db.cursor()
            cursor.execute(sql)
            db.commit()
            return jsonify({'OK': 'OK'})
        else :
            return jsonify({'OK': 'id không tồn tại'})
        db.close()     
    except :
        db.close()  
        return jsonify({'Lỗi': 'Quá nhiều lỗi'})