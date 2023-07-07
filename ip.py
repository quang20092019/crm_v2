import base64
import json
import time
from flask import Blueprint
from flask import request, jsonify
import json
import requests
from requests.auth import HTTPDigestAuth
import pandas as pd
from pandas import json_normalize
import pymysql
from flask_jsonpify import jsonpify
from function import group
import logging
from datetime import datetime,date, timedelta
ip = Blueprint('ip', __name__)
@ip.route('/listip', methods=['GET'])
def listip():
    session=datetime.now()
    logging.info(str(session) +"|listip")
    try:
        db = pymysql.connect(host="172.17.0.1",    # your host, usually localhost
                     user="leeoncrm",         # your username
                     passwd="41XmKsO3NBgHPwv",  # your password
                     db="leeon_crm",
                     port=3306
                     )
        data = group()
        if str(data)== "chua truyen token" :
            return jsonify({'NOTOK': str(data)})
        else:
            timetoken = data["expireddate"]
            group_name = data["group_name"]
            nameuser = data["user"]
            logging.info(str(session) +"|listaccount|"+str(data))
            if True:
                currentDate = time.strftime("%Y-%m-%d %H:%M:%S")
                print(currentDate)
                if str(timetoken) >= str(currentDate) :
                    sql ="SELECT a.id, a.ip ,a.port, b.nickname , CASE WHEN priority = '1' THEN 'Chính' WHEN priority = '0' THEN 'Phụ' END AS priority FROM (SELECT * FROM ip) a LEFT JOIN (SELECT * FROM partner) b ON a.partnerid = b.id"
                    df = pd.read_sql(sql,db)
                    json_records = df.to_json(orient ='records')
                    data = []
                    data = json.loads(json_records)
                    context = {'data': data,'code': 'OK'}
                    db.close()
                    logging.info(str(session) +"|response|Thành công")
                    return context
                else :
                    db.close()
                    logging.error(str(session) +"|response|token hết hạn")
                    return jsonify({'NOTOK': 'token hết hạn','code':401}),401
            else :
                db.close()
                logging.error(str(session) +"|response|Không có quyền")
                return jsonify({'NOTOK': 'khong co quyen'}),400
    except Exception as e:
        db.close()
        logging.error(str(session) +"|response|" +str(e))
        return jsonify({'NOTOK': str(e)}),400
@ip.route('/inserip', methods=['POST'])
def insertip():
    try :
        db = pymysql.connect(host="172.17.0.1",    # your host, usually localhost
                     user="leeoncrm",         # your username
                     passwd="41XmKsO3NBgHPwv",  # your password
                     db="leeon_crm",
                     port=3306
                     )
        record = json.loads(request.data)
        ip = record["ip"]
        port= record["port"]
        nickname = record["nickname"]
        priority = record["priority"]
        now = datetime.now()
        current_time = now.strftime("%Y-%m-%d %H:%M:%S")
        sqlpartner= "select id from partner where nickname = '"+str(nickname)+"'"
        dfpartner = pd.read_sql(sqlpartner,db)
        partner = dfpartner.iloc[0,0]
        if priority == 'Chính' :
                sql="insert into ip(ip,port,priority,partnerid,createdtime,updatedtime) value ('"+str(ip)+"','"+str(port)+"','1','"+str(partner)+"','"+str(current_time)+"','"+str(current_time)+"')"
                cursor = db.cursor()
                cursor.execute(sql)
                db.commit()
        else :
                sql="insert into ip(ip,port,priority,partnerid,createdtime,updatedtime) value ('"+str(ip)+"','"+str(port)+"','0','"+str(partner)+"','"+str(current_time)+"','"+str(current_time)+"')"
                cursor = db.cursor()
                cursor.execute(sql)
                db.commit()
        return jsonify({'OK': 'OK'})
        db.close()     
    except :
        db.close()  
        return jsonify({'Lỗi': 'Quá nhiều lỗi'})
@ip.route('/deteleip', methods=['DELETE'])
def deteleip():
    try :
        db = pymysql.connect(host="172.17.0.1",    # your host, usually localhost
                     user="leeoncrm",         # your username
                     passwd="41XmKsO3NBgHPwv",  # your password
                     db="leeon_crm",
                     port=3306
                     )
        record = json.loads(request.data)
        id = record["id"]
        sqlcheck= "select count(*) from ip where id ='"+str(id)+"'"
        df = pd.read_sql(sqlcheck,db)
        sl = df.iloc[0,0]
        if int(sl) > 0 :
            sql="delete from ip where id = '"+str(id)+"'"
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
@ip.route('/updateip', methods=['POST'])
def updateip():
    try :
        db = pymysql.connect(host="172.17.0.1",    # your host, usually localhost
                     user="leeoncrm",         # your username
                     passwd="41XmKsO3NBgHPwv",  # your password
                     db="leeon_crm",
                     port=3306
                     )
    
        record = json.loads(request.data)
        id = record["id"]
        ip = record["ip"]
        port= record["port"]
        nickname = record["nickname"]
        priority = record["priority"]
        now = datetime.now()
        current_time = now.strftime("%Y-%m-%d %H:%M:%S")
        sqlcheck= "select count(*) from ip where id ='"+str(id)+"'"
        df = pd.read_sql(sqlcheck,db)
        sl = df.iloc[0,0]
        if int(sl) > 0 :
            sqlpartner= "select id from partner where nickname = '"+str(nickname)+"'"
            dfpartner = pd.read_sql(sqlpartner,db)
            partner = dfpartner.iloc[0,0]
            if priority == 'Chính' :
                sql="update ip set ip = '"+str(ip)+"',priority = '1',port = '"+str(port)+"',partnerid = '"+str(partner)+"',updatedtime='"+str(current_time)+"' where id ='"+str(id)+"'"
                cursor = db.cursor()
                cursor.execute(sql)
                db.commit()
            else :
                sql="update ip set ip = '"+str(ip)+"',priority = '0',port = '"+str(port)+"',partnerid = '"+str(partner)+"',updatedtime='"+str(current_time)+"' where id ='"+str(id)+"'"
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