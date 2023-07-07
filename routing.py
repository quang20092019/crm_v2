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
routing = Blueprint('', __name__)
@routing.route('/listrouting', methods=['GET'])
def listrouting():
    try :
        db = pymysql.connect(host="172.17.0.1",    # your host, usually localhost
                     user="leeoncrm",         # your username
                     passwd="41XmKsO3NBgHPwv",  # your password
                     db="leeon_crm",
                     port=3306
                     )
        sql ="SELECT a.createdtime,a.id,a.name,b.ip, c.name'telco', CASE WHEN TYPE ='0' THEN 'Callin' WHEN TYPE ='1' THEN 'Callout' END AS type FROM (SELECT * FROM routing) a LEFT JOIN (SELECT * FROM vos) b ON a.vosid = b.id LEFT JOIN (SELECT * FROM telco) c ON a.telcoid = c.id order by createdtime desc"
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
@routing.route('/insertrouting', methods=['POST'])
def insertrouting():
    try :
        db = pymysql.connect(host="172.17.0.1",    # your host, usually localhost
                     user="leeoncrm",         # your username
                     passwd="41XmKsO3NBgHPwv",  # your password
                     db="leeon_crm",
                     port=3306
                     )
        record = json.loads(request.data)
        name = record["name"]
        type= record["type"]
        ip = record["ip"]
        telco = record["telco"]
        now = datetime.now()
        current_time = now.strftime("%Y-%m-%d %H:%M:%S")
        sqlcheck= "select count(*) from routing where name ='"+str(name)+"'"
        df = pd.read_sql(sqlcheck,db)
        sl = df.iloc[0,0]
        if int(sl) > 0 :
            return jsonify({'OK': 'name đã tồn tại'})
        else :
            sqlvos= "select id from vos where ip = '"+str(ip)+"'"
            dfvos = pd.read_sql(sqlvos,db)
            vos = dfvos.iloc[0,0]
            sqltelco= "select id from telco where name = '"+str(telco)+"'"
            dftelco = pd.read_sql(sqltelco,db)
            telco = dftelco.iloc[0,0]
            if type == 'Callout' :
                sql="insert into routing(name,type,vosid,telcoid,createdtime,updatedtime) value ('"+str(name)+"','1','"+str(vos)+"','"+str(telco)+"','"+str(current_time)+"','"+str(current_time)+"')"
                cursor = db.cursor()
                cursor.execute(sql)
                db.commit()
            else :
                sql="insert into routing(name,type,vosid,telcoid,createdtime,updatedtime) value ('"+str(name)+"','0','"+str(vos)+"','"+str(telco)+"','"+str(current_time)+"','"+str(current_time)+"')"
                cursor = db.cursor()
                cursor.execute(sql)
                db.commit()
            return jsonify({'OK': 'OK'})
        db.close()     
    except :
        db.close()  
        return jsonify({'Lỗi': 'Quá nhiều lỗi'})
@routing.route('/deleterouting', methods=['DELETE'])
def detelerouting():
    try :
        db = pymysql.connect(host="172.17.0.1",    # your host, usually localhost
                     user="leeoncrm",         # your username
                     passwd="41XmKsO3NBgHPwv",  # your password
                     db="leeon_crm",
                     port=3306
                     )
        record = json.loads(request.data)
        id = record["id"]
        sqlcheck= "select count(*) from routing where id ='"+str(id)+"'"
        df = pd.read_sql(sqlcheck,db)
        sl = df.iloc[0,0]
        if int(sl) > 0 :
            sql="delete from routing where id = '"+str(id)+"'"
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
@routing.route('/updaterouting', methods=['POST'])
def updaterouting():
    try :
        db = pymysql.connect(host="172.17.0.1",    # your host, usually localhost
                     user="leeoncrm",         # your username
                     passwd="41XmKsO3NBgHPwv",  # your password
                     db="leeon_crm",
                     port=3306
                     )
    
        record = json.loads(request.data)
        id = record["id"]
        name = record["name"]
        type= record["type"]
        ip = record["ip"]
        telco = record["telco"]
        now = datetime.now()
        current_time = now.strftime("%Y-%m-%d %H:%M:%S")
        sqlcheck= "select count(*) from routing where id ='"+str(id)+"'"
        df = pd.read_sql(sqlcheck,db)
        sl = df.iloc[0,0]
        if int(sl) > 0 :
            sqlvos= "select id from vos where ip = '"+str(ip)+"'"
            dfvos = pd.read_sql(sqlvos,db)
            vos = dfvos.iloc[0,0]
            sqltelco= "select id from telco where name = '"+str(telco)+"'"
            dftelco = pd.read_sql(sqltelco,db)
            telco = dftelco.iloc[0,0]
            if type == 'Callout' :
                sql="update routing set name = '"+str(name)+"',type = '1',vosid = '"+str(vos)+"',telcoid = '"+str(telco)+"',updatedtime='"+str(current_time)+"' where id ='"+str(id)+"'"
                cursor = db.cursor()
                cursor.execute(sql)
                db.commit()
            else :
                sql="update routing set name = '"+str(name)+"',type = '0',vosid = '"+str(vos)+"',telcoid = '"+str(telco)+"',updatedtime='"+str(current_time)+"' where id ='"+str(id)+"'"
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