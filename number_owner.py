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
from function import group
import time
number_owner = Blueprint('number_owner', __name__)
@number_owner.route('/listnumber_owner', methods=['GET'])
def listnumber_owner():
    try :
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
            print(timetoken)
            print(group_name)
            if True:
                currentDate = time.strftime("%Y-%m-%d %H:%M:%S")
                print(currentDate)
                if str(timetoken) >= str(currentDate) :
                    sql ="SELECT a.id,a.isdn,a.limits,b.name'vosip',c.name'telco' FROM leeon_crm.number_owner a left join (select * from vos) b on a.vosip= b.id left join (select * from telco) c on a.telcoid= c.id"
                    df = pd.read_sql(sql,db)
                    # df['limits'] = df['limits'].astype('int')
                    # df['limits'] = df['limits'].apply('{:,}'.format)
                    json_records = df.to_json(orient ='records')
                    data = []
                    data = json.loads(json_records)
                    context = {'data': data,'code': 'OK'}
                    db.close() 
                    return context
                else :
                    return jsonify({'NOTOK': 'token hết hạn'})
            else :
                return jsonify({'NOTOK': 'khong co quyen'})
    except :
        db.close()  
        return jsonify({'Lỗi': 'Quá nhiều lỗi'})
@number_owner.route('/deletenumber_owner', methods=['DELETE'])
def deletenumber_owner():
    try :
        db = pymysql.connect(host="172.17.0.1",    # your host, usually localhost
                     user="leeoncrm",         # your username
                     passwd="41XmKsO3NBgHPwv",  # your password
                     db="leeon_crm",
                     port=3306
                     )
        record = json.loads(request.data)
        id = record["id"]
        sqlcheck= "select count(*) from number_owner where id ='"+str(id)+"'"
        df = pd.read_sql(sqlcheck,db)
        sl = df.iloc[0,0]
        if int(sl) > 0 :
            sql="delete from number_owner where id = '"+str(id)+"'"
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
@number_owner.route('/insertnumber_owner', methods=['POST'])
def insertnumber_owner():
    # try :
        db = pymysql.connect(host="172.17.0.1",    # your host, usually localhost
                     user="leeoncrm",         # your username
                     passwd="41XmKsO3NBgHPwv",  # your password
                     db="leeon_crm",
                     port=3306
                     )
        record = json.loads(request.data)
        isdn = record["isdn"]
        limits = record["limits"]
        vosip = record["vosip"]
        sqlvos = "select id from vos where name = '"+str(vosip)+"'"
        print(sqlvos)
        df = pd.read_sql(sqlvos,db)
        vosip = df.iloc[0,0]
        telco = record["telco"]
        sqltelcoid = "select id from telco where name = '"+str(telco)+"'"
        df = pd.read_sql(sqltelcoid,db)
        telcoid = df.iloc[0,0]
        now = datetime.now()
        current_time = now.strftime("%Y-%m-%d %H:%M:%S")
        sqlcheck= "select count(*) from number_owner where isdn ='"+str(isdn)+"'"
        df = pd.read_sql(sqlcheck,db)
        sl = df.iloc[0,0]
        if int(sl) > 0 :
            return jsonify({'OK': 'isdn đã tồn tại'})
        else :
            sql="insert into number_owner(isdn,limits,vosip,telcoid,createdtime,updatedtime) value ('"+str(isdn)+"','"+str(limits)+"','"+str(vosip)+"','"+str(telcoid)+"','"+str(current_time)+"','"+str(current_time)+"')"
            cursor = db.cursor()
            cursor.execute(sql)
            db.commit()
            return jsonify({'OK': 'OK'})
        db.close()     
    # except :
    #     db.close()  
    #     return jsonify({'Lỗi': 'Quá nhiều lỗi'})
@number_owner.route('/updatenumber_owner', methods=['POST'])
def updatenumber_owner():

        db = pymysql.connect(host="172.17.0.1",    # your host, usually localhost
                     user="leeoncrm",         # your username
                     passwd="41XmKsO3NBgHPwv",  # your password
                     db="leeon_crm",
                     port=3306
                     )
        record = json.loads(request.data)
        id = record["id"]
        isdn = record["isdn"]
        limits = record["limits"]
        vosip = record["vosip"]
        sqlvos = "select id from vos where name = '"+str(vosip)+"'"
        df = pd.read_sql(sqlvos,db)
        vosip = df.iloc[0,0]
        telco = record["telco"]
        sqltelcoid = "select id from telco where name = '"+str(telco)+"'"
        df = pd.read_sql(sqltelcoid,db)
        telcoid = df.iloc[0,0]
        now = datetime.now()
        current_time = now.strftime("%Y-%m-%d %H:%M:%S")
        sqlcheck= "select count(*) from number_owner where id ='"+str(id)+"'"
        df = pd.read_sql(sqlcheck,db)
        sl = df.iloc[0,0]
        if int(sl) > 0 :
            sql="update number_owner set isdn = '"+str(isdn)+"',limits = '"+str(limits)+"',vosip = '"+str(vosip)+"',telcoid='"+str(telcoid)+"',updatedtime='"+str(current_time)+"' where id ='"+str(id)+"'"
            cursor = db.cursor()
            cursor.execute(sql)
            db.commit()
            return jsonify({'OK': 'OK'})
        else :
            return jsonify({'OK': 'id không tồn tại'})
        db.close() 
@number_owner.route('/listnumber_ownerisdn', methods=['GET'])
def listnumber_ownerisdn():
    try :
        db = pymysql.connect(host="172.17.0.1",    # your host, usually localhost
                     user="leeoncrm",         # your username
                     passwd="41XmKsO3NBgHPwv",  # your password
                     db="leeon_crm",
                     port=3306
                     )
        sql ="SELECT id,isdn'numberowner' FROM number_owner"
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
