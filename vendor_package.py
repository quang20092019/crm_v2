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
vendor_package = Blueprint('vendor_package', __name__)
@vendor_package.route('/listvendor_package', methods=['GET'])
def listvendor_package():
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
            name = data["user"]
            print(timetoken)
            print(group_name)
            if True:
                currentDate = time.strftime("%Y-%m-%d %H:%M:%S")
                print(currentDate)
                if str(timetoken) >= str(currentDate) :
                    sql ="SELECT a.id,a.name,a.packagecode,a.packagedetail,a.expiredtime,b.name'vendorname',a.createdtime,a.updatedtime,a.starttime,c.name'telco' FROM vendor_package a left join (SELECT id,name FROM vendor) b on a.vendorid = b.id left join (select * from telco) c on a.telcoid= c.id"
                    df = pd.read_sql(sql,db)
                    df['expiredtime'] = df['expiredtime'].astype('str')
                    df['updatedtime'] = df['updatedtime'].astype('str')
                    df['starttime'] = df['starttime'].astype('str')
                    df['createdtime'] = df['createdtime'].astype('str')
                    json_records = df.to_json(orient ='records')
                    data = []
                    data = json.loads(json_records)
                    context = {'data': data,'code': 'OK'}
                    sqllog = "insert into log (name,api,content) value ('"+str(name)+"','listvendor_package','view')"
                    cursor = db.cursor()
                    cursor.execute(sqllog)
                    db.commit()
                    db.close() 
                    return context
                else :
                    return jsonify({'NOTOK': 'token hết hạn','code':401}),401
            else :
                return jsonify({'NOTOK': 'khong co quyen'})
    except :
        db.close()  
        return jsonify({'Lỗi': 'Quá nhiều lỗi'})
@vendor_package.route('/deletevendor_package', methods=['DELETE'])
def deletevendor_package():
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
            name = data["user"]
            print(timetoken)
            print(group_name)
            if True:
                currentDate = time.strftime("%Y-%m-%d %H:%M:%S")
                print(currentDate)
                if str(timetoken) >= str(currentDate) :
                    record = json.loads(request.data)
                    id = record["id"]
                    sqlcheck= "select count(*) from vendor_package where id ='"+str(id)+"'"
                    df = pd.read_sql(sqlcheck,db)
                    sl = df.iloc[0,0]
                    if int(sl) > 0 :
                        sql="delete from vendor_package where id = '"+str(id)+"'"
                        cursor = db.cursor()
                        cursor.execute(sql)
                        sqllog = "insert into log (name,api,content) value ('"+str(name)+"','deletevendor_package','id = "+str(id)+"')"
                        cursor = db.cursor()
                        cursor.execute(sqllog)
                        db.commit()
                        return jsonify({'OK': 'OK'})
                    else :
                        return jsonify({'OK': 'id không tồn tại'})
                    db.close()
                else :
                    return jsonify({'NOTOK': 'token hết hạn','code':401}),401
            else :
                return jsonify({'NOTOK': 'khong co quyen'})
    except :
        db.close()  
        return jsonify({'Lỗi': 'Quá nhiều lỗi'})
@vendor_package.route('/insertvendor_package', methods=['POST'])
def insertvendor_package():
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
            print(timetoken)
            print(group_name)
            if True:
                currentDate = time.strftime("%Y-%m-%d %H:%M:%S")
                print(currentDate)
                if str(timetoken) >= str(currentDate) :
                    record = json.loads(request.data)
                    print(record)
                    name = record["name"]
                    packagecode = record["packagecode"]
                    packagedetail = record["packagedetail"]
                    expiredtime = record["expiredtime"]
                    starttime = record["starttime"]
                    vendorid = record["vendor_name"]
                    sqlvendorid = "select id from vendor where name ='"+str(vendorid)+"'"
                    df = pd.read_sql(sqlvendorid,db)
                    vendorid = df.iloc[0,0]
                    telco = record["telco"]
                    sqltelcoid = "select id from telco where name = '"+str(telco)+"'"
                    df = pd.read_sql(sqltelcoid,db)
                    telcoid = df.iloc[0,0]
                    now = datetime.now()
                    current_time = now.strftime("%Y-%m-%d %H:%M:%S")
                    sql="insert into vendor_package(name,packagecode,packagedetail,expiredtime,vendorid,telcoid,starttime) value ('"+str(name)+"','"+str(packagecode)+"','"+str(packagedetail)+"','"+str(expiredtime)+"','"+str(vendorid)+"','"+str(telcoid)+"','"+str(starttime)+"')"
                    cursor = db.cursor()
                    cursor.execute(sql)
                    sqllog = "insert into log (name,api,content) value ('"+str(nameuser)+"','insertvendor_package','"+str(record).replace("'","")+"')"
                    cursor = db.cursor()
                    cursor.execute(sqllog)
                    db.commit()
                    return jsonify({'OK': 'OK'})
                    db.close()
                else :
                    return jsonify({'NOTOK': 'token hết hạn','code':401}),401
            else :
                return jsonify({'NOTOK': 'khong co quyen'})    
    except:
        return jsonify({'NOTOK': 'NOTOK'})   
@vendor_package.route('/updatevendor_package', methods=['POST'])
def updatevendor_package():
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
            nameuser = data["user"]
            print(timetoken)
            print(group_name)
            if True:
                currentDate = time.strftime("%Y-%m-%d %H:%M:%S")
                print(currentDate)
                if str(timetoken) >= str(currentDate) :
                    record = json.loads(request.data)
                    id = record["id"]
                    name = record["name"]
                    packagecode = record["packagecode"]
                    packagedetail = record["packagedetail"]
                    expiredtime = record["expiredtime"]
                    starttime = record["starttime"]
                    vendorid = record["vendor_name"]
                    sqlvendorid = "select id from vendor where name ='"+str(vendorid)+"'"
                    df = pd.read_sql(sqlvendorid,db)
                    vendorid = df.iloc[0,0]
                    telco = record["telco"]
                    sqltelcoid = "select id from telco where name = '"+str(telco)+"'"
                    df = pd.read_sql(sqltelcoid,db)
                    telcoid = df.iloc[0,0]
                    now = datetime.now()
                    current_time = now.strftime("%Y-%m-%d %H:%M:%S")
                    sqlcheck= "select count(*) from vendor_package where id ='"+str(id)+"'"
                    df = pd.read_sql(sqlcheck,db)
                    sl = df.iloc[0,0]
                    if int(sl) > 0 :
                        sql="update vendor_package set name = '"+str(name)+"',packagecode = '"+str(packagecode)+"',packagedetail = '"+str(packagedetail)+"',expiredtime = '"+str(expiredtime)+"',starttime='"+str(starttime)+"',telcoid='"+str(telcoid)+"',vendorid = '"+str(vendorid)+"' where id = '"+str(id)+"'"
                        cursor = db.cursor()
                        cursor.execute(sql)
                        sqllog = "insert into log (name,api,content) value ('"+str(nameuser)+"','updatevendor_package','"+str(record).replace("'","")+"')"
                        cursor = db.cursor()
                        cursor.execute(sqllog)
                        db.commit()
                        return jsonify({'OK': 'OK'})
                    else :
                        return jsonify({'OK': 'id không tồn tại'})
                    db.close()
                else :
                    return jsonify({'NOTOK': 'token hết hạn','code':401}),401
            else :
                return jsonify({'NOTOK': 'khong co quyen'}) 
    except :
        db.close()  
        return jsonify({'Lỗi': 'Quá nhiều lỗi'})