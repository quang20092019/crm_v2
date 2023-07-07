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
vendor_contact = Blueprint('vendor_contact', __name__)
@vendor_contact.route('/listvendor_contact', methods=['GET'])
def listvendor_contact():
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
                    sql ="SELECT a.id,a.name,b.name'vendor_name',a.department,a.address,a.phone,a.email,a.bank,a.bankaccount,a.bankbranch FROM vendor_contact a left join (SELECT * FROM vendor) b on a.vendorid = b.id"
                    df = pd.read_sql(sql,db)
                    json_records = df.to_json(orient ='records')
                    data = []
                    data = json.loads(json_records)
                    context = {'data': data,'code': 'OK'}
                    sqllog = "insert into log (name,api,content,type) value ('"+str(nameuser)+"','listvendor_contact','view','SELECT')"
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
        return jsonify({'NOTOK': 'NOTOK'})
@vendor_contact.route('/deletevendor_contact', methods=['DELETE'])
def deletevendor_contact():
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
                    sqlcheck= "select count(*) from vendor_contact where id ='"+str(id)+"'"
                    df = pd.read_sql(sqlcheck,db)
                    sl = df.iloc[0,0]
                    if int(sl) > 0 :
                        sql="delete from vendor_contact where id = '"+str(id)+"'"
                        cursor = db.cursor()
                        cursor.execute(sql)
                        sqllog = "insert into log (name,api,content,type) value ('"+str(nameuser)+"','deletevendor_contact','id = "+str(id)+"','DELETE')"
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
@vendor_contact.route('/insertvendor_contact', methods=['POST'])
def insertvendor_contact():
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
                    vendorid = record["vendor_name"]
                    sqlvendorid="select id from vendor where name = '"+str(vendorid)+"'"
                    df = pd.read_sql(sqlvendorid,db)
                    vendorid = df.iloc[0,0]
                    department = record["department"]
                    address = record["address"]
                    phone = record["phone"]
                    email = record["email"]
                    bank = record["bank"]
                    bankaccount = record["bankaccount"]
                    bankbranch = record["bankbranch"]
                    now = datetime.now()
                    current_time = now.strftime("%Y-%m-%d %H:%M:%S")
                    sqlcheck= "select count(*) from vendor_contact where email ='"+str(email)+"'"
                    df = pd.read_sql(sqlcheck,db)
                    sl = df.iloc[0,0]
                    if int(sl) > 0 :
                        return jsonify({'OK': 'email đã tồn tại'})
                    else :
                        sql="insert into vendor_contact(name,vendorid,department,address,phone,email,bank,bankaccount,bankbranch,createdtime,updatedtime) value ('"+str(name)+"','"+str(vendorid)+"','"+str(department)+"','"+str(address)+"','"+str(phone)+"','"+str(email)+"','"+str(bank)+"','"+str(bankaccount)+"','"+str(bankbranch)+"','"+str(current_time)+"','"+str(current_time)+"')"
                        cursor = db.cursor()
                        cursor.execute(sql)
                        sqllog = "insert into log (name,api,content,type) value ('"+str(nameuser)+"','insertvendor_contact','"+str(record).replace("'","")+"','INSERT')"
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
@vendor_contact.route('/updatevendor_contact', methods=['POST'])
def updatevendor_contact():
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
                    id = record["id"]
                    name = record["name"]
                    department = record["department"]
                    address = record["address"]
                    phone = record["phone"]
                    email = record["email"]
                    bank = record["bank"]
                    bankaccount = record["bankaccount"]
                    bankbranch = record["bankbranch"]
                    now = datetime.now()
                    current_time = now.strftime("%Y-%m-%d %H:%M:%S")
                    sqlcheck= "select count(*) from vendor_contact where id ='"+str(id)+"'"
                    df = pd.read_sql(sqlcheck,db)
                    sl = df.iloc[0,0]
                    if int(sl) > 0 :
                        sql="update vendor_contact set name = '"+str(name)+"',department = '"+str(department)+"',address = '"+str(address)+"',phone = '"+str(phone)+"',email = '"+str(email)+"',bank = '"+str(bank)+"',bankaccount = '"+str(bankaccount)+"',bankbranch = '"+str(bankbranch)+"',updatedtime='"+str(current_time)+"' where id ='"+str(id)+"'"
                        cursor = db.cursor()
                        cursor.execute(sql)
                        sqllog = "insert into log (name,api,content) value ('"+str(nameuser)+"','updatevendor_contact','"+str(record).replace("'","")+"')"
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
