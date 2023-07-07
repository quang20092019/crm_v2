from flask import Blueprint,send_from_directory,send_file
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
import os
from function import group
import time
import logging
session=datetime.datetime.now()
contact = Blueprint('contact', __name__)
@contact.route('/listcontact', methods=['GET'])
def listcontact():
    logging.info(str(session) +"|listcontact")
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
            logging.info(str(session) +"|listcontact|"+str(data))
            if True:
                currentDate = time.strftime("%Y-%m-%d %H:%M:%S")
                print(currentDate)
                if str(timetoken) >= str(currentDate) :
                    sql ="SELECT a.id,a.name,a.email,a.phone,case when position = 1 then 'Sale' when position = 2 then 'Kỹ thuật' else 'Giám Đốc' END AS position ,b.nickname'partner' FROM contact a left join partner b on a.partnerid =b.id"
                    df = pd.read_sql(sql,db)
                    json_records = df.to_json(orient ='records')
                    data = []
                    data = json.loads(json_records)
                    context = {'data': data,'code': 'OK'}
                    sqllog = "insert into log (name,api,content,type) value ('"+str(nameuser)+"','listcontact','view','SELECT')"
                    cursor = db.cursor()
                    cursor.execute(sqllog)
                    db.commit()
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
@contact.route('/deletecontact', methods=['DELETE'])
def deletecontact():
    logging.info(str(session) +"|deletecontact")
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
            logging.info(str(session) +"|deteleaccount|"+str(data))
            if True:
                currentDate = time.strftime("%Y-%m-%d %H:%M:%S")
                print(currentDate)
                if str(timetoken) >= str(currentDate) :
                    record = json.loads(request.data)
                    logging.info(str(session) +"|input|"+str(request.data))
                    id = record["id"]
                    sqlcontent= "select * from contact where id = '"+str(id)+"'"
                    dfcontent = pd.read_sql(sqlcontent,db)
                    if not dfcontent.empty:
                        dfcontent=dfcontent.values.tolist()
                        dfcontent=str(dfcontent).replace("'","")
                        logging.info(str(session) +"|content|"+str(dfcontent))
                        sql="delete from contact where id = '"+str(id)+"'"
                        logging.info(str(session) +"|query|"+str(sql))
                        cursor = db.cursor()
                        cursor.execute(sql)
                        sqllog = "insert into log (name,api,content,type) value ('"+str(nameuser)+"','deletecontact','"+str(dfcontent)+"','DELETE')"
                        cursor = db.cursor()
                        cursor.execute(sqllog)
                        db.commit()
                        db.close()
                        logging.info(str(session) +"|response | Thành công")
                        return jsonify({'OK': 'OK'})
                    else :
                        db.commit()
                        db.close()
                        logging.error(str(session) +"|response|id không tồn tại")
                        return jsonify({'OK': 'id không tồn tại'}),400
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
@contact.route('/insertcontact', methods=['POST'])
def insertcontact():
    logging.info(str(session) +"|insertcontact")
    try:
        db = pymysql.connect(host="172.17.0.1",    # your host, usually localhost
                     user="leeoncrm",         # your username
                     passwd="41XmKsO3NBgHPwv",  # your password
                     db="leeon_crm",
                     port=3306
                     )
        cursor = db.cursor()
        data = group()
        if str(data)== "chua truyen token" :
            return jsonify({'NOTOK': str(data)})
        else:
            timetoken = data["expireddate"]
            group_name = data["group_name"]
            nameuser = data["user"]
            logging.info(str(session) +"|deteleaccount|"+str(data))
            if True:
                currentDate = time.strftime("%Y-%m-%d %H:%M:%S")
                print(currentDate)
                if str(timetoken) >= str(currentDate) :
                    record = json.loads(request.data)
                    logging.info(str(session) +"|input|"+str(request.data))
                    name = record["name"]
                    email = record["email"]
                    phone = record["phone"]
                    position = record["position"]
                    partner = record["partner"]
                    # lấy thông tin partner
                    sqlpartner = "select id from partner where nickname = '"+str(partner)+"'"
                    logging.info(str(session) +"|query_getpartner|"+str(sqlpartner))
                    df = pd.read_sql(sqlpartner,db)
                    if df.empty:
                        db.close()
                        logging.error(str(session) +"|response|partner không tồn tại")
                        return jsonify({'NOTOK': 'partner khong ton tai'}),400
                    partnerid = df.iloc[0,0]
                    now = datetime.datetime.now()
                    current_time = now.strftime("%Y-%m-%d %H:%M:%S")
                    sql="insert into contact(name,email,phone,position,partnerid) value ('"+str(name)+"','"+str(email)+"','"+str(phone)+"','"+str(position)+"','"+str(partnerid)+"')"
                    logging.info(str(session) +"|query |"+ str(sql))
                    cursor.execute(sql)
                    sqllog = "insert into log (name,api,content,type) value ('"+str(nameuser)+"','insertcontact','"+str(record).replace("'","")+"','INSERT')"
                    cursor.execute(sqllog)
                    db.commit()
                    logging.info(str(session) +"|response | Thành công")
                    return jsonify({'OK': 'OK'})
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
@contact.route('/updatecontact', methods=['POST'])
def updatecontact():
    logging.info(str(session) +"|updatecontact")
    try:
        db = pymysql.connect(host="172.17.0.1",    # your host, usually localhost
                     user="leeoncrm",         # your username
                     passwd="41XmKsO3NBgHPwv",  # your password
                     db="leeon_crm",
                     port=3306
                     )
        cursor = db.cursor()
        data = group()
        if str(data)== "chua truyen token" :
            return jsonify({'NOTOK': str(data)})
        else:
            timetoken = data["expireddate"]
            group_name = data["group_name"]
            nameuser = data["user"]
            logging.info(str(session) +"|deteleaccount|"+str(data))
            if True:
                currentDate = time.strftime("%Y-%m-%d %H:%M:%S")
                print(currentDate)
                if str(timetoken) >= str(currentDate) :
                    record = json.loads(request.data)
                    logging.info(str(session) +"|input|"+str(request.data))
                    id = record["id"]
                    name = record["name"]
                    email = record["email"]
                    phone = record["phone"]
                    position = record["position"]
                    partner = record["partner"]
                    # lấy thông tin partner
                    sqlpartner = "select id from partner where nickname = '"+str(partner)+"'"
                    df = pd.read_sql(sqlpartner,db)
                    if df.empty:
                        db.close()
                        logging.error(str(session) +"|response|id partner tồn tại")
                        return jsonify({'OK': 'id partner tồn tại'}),400
                    partnerid = df.iloc[0,0]
                    now = datetime.datetime.now()
                    current_time = now.strftime("%Y-%m-%d %H:%M:%S")
                    sqlcheck= "select count(*) from contact where id ='"+str(id)+"'"
                    df = pd.read_sql(sqlcheck,db)
                    sl = df.iloc[0,0]
                    if int(sl) > 0 :
                        sql="update contact set name = '"+str(name)+"',email = '"+str(email)+"',phone = '"+str(phone)+"',position = '"+str(position)+"',partnerid = '"+str(partnerid)+"' where id ='"+str(id)+"'"
                        logging.info(str(session) +"|query|"+str(sql))
                        cursor.execute(sql)
                        sqllog = "insert into log (name,api,content,type) value ('"+str(nameuser)+"','updatecontact','"+str(record).replace("'","")+"','UPDATE')"
                        cursor.execute(sqllog)
                        db.commit()
                        db.close()
                        logging.info(str(session) +"|response | Thành công")
                        return jsonify({'OK': 'OK'})
                    else :
                        db.close()
                        logging.error(str(session) +"|response|id không tồn tại")
                        return jsonify({'OK': 'id không tồn tại'}),400
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
@contact.route('/saovang_listcontact', methods=['GET'])
def saovang_listcontact():
    logging.info(str(session) +"|listcontact")
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
            logging.info(str(session) +"|listcontact|"+str(data))
            if True:
                currentDate = time.strftime("%Y-%m-%d %H:%M:%S")
                print(currentDate)
                if str(timetoken) >= str(currentDate) :
                    sql ="SELECT a.id,a.name,a.email,a.phone,case when position = 1 then 'Sale' when position = 2 then 'Kỹ thuật' else 'Giám Đốc' END AS position ,b.nickname'partner' FROM (select * from contact where note = 'saovang') a left join partner b on a.partnerid =b.id"
                    df = pd.read_sql(sql,db)
                    json_records = df.to_json(orient ='records')
                    data = []
                    data = json.loads(json_records)
                    context = {'data': data,'code': 'OK'}
                    sqllog = "insert into log (name,api,content,type) value ('"+str(nameuser)+"','listcontact','view','SELECT')"
                    cursor = db.cursor()
                    cursor.execute(sqllog)
                    db.commit()
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
@contact.route('/saovang_insertcontact', methods=['POST'])
def saovang_insertcontact():
    logging.info(str(session) +"|insertcontact")
    try:
        db = pymysql.connect(host="172.17.0.1",    # your host, usually localhost
                     user="leeoncrm",         # your username
                     passwd="41XmKsO3NBgHPwv",  # your password
                     db="leeon_crm",
                     port=3306
                     )
        cursor = db.cursor()
        data = group()
        if str(data)== "chua truyen token" :
            return jsonify({'NOTOK': str(data)})
        else:
            timetoken = data["expireddate"]
            group_name = data["group_name"]
            nameuser = data["user"]
            logging.info(str(session) +"|deteleaccount|"+str(data))
            if True:
                currentDate = time.strftime("%Y-%m-%d %H:%M:%S")
                print(currentDate)
                if str(timetoken) >= str(currentDate) :
                    record = json.loads(request.data)
                    logging.info(str(session) +"|input|"+str(request.data))
                    name = record["name"]
                    email = record["email"]
                    phone = record["phone"]
                    position = record["position"]
                    partner = record["partner"]
                    # lấy thông tin partner
                    sqlpartner = "select id from partner where nickname = '"+str(partner)+"'"
                    logging.info(str(session) +"|query_getpartner|"+str(sqlpartner))
                    df = pd.read_sql(sqlpartner,db)
                    if df.empty:
                        db.close()
                        logging.error(str(session) +"|response|partner không tồn tại")
                        return jsonify({'NOTOK': 'partner khong ton tai'}),400
                    partnerid = df.iloc[0,0]
                    now = datetime.datetime.now()
                    current_time = now.strftime("%Y-%m-%d %H:%M:%S")
                    sql="insert into contact(name,email,phone,position,partnerid,note) value ('"+str(name)+"','"+str(email)+"','"+str(phone)+"','"+str(position)+"','"+str(partnerid)+"','saovang')"
                    logging.info(str(session) +"|query |"+ str(sql))
                    cursor.execute(sql)
                    sqllog = "insert into log (name,api,content,type) value ('"+str(nameuser)+"','insertcontact','"+str(record).replace("'","")+"','INSERT')"
                    cursor.execute(sqllog)
                    db.commit()
                    logging.info(str(session) +"|response | Thành công")
                    return jsonify({'OK': 'OK'})
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