from flask import Blueprint,send_from_directory,send_file
from flask import request, jsonify
import json
import logging
import requests
from requests.auth import HTTPDigestAuth
import pandas as pd
from pandas import json_normalize
import pymysql
from flask_jsonpify import jsonpify
from datetime import datetime
from function import group
import time
from datetime import datetime
import datetime
import re
import os,zipfile
import traceback
import math
import random
import string
import smtplib
import email
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from email import encoders
from email.header import Header
from itertools import chain
daily = Blueprint('daily', __name__)
@daily.route('/list_daily', methods=['GET'])
def list_daily():
    try:
        db = pymysql.connect(host="172.17.0.1",    # your host, usually localhost
                     user="leeoncrm",         # your username
                     passwd="41XmKsO3NBgHPwv",  # your password
                     db="leeon_crm",
                     port=3306
                     )

        data = group()
        print(data)
        if str(data)== "chua truyen token" :
            return jsonify({'NOTOK': str(data)})
        else:
            timetoken = data["expireddate"]
            group_name = data["group_name"]
            if True:
                currentDate = time.strftime("%Y-%m-%d %H:%M:%S")
                logging.info(currentDate)
                if str(timetoken) >= str(currentDate) :
                    sql = "SELECT * FROM agency"
                    df = pd.read_sql(sql,db)
                    df['createdtime'] = df['createdtime'].astype(str)
                    #convert cột created_time sang dạng chuỗi
                    json_records = df.to_json(orient ='records')
                    data = []
                    data = json.loads(json_records)
                    context = {'data': data,'code': 'OK'}
                    db.close()
                    return context
                else :
                    return jsonify({'NOTOK': 'token hết hạn','code':401}),401
            else :
                return jsonify({'NOTOK': 'khong co quyen'})
    except Exception as e:
        logging.error(traceback.format_exc())
        return jsonify({'NOTOK': 'loi he thong'}),400
@daily.route('/insert_daily', methods=['POST'])
def insert_daily():
    db = pymysql.connect(host="172.17.0.1",    # your host, usually localhost
                     user="leeoncrm",         # your username
                     passwd="41XmKsO3NBgHPwv",  # your password
                     db="leeon_crm",
                     port=3306
                     )
    cursor = db.cursor()
    # session=datetime.now()
    logging.info("------------------insert_daily------------")
    try:
        data = group()
        if str(data)== "chua truyen token" :
            return jsonify({'NOTOK': str(data)})
        else:
            timetoken = data["expireddate"]
            group_name = data["group_name"]
            nameuser = data["user"]
            logging.info(str(data))
            if True:
                currentDate = time.strftime("%Y-%m-%d %H:%M:%S")
                print(currentDate)
                if str(timetoken) >= str(currentDate) :
                    record = json.loads(request.data)
                    logging.info("|input|"+str(request.data))
                    company = record["company"]
                    taxcode = record["taxcode"]
                    nickname = record["nickname"]
                    represent = record["represent"]
                    email = record["email"]
                    address = record["address"]
                    phone = record["phone"]
                    website= record["website"]
                    bank= record["bank"]
                    bankbranch= record["bankbranch"]
                    bankaccount= record["bankaccount"]
                    ip= record["ip"]
                    ipss = ip.split(":")
                    ips = ipss[0]
                    try :
                        port = ipss[1]
                    except :
                        port = ""
                    now = datetime.datetime.now()
                    current_time = now.strftime("%Y-%m-%d %H:%M:%S")
                    sqlcheck= "select * from agency where nickname ='"+str(nickname)+"'"
                    logging.info("|sqlcheck|"+str(sqlcheck))
                    df = pd.read_sql(sqlcheck,db)
                    if not df.empty:
                        logging.error("|response|Nickname đã tồn tại")
                        return jsonify({'NOTOK': 'Nickname đã tồn tại'}),400
                    sql="insert into agency(company,nickname,taxcode,address,represent,phone,email,website,createdtime,updatedtime,bankaccount,bank,bankbranch,ip) value ('"+str(company)+"','"+str(nickname)+"','"+str(taxcode)+"','"+str(address)+"','"+str(represent)+"','"+str(phone)+"','"+str(email)+"','"+str(website)+"','"+str(current_time)+"','"+str(current_time)+"','"+str(bankaccount)+"','"+str(bank)+"','"+str(bankbranch)+"','"+str(ip)+"')"
                    logging.info("|query|"+str(sql))
                    try:
                        cursor.execute(sql)
                        db.commit()
                        db.close()
                    except Exception as e:
                        logging.error("|response|" +str(traceback.format_exc()))
                        return jsonify({'NOTOK': str(e)}),400
                    logging.info("|response|Thành công")
                    return jsonify({'OK': 'OK'})
                else :
                    logging.error("|response|token hết hạn")
                    return jsonify({'NOTOK': 'token hết hạn','code':401}),401
            else :
                logging.error("|response|Không có quyền")
                return jsonify({'NOTOK': 'khong co quyen'}),400
    except Exception as e:
        logging.error(str(traceback.format_exc()))
        return jsonify({'NOTOK': str(e)}),400
@daily.route('/delete_daily', methods=['DELETE'])
def delete_daily():
    logging.info("----------------------daily_detail-------------------------")
    try:
        db = pymysql.connect(host="172.17.0.1",    # your host, usually localhost
                     user="leeoncrm",         # your username
                     passwd="41XmKsO3NBgHPwv",  # your password
                     db="leeon_crm",
                     port=3306
                     )
        data = group()
        logging.info(data)
        if str(data)== "chua truyen token" :
            return jsonify({'NOTOK': str(data)})
        else:
            timetoken = data["expireddate"]
            group_name = data["group_name"]
            if True:
                currentDate = time.strftime("%Y-%m-%d %H:%M:%S")
                logging.info(currentDate)
                if str(timetoken) >= str(currentDate) :
                    # lấy thời gian hiện tại
                    record = json.loads(request.data)
                    logging.info(record)
                    id = record["id"]
                    sql="delete from agency where id = '"+str(id)+"'"
                    logging.info(sql)
                    cursor = db.cursor()
                    cursor.execute(sql)
                    db.commit()
                    return jsonify({'OK': 'OK'}),200
                else :
                    return jsonify({'NOTOK': 'token hết hạn','code':401}),401
            else :
                return jsonify({'NOTOK': 'khong co quyen'}),400
    except Exception as e:
        logging.error(traceback.format_exc())
        return jsonify({'NOTOK': 'NOTOK'}),400

@daily.route('/updateagency', methods=['POST'])
def updateagency():
    db = pymysql.connect(host="172.17.0.1",    # your host, usually localhost
                     user="leeoncrm",         # your username
                     passwd="41XmKsO3NBgHPwv",  # your password
                     db="leeon_crm",
                     port=3306
                     )
    cursor = db.cursor()
    # session=datetime.now()
    logging.info("------------------insert_daily------------")
    try:
        data = group()
        if str(data)== "chua truyen token" :
            return jsonify({'NOTOK': str(data)})
        else:
            timetoken = data["expireddate"]
            group_name = data["group_name"]
            nameuser = data["user"]
            logging.info(str(data))
            if True:
                currentDate = time.strftime("%Y-%m-%d %H:%M:%S")
                print(currentDate)
                if str(timetoken) >= str(currentDate) :
                    record = json.loads(request.data)
                    logging.info("|input|"+str(request.data))
                    id = record["id"]
                    company = record["company"]
                    taxcode = record["taxcode"]
                    nickname = record["nickname"]
                    represent = record["represent"]
                    email = record["email"]
                    address = record["address"]
                    phone = record["phone"]
                    website= record["website"]
                    bank= record["bank"]
                    bankbranch= record["bankbranch"]
                    bankaccount= record["bankaccount"]
                    ip= record["ip"]
                    ipss = ip.split(":")
                    ips = ipss[0]
                    try :
                        port = ipss[1]
                    except :
                        port = ""
                    sql="update agency set company ='"+str(company)+"',address ='"+str(address)+"',email = '"+str(email)+"',nickname = '"+str(nickname)+"',phone = '"+str(phone)+"',represent = '"+str(represent)+"',website = '"+str(website)+"',ip = '"+str(ip)+"',taxcode='"+str(taxcode)+"',bankaccount='"+str(bankaccount)+"',bank='"+str(bank)+"',bankbranch='"+str(bankbranch)+"' where id ='"+str(id)+"'"
                    logging.info("|query|"+str(sql))
                    cursor.execute(sql)
                    db.commit()
                    db.close()
                    logging.info("|response|Thành công")
                    return jsonify({'OK': 'OK'})
                else :
                    logging.error("|response|token hết hạn")
                    return jsonify({'NOTOK': 'token hết hạn','code':401}),401
            else :
                logging.error("|response|Không có quyền")
                return jsonify({'NOTOK': 'khong co quyen'}),400
    except Exception as e:
        logging.error(str(traceback.format_exc()))
        return jsonify({'NOTOK': str(e)}),400