from flask import Blueprint,send_from_directory,send_file
from flask import request, jsonify
import pandas as pd
import pymysql
from flask_jsonpify import jsonpify
from datetime import datetime
import datetime
import json
import os
import time
import logging
import traceback
from function import group
from sqlalchemy import create_engine
import threading
sms_otp = Blueprint('sms_otp', __name__)
@sms_otp.route('/list_otp_partner', methods=['GET'])
def list_otp_partner():
    try :
        session=datetime.datetime.now()
        logging.info(str(session) + "| list_otp_partner")
        db_connection_str = 'mysql+pymysql://crmleeon:BNAie92839470938888@172.17.0.18/gmb_otpgw'
        db_connection = create_engine(db_connection_str)
        # write log with logging
        logging.info(str(session) + "| connect to db ok")
        sql = "SELECT b.id,a.company,a.nickname,a.limitbalance ,a.currentbalance,a.todayconsumption,a.link,b.apikey,b.status,b.ip, CASE WHEN a.type = 1 THEN 'API' ELSE 'SMPP' END AS type FROM partner a,partner_apikey b WHERE a.id = b.partner_id ORDER BY a.id"
        df = pd.read_sql_query(sql, db_connection)
        logging.info(str(session) + "| query ok")
        df = df.to_json(orient='records')
        data = []
        data = json.loads(df)
        logging.info(str(session) + "| convert to json ok")
        context = {'data': data,'code': 'OK'}
        logging.info(str(session) + "| return ok")
        return context
    except Exception as e :
        #write log with traceback
        logging.error(str(session) + "| error : " + str(traceback.format_exc()))
        logging.info(str(session) + "| return error")
        return jsonify({'NOTOK': 'ERROR'}),400
@sms_otp.route('/insert_otp_partner', methods=['POST'])
def insert_otp_partner():
    try:
        session=datetime.datetime.now()
        logging.info(str(session) + "| insert_otp_partner")
        data = group()
        if str(data)== "chua truyen token" :
            return jsonify({'NOTOK': str(data)})
        else :
            timetoken = data["expireddate"]
            group_name = data["group_name"]
            nameuser = data["user"]
            logging.info(str(session) + "| check token ok | "+str(data))
            if True:
                #so sánh thời gian hiện tại với timetoken
                timetoken = int(timetoken.replace(" ","").replace(":","").replace("-","").replace(".",""))
                logging.info(str(session) + "| timetoken : " + str(timetoken))
                timenow = int(str(datetime.datetime.now()).replace(" ","").replace(":","").replace("-","").replace(".",""))
                logging.info(str(session) + "| timenow : " + str(timenow))
                if timetoken > timenow :
                    db_connection_str = 'mysql+pymysql://crmleeon:BNAie92839470938888@172.17.0.18/gmb_otpgw'
                    db_connection = create_engine(db_connection_str)
                    record = json.loads(request.data)
                    company = record['company']
                    nickname = record['nickname']
                    type = record['type']
                    if type == "API" :
                        type = 1
                    else :
                        type = 2
                    apikey = record['apikey']
                    link_api = record['link_api']
                    ip = record['ip']
                    #ghi log dữ liệu đầu vào
                    logging.info(str(session) + "| company : " + str(company) + "| nickname : " + str(nickname) + "| type : " + str(type) + "| apikey : " + str(apikey) + "| link_api : " + str(link_api)+ "| ip : " + str(ip))
                    # check dữ liệu trùng nickname
                    sql_check_nickname = "SELECT * FROM partner WHERE nickname = '"+str(nickname)+"'"
                    df = pd.read_sql_query(sql_check_nickname, db_connection)
                    if df.empty == False :
                        logging.info(str(session) + "| nickname da ton tai")
                        return jsonify({'NOTOK': 'nickname da ton tai'}),400
                    #check dữ liệu trùng apikey
                    sql_check_apikey = "SELECT * FROM partner_apikey WHERE apikey = '"+str(apikey)+"'"
                    df = pd.read_sql_query(sql_check_apikey, db_connection)
                    if df.empty == False :
                        logging.info(str(session) + "| apikey da ton tai")
                        return jsonify({'NOTOK': 'apikey da ton tai'}),400
                    sql_insert_partner = "INSERT INTO partner (company,nickname,type,link) VALUES ('"+str(company)+"','"+str(nickname)+"','"+str(type)+"','"+str(link_api)+"')"
                    # execute query
                    db_connection.execute(sql_insert_partner)
                    logging.info(str(session) + "| insert partner | " +str(sql_insert_partner))
                    logging.info(str(session) + "| insert partner ok")
                    # get id partner
                    sql_get_id_partner = "SELECT id FROM partner WHERE nickname = '"+str(nickname)+"'"
                    df = pd.read_sql_query(sql_get_id_partner, db_connection)
                    id_partner = df['id'][0]
                    logging.info(str(session) + "| get id partner | " +str(id_partner))
                    # insert partner_apikey
                    sql_insert_partner_apikey = "INSERT INTO partner_apikey (partner_id,apikey,ip,status) VALUES ('"+str(id_partner)+"','"+str(apikey)+"','"+str(ip)+"','0')"
                    # execute query
                    db_connection.execute(sql_insert_partner_apikey)
                    logging.info(str(session) + "| insert partner_apikey | " +str(sql_insert_partner_apikey))
                    logging.info(str(session) + "| insert partner_apikey ok")
                    logging.info(str(session) + "| return ok")
                    return jsonify({'OK': 'OK'}),200
                else :
                    logging.error(str(session) +"|response|token hết hạn")
                    return jsonify({'NOTOK': 'token hết hạn','code':401}),401
    except Exception as e :
        #write log with traceback
        logging.error(str(session) + "| error : " + str(traceback.format_exc()))
        logging.info(str(session) + "| return error")
        return jsonify({'NOTOK': 'ERROR'}),400
@sms_otp.route('/topup_partner', methods=['POST'])
def topup_partner():
    try:
        session=datetime.datetime.now()
        logging.info(str(session) + "| topup_partner")
        data = group()
        if str(data)== "chua truyen token" :
            return jsonify({'NOTOK': str(data)})
        else :
            timetoken = data["expireddate"]
            group_name = data["group_name"]
            nameuser = data["user"]
            logging.info(str(session) + "| check token ok | "+str(data))
            if True:
                #so sánh thời gian hiện tại với timetoken
                if True:
                    db_connection_str = 'mysql+pymysql://crmleeon:BNAie92839470938888@172.17.0.18/gmb_otpgw'
                    db_connection = create_engine(db_connection_str)
                    logging.info(str(session) + "| connect db ok")
                    record = json.loads(request.data)
                    balance = record['balance']
                    #check balance chỉ có số
                    if balance.isdigit() == False :
                        logging.info(str(session) + "| balance chi duoc chua so")
                        return jsonify({'NOTOK': 'balance chi duoc chua so'}),400
                    apikey = record['apikey']
                    logging.info(str(session) + "| balance : " + str(balance) + "| apikey : " + str(apikey))
                    #check dữ liệu trùng apikey
                    sql_check_nickname = "SELECT * FROM partner_apikey WHERE apikey = '"+str(apikey)+"'"
                    df = pd.read_sql_query(sql_check_nickname, db_connection)
                    if df.empty == True :
                        logging.info(str(session) + "| apikey khong ton tai")
                        return jsonify({'NOTOK': 'apikey khong ton tai'}),400
                    sqlupdate = "UPDATE partner SET limitbalance = limitbalance + "+str(balance)+" WHERE id = (select partner_id from partner_apikey where apikey = '"+str(apikey)+"')"
                    logging.info(str(session) + "| update partner | " +str(sqlupdate))
                    # execute query
                    db_connection.execute(sqlupdate)
                    logging.info(str(session) + "| update partner ok")
                    return jsonify({'OK': 'OK'}),200
    except Exception as e:
        #write log with traceback
        logging.error(str(session) + "| error : " + str(e))
        logging.error(str(session) + "| error : " + str(traceback.format_exc()))
        return jsonify({'NOTOK': 'ERROR'}),400
@sms_otp.route('/update_otp_partner', methods=['POST'])
def update_otp_partner():
    try:
        session=datetime.datetime.now()
        logging.info(str(session) + "| insert_otp_partner")
        data = group()
        if str(data)== "chua truyen token" :
            return jsonify({'NOTOK': str(data)})
        else :
            timetoken = data["expireddate"]
            group_name = data["group_name"]
            nameuser = data["user"]
            logging.info(str(session) + "| check token ok | "+str(data))
            if True:
                #so sánh thời gian hiện tại với timetoken
                if timetoken > str(datetime.datetime.now()):
                    db_connection_str = 'mysql+pymysql://crmleeon:BNAie92839470938888@172.17.0.18/gmb_otpgw'
                    db_connection = create_engine(db_connection_str)
                    record = json.loads(request.data)
                    company = record['company']
                    nickname = record['nickname']
                    type = record['type']
                    if type == "API" :
                        type = 1
                    else :
                        type = 2
                    apikey = record['apikey']
                    link_api = record['link_api']
                    ip = record['ip']
                    id = record['id']
                    return jsonify({'OK': 'OK'}),200
    except Exception :
        #write log with traceback
        logging.error(str(session) + "| error : " + str(traceback.format_exc()))
        return jsonify({'NOTOK': 'ERROR'}),400

@sms_otp.route('/sms_statistical', methods=['GET'])
def sms_statistical():
    try:
        session=datetime.datetime.now()
        logging.info(str(session) + "| sms_statistical")
        data = group()
        if str(data)== "chua truyen token" :
            return jsonify({'NOTOK': str(data)})
        else :
            timetoken = data["expireddate"]
            group_name = data["group_name"]
            nameuser = data["user"]
            logging.info(str(session) + "| check token ok | "+str(data))
            if True:
                #so sánh thời gian hiện tại với timetoken
                if timetoken > str(datetime.datetime.now()):
                    session=datetime.datetime.now()
                    table = str(session).replace("-","").replace(":","").replace(" ","")[0:6]
                    logging.info(str(session) + "| list_otp_partner")
                    db_connection_str = 'mysql+pymysql://crmleeon:BNAie92839470938888@172.17.0.18/gmb_otpgw'
                    db_connection = create_engine(db_connection_str)
                    # write log with logging
                    logging.info(str(session) + "| connect to db ok")
                    sql = "SELECT a.id,a.created_time,a.sender,a.mobile,b.nickname,a.message FROM sms_log"+table+" a, vendor b WHERE a.vendorid=b.id ORDER BY a.id DESC limit 100"
                    df = pd.read_sql_query(sql, db_connection)
                    #convert create_time to string
                    df['created_time'] = df['created_time'].astype(str)
                    logging.info(str(session) + "| query ok")
                    df = df.to_json(orient='records')
                    data = []
                    data = json.loads(df)
                    logging.info(str(session) + "| convert to json ok")
                    context = {'data': data,'code': 'OK'}
                    logging.info(str(session) + "| return ok")
                    return context
                else:
                    return jsonify({'NOTOK': 'token expired'}),400
    except Exception :
        #write log with traceback
        logging.error(str(session) + "| error : " + str(traceback.format_exc()))
        return jsonify({'NOTOK': 'ERROR'}),400

@sms_otp.route('/find_sms_statistical', methods=['GET'])
def find_sms_statistical():
    try:
        session=datetime.datetime.now()
        logging.info(str(session) + "| find_sms_statistical")
        data = group()
        if str(data)== "chua truyen token" :
            return jsonify({'NOTOK': str(data)})
        else :
            timetoken = data["expireddate"]
            group_name = data["group_name"]
            nameuser = data["user"]
            logging.info(str(session) + "| check token ok | "+str(data))
            if True:
                #so sánh thời gian hiện tại với timetoken
                if True:
                    table = str(session).replace("-","").replace(":","").replace(" ","")[0:6]
                    db_connection_str = 'mysql+pymysql://crmleeon:BNAie92839470938888@172.17.0.18/gmb_otpgw'
                    db_connection = create_engine(db_connection_str)
                    logging.info(str(session) + "| connect db ok")
                    start_time = request.args.get('start_time')
                    end_time = request.args.get('end_time')
                    end_time =str(end_time) +" 23:59:59"
                    sender = request.args.get('sender')
                    mobile = request.args.get('mobile')
                    message = request.args.get('message')
                    # insert log all input
                    logging.info(str(session) + "| start_time : "+str(request.args))
                    logging.info(str(session) + "| start_time : "+str(start_time))
                    logging.info(str(session) + "| end_time : "+str(end_time))
                    logging.info(str(session) + "| sender : "+str(sender))
                    logging.info(str(session) + "| mobile : "+str(mobile))
                    logging.info(str(session) + "| message : "+str(message))
                    if start_time == "" :
                        sql_start_time = ""
                    else :
                        sql_start_time = " and a.created_time >= '"+str(start_time)+"'"
                    if end_time == "" :
                        sql_end_time = ""
                    else :
                        sql_end_time = " and a.created_time <= '"+str(end_time)+"'"
                    if sender == "" :
                        sql_sender = ""
                    else :
                        sql_sender = " and a.sender = '"+str(sender)+"'"
                    if mobile == "" :
                        sql_mobile = ""
                    else :
                        sql_mobile = " and a.mobile = '"+str(mobile)+"'"
                    if message == "" :
                        sql_message = ""
                    else :
                        sql_message = " and a.message = '"+str(message)+"'"
                    sql = "SELECT a.id,a.created_time,a.sender,a.mobile,b.nickname,a.message FROM sms_log202302 a, vendor b WHERE a.id is not null"+sql_sender+sql_start_time+sql_end_time+sql_mobile+sql_message+" AND a.vendorid=b.id ORDER BY a.id DESC"
                    logging.info(str(session) + "| sql | " +str(sql))
                    df = pd.read_sql_query(sql, db_connection)
                    df['created_time'] = df['created_time'].astype(str)
                    logging.info(str(session) + "| query ok")
                    df = df.to_json(orient='records')
                    #convert create_time to string
                    data = []
                    data = json.loads(df)
                    logging.info(str(session) + "| convert to json ok")
                    context = {'data': data,'code': 'OK'}
                    logging.info(str(session) + "| return ok")
                    return context
    except Exception as e:
        #write log with traceback
        logging.error(str(session) + "| error : " + str(e))
        logging.error(str(session) + "| error : " + str(traceback.format_exc()))
        return jsonify({'NOTOK': 'ERROR'}),400