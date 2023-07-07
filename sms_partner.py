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
from sqlalchemy import create_engine
import datetime
import time
import logging
import traceback
from function import group
sms_partner = Blueprint('sms_partner', __name__)
@sms_partner.route('/report_sms_vendor', methods=['GET'])
def report_sms_vendor():
    session=datetime.datetime.now()
    logging.info(str(session) +"|report_sms_vendor")
    try :
        db_connection_str = 'mysql+pymysql://crmleeon:BNAie92839470938888@172.17.0.18/gmb_otpgw'
        db_connection = create_engine(db_connection_str)
        data = group()
        if str(data)== "chua truyen token" :
            return jsonify({'NOTOK': str(data)})
        else:
            timetoken = data["expireddate"]
            group_name = data["group_name"]
            nameuser = data["user"]
            logging.info(str(session) +"|updateaccount|"+str(data))
            if True:
                currentDate = time.strftime("%Y-%m-%d %H:%M:%S")
                print(currentDate)
                if str(timetoken) >= str(currentDate) :
                    sql ="SELECT id,DATE(created_time)'created_time',vendor_name,total_sender,sender_exists,sender_not_exists,revenue FROM report_vendor WHERE DATE(created_time)=CURDATE()"
                    df = pd.read_sql(sql,db_connection)
                    df['created_time'] = df['created_time'].astype('str')
                    json_records = df.to_json(orient ='records')
                    data = []
                    data = json.loads(json_records)
                    sql_total = "SELECT 1'id',count(vendor_name)'vendor_name',SUM(total_sender)'total_sender',SUM(sender_exists)'sender_exists',SUM(sender_not_exists)'sender_not_exists',SUM(revenue)'revenue' FROM report_vendor WHERE DATE(created_time)= CURDATE()"
                    df_total = pd.read_sql(sql_total,db_connection)
                    json_records_total = df_total.to_json(orient ='records')
                    data_total = []
                    data_total = json.loads(json_records_total)
                    context = {'data': data,'total':data_total,'code': 'OK'}
                    logging.info(str(session) +"|response|Thành công")
                    return context
                else :
                    logging.error(str(session) +"|response|token hết hạn")
                    return jsonify({'NOTOK': 'token hết hạn','code':401}),401
            else :
                logging.error(str(session) +"|response|Không có quyền")
                return jsonify({'NOTOK': 'khong co quyen'}) 
    except Exception as e:
        logging.error(str(session) + "| error : " + str(traceback.format_exc()))
        return jsonify({'NOTOK': str(e)}),400
@sms_partner.route('/report_sms_partner', methods=['GET'])
def report_sms_partner():
    session=datetime.datetime.now()
    logging.info(str(session) +"|report_sms_partner")
    try :
        db_connection_str = 'mysql+pymysql://crmleeon:BNAie92839470938888@172.17.0.18/gmb_otpgw'
        db_connection = create_engine(db_connection_str)
        data = group()
        if str(data)== "chua truyen token" :
            logging.error(str(session) +"|response|chua truyen token")
            return jsonify({'NOTOK': str(data)})
        else:
            timetoken = data["expireddate"]
            group_name = data["group_name"]
            nameuser = data["user"]
            logging.info(str(session) +"|updateaccount|"+str(data))
            if True:
                currentDate = time.strftime("%Y-%m-%d %H:%M:%S")
                print(currentDate)
                if str(timetoken) >= str(currentDate) :
                    sql ="SELECT a.id,DATE(a.created_time)'created_time',b.nickname'partner_name',a.pick_isdn,a.otp_receive,a.revenue FROM report_partner a,partner b WHERE DATE(a.created_time)=CURDATE() AND a.partner_id = b.id"
                    df = pd.read_sql(sql,db_connection)
                    df['created_time'] = df['created_time'].astype('str')
                    json_records = df.to_json(orient ='records')
                    data = []
                    data = json.loads(json_records)
                    sql_total = "SELECT 1'id',count(partner_name)'partner_name',sum(pick_isdn)'pick_isdn',sum(otp_receive)'otp_receive',sum(revenue)'revenue' FROM report_partner WHERE DATE(created_time)=CURDATE()"
                    df_total = pd.read_sql(sql_total,db_connection)
                    json_records_total = df_total.to_json(orient ='records')
                    data_total = []
                    data_total = json.loads(json_records_total)
                    context = {'data': data,'total':data_total,'code': 'OK'}
                    logging.info(str(session) +"|response|Thành công")
                    return context
                else :
                    logging.error(str(session) +"|response|token hết hạn")
                    return jsonify({'NOTOK': 'token hết hạn','code':401}),401
            else :
                logging.error(str(session) +"|response|Không có quyền")
                return jsonify({'NOTOK': 'khong co quyen'}) 
    except Exception as e:
        logging.error(str(session) + "| error : " + str(traceback.format_exc()))
        return jsonify({'NOTOK': str(e)}),400

@sms_partner.route('/report_sms_brand', methods=['GET'])
def report_sms_brand():
    session=datetime.datetime.now()
    logging.info(str(session) +"|report_sms_brand")
    try :
        db_connection_str = 'mysql+pymysql://crmleeon:BNAie92839470938888@172.17.0.18/gmb_otpgw'
        db_connection = create_engine(db_connection_str)
        data = group()
        if str(data)== "chua truyen token" :
            return jsonify({'NOTOK': str(data)})
        else:
            timetoken = data["expireddate"]
            group_name = data["group_name"]
            nameuser = data["user"]
            logging.info(str(session) +"|report_sms_brand|"+str(data))
            if True:
                currentDate = time.strftime("%Y-%m-%d %H:%M:%S")
                print(currentDate)
                if str(timetoken) >= str(currentDate) :
                    sql ="SELECT id,DATE(created_time)'created_time',sender_name,vendor_sent,sent_telco,sent_partner_otp,revenue FROM report_sender WHERE DATE(created_time)=CURDATE()"
                    df = pd.read_sql(sql,db_connection)
                    df['created_time'] = df['created_time'].astype('str')
                    json_records = df.to_json(orient ='records')
                    data = []
                    data = json.loads(json_records)
                    sql_total = "SELECT 1'id',count(sender_name)'sender_name',sum(vendor_sent)'vendor_sent',sum(sent_telco)'sent_telco',sum(sent_partner_otp)'sent_partner_otp',sum(revenue)'revenue' FROM report_sender WHERE DATE(created_time)=CURDATE()"
                    df_total = pd.read_sql(sql_total,db_connection)
                    json_records_total = df_total.to_json(orient ='records')
                    data_total = []
                    data_total = json.loads(json_records_total)
                    context = {'data': data,'total':data_total,'code': 'OK'}
                    logging.info(str(session) +"|response|Thành công")
                    return context
                else :
                    logging.error(str(session) +"|response|token hết hạn")
                    return jsonify({'NOTOK': 'token hết hạn','code':401}),401
            else :
                logging.error(str(session) +"|response|Không có quyền")
                return jsonify({'NOTOK': 'khong co quyen'}) 
    except Exception as e:
        logging.error(str(session) + "| error : " + str(traceback.format_exc()))
        return jsonify({'NOTOK': str(e)}),400
@sms_partner.route('/find_report_sms_vendor', methods=['GET'])
def find_report_sms_vendor():
    session=datetime.datetime.now()
    logging.info(str(session) +"|find_report_sms_vendor")
    try :
        db_connection_str = 'mysql+pymysql://crmleeon:BNAie92839470938888@172.17.0.18/gmb_otpgw'
        db_connection = create_engine(db_connection_str)
        data = group()
        if str(data)== "chua truyen token" :
            logging.error(str(session) +"|response|chua truyen token")
            return jsonify({'NOTOK': str(data)})
        else:
            timetoken = data["expireddate"]
            group_name = data["group_name"]
            nameuser = data["user"]
            logging.info(str(session) +"|updateaccount|"+str(data))
            if True:
                currentDate = time.strftime("%Y-%m-%d %H:%M:%S")
                print(currentDate)
                if str(timetoken) >= str(currentDate) :
                    starttime = request.args.get('starttime')
                    endtime = request.args.get('endtime')
                    endtime = str(endtime) + " 23:59:59"
                    vendor_name = request.args.get('vendor_name')
                    if str(starttime) == "" :
                        timestartquery = ""
                    else :
                        timestartquery = " and created_time >= '"+ str(starttime) +"'"
                    if str(endtime) == "" :
                        timeendquery = ""
                    else :
                        timeendquery = " and created_time <= '"+ str(endtime) +"'"
                    if str(vendor_name) == "" :
                        vendor_namequery = ""
                    else :
                        vendor_namequery = " and vendor_name = '"+ str(vendor_name)+"'"
                    query = "id is not null" +timestartquery + timeendquery + vendor_namequery
                    sql ="SELECT id,DATE(created_time)'created_time',vendor_name,total_sender,sender_exists,sender_not_exists,revenue FROM report_vendor WHERE "+ query
                    sql_total = "SELECT 1'id',count(vendor_name)'vendor_name',SUM(total_sender)'total_sender',SUM(sender_exists)'sender_exists',SUM(sender_not_exists)'sender_not_exists',SUM(revenue)'revenue' FROM report_vendor WHERE " + query
                    df = pd.read_sql(sql,db_connection)
                    df['created_time'] = df['created_time'].astype('str')
                    json_records = df.to_json(orient ='records')
                    data = []
                    data = json.loads(json_records)
                    df_total = pd.read_sql(sql_total,db_connection)
                    json_records_total = df_total.to_json(orient ='records')
                    data_total = []
                    data_total = json.loads(json_records_total)
                    context = {'data': data,'total':data_total,'code': 'OK'}
                    logging.info(str(session) +"|response|Thành công")
                    return context
                else :
                    logging.error(str(session) +"|response|token hết hạn")
                    return jsonify({'NOTOK': 'token hết hạn','code':401}),401
            else :
                logging.error(str(session) +"|response|Không có quyền")
                return jsonify({'NOTOK': 'khong co quyen'}) 
    except Exception as e:
        logging.error(str(session) + "| error : " + str(traceback.format_exc()))
        return jsonify({'NOTOK': str(e)}),400

@sms_partner.route('/find_report_sms_partner', methods=['GET'])
def find_report_sms_partner():
    session=datetime.datetime.now()
    logging.info(str(session) +"|find_report_sms_partner")
    try :
        db_connection_str = 'mysql+pymysql://crmleeon:BNAie92839470938888@172.17.0.18/gmb_otpgw'
        db_connection = create_engine(db_connection_str)
        data = group()
        if str(data)== "chua truyen token" :
            return jsonify({'NOTOK': str(data)})
        else:
            timetoken = data["expireddate"]
            group_name = data["group_name"]
            nameuser = data["user"]
            logging.info(str(session) +"|updateaccount|"+str(data))
            if True:
                currentDate = time.strftime("%Y-%m-%d %H:%M:%S")
                print(currentDate)
                if str(timetoken) >= str(currentDate) :
                    starttime = request.args.get('starttime')
                    endtime = request.args.get('endtime')
                    endtime = str(endtime) + " 23:59:59"
                    partner_name = request.args.get('partner_name')
                    if str(starttime) == "" :
                        timestartquery = ""
                    else :
                        timestartquery = " and a.created_time >= '"+ str(starttime) +"'"
                    if str(endtime) == "" :
                        timeendquery = ""
                    else :
                        timeendquery = " and a.created_time <= '"+ str(endtime) +"'"
                    if str(partner_name) == "" :
                        partner_namequery = ""
                    else :
                        partner_namequery = " and b.nickname = '"+ str(partner_name)+"'"
                    query = timestartquery + timeendquery + partner_namequery
                    sql ="SELECT a.id,DATE(a.created_time)'created_time',b.nickname'partner_name',a.pick_isdn,a.otp_receive,a.revenue FROM report_partner a, partner b WHERE a.partner_id = b.id " +query
                    df = pd.read_sql(sql,db_connection)
                    df['created_time'] = df['created_time'].astype('str')
                    json_records = df.to_json(orient ='records')
                    data = []
                    data = json.loads(json_records)
                    sql_total = "SELECT 1'id',count(a.partner_name)'partner_name',sum(a.pick_isdn)'pick_isdn',sum(a.otp_receive)'otp_receive',sum(a.revenue)'revenue' FROM report_partner a, partner b WHERE a.partner_id = b.id "+query
                    df_total = pd.read_sql(sql_total,db_connection)
                    json_records_total = df_total.to_json(orient ='records')
                    data_total = []
                    data_total = json.loads(json_records_total)
                    context = {'data': data,'total':data_total,'code': 'OK'}
                    logging.info(str(session) +"|response|Thành công")
                    return context
                else :
                    logging.error(str(session) +"|response|token hết hạn")
                    return jsonify({'NOTOK': 'token hết hạn','code':401}),401
            else :
                logging.error(str(session) +"|response|Không có quyền")
                return jsonify({'NOTOK': 'khong co quyen'}) 
    except Exception as e:
        logging.error(str(session) + "| error : " + str(traceback.format_exc()))
        return jsonify({'NOTOK': str(e)}),400


@sms_partner.route('/find_report_sms_brand', methods=['GET'])
def find_report_sms_brand():
    session=datetime.datetime.now()
    logging.info(str(session) +"|find_report_sms_brand")
    try :
        db_connection_str = 'mysql+pymysql://crmleeon:BNAie92839470938888@172.17.0.18/gmb_otpgw'
        db_connection = create_engine(db_connection_str)
        data = group()
        if str(data)== "chua truyen token" :
            return jsonify({'NOTOK': str(data)})
        else:
            timetoken = data["expireddate"]
            group_name = data["group_name"]
            nameuser = data["user"]
            logging.info(str(session) +"|find_report_sms_brand|"+str(data))
            if True:
                currentDate = time.strftime("%Y-%m-%d %H:%M:%S")
                print(currentDate)
                if str(timetoken) >= str(currentDate) :
                    starttime = request.args.get('starttime')
                    endtime = request.args.get('endtime')
                    endtime = str(endtime) + " 23:59:59"
                    sender_name = request.args.get('sender_name')
                    if str(starttime) == "" :
                        timestartquery = ""
                    else :
                        timestartquery = " and created_time >= '"+ str(starttime) +"'"
                    if str(endtime) == "" :
                        timeendquery = ""
                    else :
                        timeendquery = " and created_time <= '"+ str(endtime) +"'"
                    if str(sender_name) == "" :
                        sender_namequery = ""
                    else :
                        sender_namequery = " and sender_name = '"+ str(sender_name)+"'"
                    query = "id is not null" +timestartquery + timeendquery + sender_namequery
                    sql ="SELECT id,DATE(created_time)'created_time',sender_name,vendor_sent,sent_telco,sent_partner_otp,revenue FROM report_sender WHERE " +query
                    df = pd.read_sql(sql,db_connection)
                    df['created_time'] = df['created_time'].astype('str')
                    json_records = df.to_json(orient ='records')
                    data = []
                    data = json.loads(json_records)
                    sql_total = "SELECT 1'id',count(sender_name)'sender_name',sum(vendor_sent)'vendor_sent',sum(sent_telco)'sent_telco',sum(sent_partner_otp)'sent_partner_otp',sum(revenue)'revenue' FROM report_sender WHERE "+query
                    df_total = pd.read_sql(sql_total,db_connection)
                    json_records_total = df_total.to_json(orient ='records')
                    data_total = []
                    data_total = json.loads(json_records_total)
                    context = {'data': data,'total':data_total,'code': 'OK'}
                    logging.info(str(session) +"|response|Thành công")
                    return context
                else :
                    logging.error(str(session) +"|response|token hết hạn")
                    return jsonify({'NOTOK': 'token hết hạn','code':401}),401
            else :
                logging.error(str(session) +"|response|Không có quyền")
                return jsonify({'NOTOK': 'khong co quyen'}) 
    except Exception as e:
        logging.error(str(session) + "| error : " + str(traceback.format_exc()))
        return jsonify({'NOTOK': str(e)}),400

@sms_partner.route('/vendor_sender', methods=['GET'])
def vendor_sender():
    session=datetime.datetime.now()
    logging.info(str(session) +"|vendor_sender")
    try :
        db_connection_str = 'mysql+pymysql://crmleeon:BNAie92839470938888@172.17.0.5/gmb_smsgw'
        db_connection = create_engine(db_connection_str)
        data = group()
        if str(data)== "chua truyen token" :
            return jsonify({'NOTOK': str(data)})
        else:
            timetoken = data["expireddate"]
            group_name = data["group_name"]
            nameuser = data["user"]
            logging.info(str(session) +"|vendor_sender|"+str(data))
            if True:
                currentDate = time.strftime("%Y-%m-%d %H:%M:%S")
                print(currentDate)
                if str(timetoken) >= str(currentDate) :
                    sql ="SELECT *,CASE WHEN TYPE =1 THEN 'Y Te, Giao duc' WHEN TYPE =2 THEN 'Dien luc' WHEN TYPE =3 THEN 'Ngan hang' WHEN TYPE =4 THEN 'Tai chinh, Chung khoan' WHEN TYPE =5 THEN 'Thuong mai dien tu' WHEN TYPE =6  THEN 'Hanh chinh cong' WHEN TYPE =7  THEN 'Linh vuc khac' WHEN TYPE = 8  THEN 'Quoc te, OTT, MXH' END AS TYPES  FROM vendor_sender"
                    df = pd.read_sql(sql,db_connection)
                    df['created_time'] = df['created_time'].astype('str')
                    df['updated_time'] = df['updated_time'].astype('str')
                    json_records = df.to_json(orient ='records')
                    data = []
                    data = json.loads(json_records)
                    context = {'data': data,'code': 'OK'}
                    logging.info(str(session) +"|response|Thành công")
                    return context
                else :
                    logging.error(str(session) +"|response|token hết hạn")
                    return jsonify({'NOTOK': 'token hết hạn','code':401}),401
            else :
                logging.error(str(session) +"|response|Không có quyền")
                return jsonify({'NOTOK': 'khong co quyen'}) 
    except Exception as e:
        logging.error(str(session) + "| error : " + str(traceback.format_exc()))
        return jsonify({'NOTOK': str(e)}),400


@sms_partner.route('/list_sender', methods=['GET'])
def list_sender():
    session=datetime.datetime.now()
    logging.info(str(session) +"|vendor_sender")
    try :
        db_connection_str = 'mysql+pymysql://crmleeon:BNAie92839470938888@172.17.0.18/gmb_otpgw'
        db_connection = create_engine(db_connection_str)
        data = group()
        if str(data)== "chua truyen token" :
            return jsonify({'NOTOK': str(data)})
        else:
            timetoken = data["expireddate"]
            group_name = data["group_name"]
            nameuser = data["user"]
            logging.info(str(session) +"|vendor_sender|"+str(data))
            if True:
                currentDate = time.strftime("%Y-%m-%d %H:%M:%S")
                print(currentDate)
                if str(timetoken) >= str(currentDate) :
                    sql ="select sender_name FROM vendor_sender"
                    df = pd.read_sql(sql,db_connection)
                    # df['created_time'] = df['created_time'].astype('str')
                    # df['updated_time'] = df['updated_time'].astype('str')
                    json_records = df.to_json(orient ='records')
                    data = []
                    data = json.loads(json_records)
                    context = {'data': data,'code': 'OK'}
                    logging.info(str(session) +"|response|Thành công")
                    return context
                else :
                    logging.error(str(session) +"|response|token hết hạn")
                    return jsonify({'NOTOK': 'token hết hạn','code':401}),401
            else :
                logging.error(str(session) +"|response|Không có quyền")
                return jsonify({'NOTOK': 'khong co quyen'}) 
    except Exception as e:
        logging.error(str(session) + "| error : " + str(traceback.format_exc()))
        return jsonify({'NOTOK': str(e)}),400

@sms_partner.route('/find_sender_vender', methods=['GET'])
def find_sender_vender():
    session=datetime.datetime.now()
    logging.info(str(session) +"|find_sender_vender")
    try :
        db_connection_str = 'mysql+pymysql://crmleeon:BNAie92839470938888@172.17.0.18/gmb_otpgw'
        db_connection = create_engine(db_connection_str)
        data = group()
        if str(data)== "chua truyen token" :
            return jsonify({'NOTOK': str(data)})
        else:
            timetoken = data["expireddate"]
            group_name = data["group_name"]
            nameuser = data["user"]
            logging.info(str(session) +"|find_sender_vender|"+str(data))
            if True:
                currentDate = time.strftime("%Y-%m-%d %H:%M:%S")
                print(currentDate)
                if str(timetoken) >= str(currentDate) :
                    starttime = request.args.get('starttime')
                    endtime = request.args.get('endtime')
                    sender_name = request.args.get('sender_name')
                    otp = request.args.get('TYPES')
                    if str(starttime) == "" :
                        sqlstarttime = ""
                    else :
                        sqlstarttime = " and created_time >= '"+str(starttime)+"'"
                    if str(endtime) == "" :
                        sqlendtime = ""
                    else :
                        sqlendtime = " and created_time <= '"+str(endtime)+"'"
                    if str(sender_name) == "" :
                        sqlsender_name = ""
                    else :
                        sqlsender_name = " and sender_name = '"+str(sender_name)+"'"
                    if str(otp) == "" :
                        sqlotp = ""
                    else :
                        sqlotp = " and type = '"+str(otp)+"'"
                    sql ="SELECT *,CASE WHEN TYPE =1 THEN 'Y Te, Giao duc' WHEN TYPE =2 THEN 'Dien luc' WHEN TYPE =3 THEN 'Ngan hang' WHEN TYPE =4 THEN 'Tai chinh, Chung khoan' WHEN TYPE =5 THEN 'Thuong mai dien tu' WHEN TYPE =6  THEN 'Hanh chinh cong' WHEN TYPE =7  THEN 'Linh vuc khac' WHEN TYPE = 8  THEN 'Quoc te, OTT, MXH' END AS TYPES  FROM vendor_sender where id is not null" + sqlstarttime+sqlendtime+sqlsender_name+sqlotp
                    print(sql)
                    df = pd.read_sql(sql,db_connection)
                    df['created_time'] = df['created_time'].astype('str')
                    df['updated_time'] = df['updated_time'].astype('str')
                    json_records = df.to_json(orient ='records')
                    data = []
                    data = json.loads(json_records)
                    context = {'data': data,'code': 'OK'}
                    logging.info(str(session) +"|response|Thành công")
                    return context
                else :
                    logging.error(str(session) +"|response|token hết hạn")
                    return jsonify({'NOTOK': 'token hết hạn','code':401}),401
            else :
                logging.error(str(session) +"|response|Không có quyền")
                return jsonify({'NOTOK': 'khong co quyen'}) 
    except Exception as e:
        logging.error(str(session) + "| error : " + str(traceback.format_exc()))
        return jsonify({'NOTOK': str(e)}),400


@sms_partner.route('/insert_sender_vender', methods=['POST'])
def insert_sender_vender():
    session=datetime.datetime.now()
    logging.info(str(session) +"|insert_sender_vender")
    try :
        db_connection_str = 'mysql+pymysql://crmleeon:BNAie92839470938888@172.17.0.18/gmb_otpgw'
        db_connection = create_engine(db_connection_str)
        data = group()
        if str(data)== "chua truyen token" :
            return jsonify({'NOTOK': str(data)})
        else:
            timetoken = data["expireddate"]
            group_name = data["group_name"]
            nameuser = data["user"]
            logging.info(str(session) +"|find_sender_vender|"+str(data))
            if True:
                currentDate = time.strftime("%Y-%m-%d %H:%M:%S")
                print(currentDate)
                if str(timetoken) >= str(currentDate) :
                    record = json.loads(request.data)
                    logging.info(str(session) +"|input|"+str(record))
                    sender_name = record["sender_name"]
                    type = record["type"]
                    gtel = record["gtel"]
                    braxis = record["braxis"]
                    gapit = record["gapit"]
                    fts = record["fts"]
                    vmg = record["vmg"]
                    imedia = record["imedia"]
                    if type == "Y Te, Giao duc" :
                        type = "1"
                    elif type == "Dien luc" :
                        type = "2"
                    elif type == "Ngan hang" :
                        type = "3"
                    elif type == "Tai chinh, Chung khoan" :
                        type = "4"
                    elif type == "Thuong mai dien tu" :
                        type = "5"
                    elif type == "Hanh chinh cong" :
                        type = "6"
                    elif type == "Linh vuc khac" :
                        type = "7"
                    elif type == "Quoc te, OTT, MXH" :
                        type = "8"
                    sql ="insert into vendor_sender (sender_name,type,gtel,braxis,gapit,fts,vmg,imedia) value ('"+str(sender_name)+"','"+str(type)+"','"+str(gtel)+"','"+str(braxis)+"','"+str(gapit)+"','"+str(fts)+"','"+str(vmg)+"','"+str(imedia)+"')"
                    logging.info(str(session) +"|sql|"+str(sql))
                    db_connection.execute(sql)
                    return jsonify({'OK': 'OK'})
                else :
                    logging.error(str(session) +"|response|token hết hạn")
                    return jsonify({'NOTOK': 'token hết hạn','code':401}),401
            else :
                logging.error(str(session) +"|response|Không có quyền")
                return jsonify({'NOTOK': 'khong co quyen'}) 
    except Exception as e:
        logging.error(str(session) + "| error : " + str(traceback.format_exc()))
        return jsonify({'NOTOK': str(e)}),400

@sms_partner.route('/update_sender_vender', methods=['POST'])
def update_sender_vender():
    session=datetime.datetime.now()
    logging.info(str(session) +"|insert_sender_vender")
    try :
        db_connection_str = 'mysql+pymysql://crmleeon:BNAie92839470938888@172.17.0.18/gmb_otpgw'
        db_connection = create_engine(db_connection_str)
        data = group()
        if str(data)== "chua truyen token" :
            return jsonify({'NOTOK': str(data)})
        else:
            timetoken = data["expireddate"]
            group_name = data["group_name"]
            nameuser = data["user"]
            logging.info(str(session) +"|find_sender_vender|"+str(data))
            if True:
                currentDate = time.strftime("%Y-%m-%d %H:%M:%S")
                print(currentDate)
                if str(timetoken) >= str(currentDate) :
                    record = json.loads(request.data)
                    logging.info(str(session) +"|input|"+str(record))
                    id = record["id"]
                    sender_name = record["sender_name"]
                    type = record["type"]
                    gtel = record["gtel"]
                    braxis = record["braxis"]
                    gapit = record["gapit"]
                    fts = record["fts"]
                    vmg = record["vmg"]
                    imedia = record["imedia"]
                    if type == "Y Te, Giao duc" :
                        type = "1"
                    elif type == "Dien luc" :
                        type = "2"
                    elif type == "Ngan hang" :
                        type = "3"
                    elif type == "Tai chinh, Chung khoan" :
                        type = "4"
                    elif type == "Thuong mai dien tu" :
                        type = "5"
                    elif type == "Hanh chinh cong" :
                        type = "6"
                    elif type == "Linh vuc khac" :
                        type = "7"
                    elif type == "Quoc te, OTT, MXH" :
                        type = "8"
                    sql ="update vendor_sender set sender_name = '"+str(sender_name)+"',type ='"+str(type)+"',gtel='"+str(gtel)+"',braxis='"+str(braxis)+"',gapit='"+str(gapit)+"',fts='"+str(fts)+"',vmg ='"+str(vmg)+"',imedia ='"+str(imedia)+"' where id = "+str(id)
                    logging.info(str(session) +"|sql|"+str(sql))
                    db_connection.execute(sql)
                    return jsonify({'OK': 'OK'})
                else :
                    logging.error(str(session) +"|response|token hết hạn")
                    return jsonify({'NOTOK': 'token hết hạn','code':401}),401
            else :
                logging.error(str(session) +"|response|Không có quyền")
                return jsonify({'NOTOK': 'khong co quyen'}) 
    except Exception as e:
        logging.error(str(session) + "| error : " + str(traceback.format_exc()))
        return jsonify({'NOTOK': str(e)}),400

@sms_partner.route('/top_revenue_vendor', methods=['GET'])
def top_revenue_vendor():
    session=datetime.datetime.now()
    logging.info(str(session) +"|top_revenue_vendor")
    try :
        db_connection_str = 'mysql+pymysql://crmleeon:BNAie92839470938888@172.17.0.18/gmb_otpgw'
        db_connection = create_engine(db_connection_str)
        data = group()
        if str(data)== "chua truyen token" :
            return jsonify({'NOTOK': str(data)})
        else:
            timetoken = data["expireddate"]
            group_name = data["group_name"]
            nameuser = data["user"]
            logging.info(str(session) +"|top_revenue_vendor|"+str(data))
            if True:
                currentDate = time.strftime("%Y-%m-%d %H:%M:%S")
                print(currentDate)
                if str(timetoken) >= str(currentDate) :
                    sql ="SELECT SUM(revenue)'revenue',LEFT(created_time,10)'created_time' FROM report_vendor WHERE created_time >= SUBDATE(CURRENT_DATE, 30) GROUP BY LEFT(created_time,10) ORDER BY  created_time DESC"
                    df = pd.read_sql(sql,db_connection)
                    df['created_time'] = df['created_time'].astype('str')
                    json_records = df.to_json(orient ='records')
                    data = []
                    data = json.loads(json_records)
                    context = {'data': data,'code': 'OK'}
                    logging.info(str(session) +"|response|Thành công")
                    return context
                else :
                    logging.error(str(session) +"|response|token hết hạn")
                    return jsonify({'NOTOK': 'token hết hạn','code':401}),401
            else :
                logging.error(str(session) +"|response|Không có quyền")
                return jsonify({'NOTOK': 'khong co quyen'}) 
    except Exception as e:
        logging.error(str(session) + "| error : " + str(traceback.format_exc()))
        return jsonify({'NOTOK': str(e)}),400

@sms_partner.route('/top_revenue_partner', methods=['GET'])
def top_revenue_partner():
    session=datetime.datetime.now()
    logging.info(str(session) +"|top_revenue_vendor")
    try :
        db_connection_str = 'mysql+pymysql://crmleeon:BNAie92839470938888@172.17.0.18/gmb_otpgw'
        db_connection = create_engine(db_connection_str)
        data = group()
        if str(data)== "chua truyen token" :
            return jsonify({'NOTOK': str(data)})
        else:
            timetoken = data["expireddate"]
            group_name = data["group_name"]
            nameuser = data["user"]
            logging.info(str(session) +"|top_revenue_vendor|"+str(data))
            if True:
                currentDate = time.strftime("%Y-%m-%d %H:%M:%S")
                print(currentDate)
                if str(timetoken) >= str(currentDate) :
                    sql ="SELECT SUM(revenue)'revenue',LEFT(created_time,10)'created_time' FROM report_partner WHERE created_time >= SUBDATE(CURRENT_DATE, 30) GROUP BY LEFT(created_time,10) ORDER BY  created_time DESC"
                    df = pd.read_sql(sql,db_connection)
                    df['created_time'] = df['created_time'].astype('str')
                    json_records = df.to_json(orient ='records')
                    data = []
                    data = json.loads(json_records)
                    context = {'data': data,'code': 'OK'}
                    logging.info(str(session) +"|response|Thành công")
                    return context
                else :
                    logging.error(str(session) +"|response|token hết hạn")
                    return jsonify({'NOTOK': 'token hết hạn','code':401}),401
            else :
                logging.error(str(session) +"|response|Không có quyền")
                return jsonify({'NOTOK': 'khong co quyen'}) 
    except Exception as e:
        logging.error(str(session) + "| error : " + str(traceback.format_exc()))
        return jsonify({'NOTOK': str(e)}),400



@sms_partner.route('/top_sms_brand', methods=['GET'])
def top_sms_brand():
    session=datetime.datetime.now()
    logging.info(str(session) +"|report_sms_brand")
    try :
        db_connection_str = 'mysql+pymysql://crmleeon:BNAie92839470938888@172.17.0.18/gmb_otpgw'
        db_connection = create_engine(db_connection_str)
        data = group()
        if str(data)== "chua truyen token" :
            return jsonify({'NOTOK': str(data)})
        else:
            timetoken = data["expireddate"]
            group_name = data["group_name"]
            nameuser = data["user"]
            logging.info(str(session) +"|report_sms_brand|"+str(data))
            if True:
                currentDate = time.strftime("%Y-%m-%d %H:%M:%S")
                print(currentDate)
                if str(timetoken) >= str(currentDate) :
                    sql ="SELECT sender_name,SUM(revenue)'revenues' FROM report_sender WHERE DATE(created_time)>=SUBDATE(CURRENT_DATE, 7) GROUP BY sender_name ORDER BY revenues DESC LIMIT 10"
                    df = pd.read_sql(sql,db_connection)
                    #column index +1
                    df.index = df.index + 1
                    #rename column index to id
                    df.index.name = 'id'
                    df.index.name = 'ids'
                    json_records = df.reset_index().to_json(orient ='records')
                    data = []
                    data = json.loads(json_records)
                    context = {'data': data,'code': 'OK'}
                    logging.info(str(session) +"|response|Thành công")
                    return context
                else :
                    logging.error(str(session) +"|response|token hết hạn")
                    return jsonify({'NOTOK': 'token hết hạn','code':401}),401
            else :
                logging.error(str(session) +"|response|Không có quyền")
                return jsonify({'NOTOK': 'khong co quyen'}) 
    except Exception as e:
        logging.error(str(session) + "| error : " + str(traceback.format_exc()))
        return jsonify({'NOTOK': str(e)}),400