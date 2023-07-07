from flask import Blueprint
from flask import request, jsonify
import json
import requests
from requests.auth import HTTPDigestAuth
import pandas as pd
import numpy as np
from pandas import json_normalize
import pymysql
from flask_jsonpify import jsonpify
from datetime import datetime,date, timedelta
from sqlalchemy import create_engine,text
import datetime
import time
import ast
import logging
import traceback
import configparser
from function import group
service_config = Blueprint('service_config', __name__)
config = configparser.ConfigParser()
config.read('config.conf')
dbconfig=config['db_config']['db_config']
@service_config.route('/service_config_list', methods=['GET'])
def service_config_list():
    session=datetime.datetime.now()
    logging.info(str(session) +"|service_config_list")
    try :
        db_connection = create_engine(dbconfig)
        data = group()
        if str(data)== "chua truyen token" :
            return jsonify({'NOTOK': str(data)})
        else:
            timetoken = data["expireddate"]
            group_name = data["group_name"]
            nameuser = data["user"]
            logging.info(str(session) +"|service_config_list|"+str(data))
            if True:
                currentDate = time.strftime("%Y-%m-%d %H:%M:%S")
                print(currentDate)
                if str(timetoken) >= str(currentDate) :
                    sql ="SELECT a.id,a.info 'test',left(a.createAt,10)'createAt',b.nickname,c.packetName,d.callType,a.price,a.startDuration,a.endDuration, case when a.isBrand=0 then 'Brand' else 'Sip Trunk' end as isBrand from service_config a, partner b, service_packet c, service_type d where a.partnerCode = b.partner_code and a.packetId = c.id and a.callTypeId = d.id order by a.createAt desc"
                    df = pd.read_sql(sql,db_connection)
                    df['createAt'] = df['createAt'].astype('str')
                    df['test'] = "["+df['test']+"]"
                    df['test'] = df['test'].apply(ast.literal_eval)
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

@service_config.route('/insert_service_config', methods=['POST'])
def insert_service_config():
    session=datetime.datetime.now()
    logging.info(str(session) +"|insert_sender_vender")
    try :
        db_connection = create_engine(dbconfig)
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
                    info = record["test"]
                    for i in range(len(info)):
                        nickname = info[i]["nickname"]
                        packetName = info[i]["packetName"]
                        callType = info[i]["callType"]
                        price = info[i]["price"]
                        startDuration = info[i]["startDuration"]
                        endDuration = info[i]["endDuration"]
                        isBrand = info[i]["isBrand"]
                        if str(isBrand) == "Brand":
                          isBrand = 0
                        else:
                          isBrand = 1
                        sql ="insert into service_config (partnerCode,packetId,callTypeId,price,startDuration,endDuration,isBrand,info) value ((select partner_code from partner where nickname = '"+str(nickname)+"'),(select id from service_packet where packetName = '"+str(packetName)+"'),(select id from service_type where callType = '"+str(callType)+"'),'"+str(price)+"','"+str(startDuration)+"','"+str(endDuration)+"', '"+str(isBrand)+"', \""+str(info[i])+"\")"
                        sql=text(sql)
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


@service_config.route('/service_type_list', methods=['GET'])
def service_type_list():
    session=datetime.datetime.now()
    logging.info(str(session) +"|service_type_list")
    try :
        db_connection = create_engine(dbconfig)
        data = group()
        if str(data)== "chua truyen token" :
            return jsonify({'NOTOK': str(data)})
        else:
            timetoken = data["expireddate"]
            group_name = data["group_name"]
            nameuser = data["user"]
            logging.info(str(session) +"|service_type_list|"+str(data))
            if True:
                currentDate = time.strftime("%Y-%m-%d %H:%M:%S")
                print(currentDate)
                if str(timetoken) >= str(currentDate) :
                    sql ="SELECT * from service_type"
                    df = pd.read_sql(sql,db_connection)
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

@service_config.route('/delete_service_config', methods=['DELETE'])
def delete_service_config():
    try :
        session=datetime.datetime.now()
        engine = create_engine(dbconfig)
        data = group()
        if str(data)== "chua truyen token" :
            return jsonify({'NOTOK': str(data)})
        else:
            timetoken = data["expireddate"]
            group_name = data["group_name"]
            nameuser = data["user"]
            if True:
                currentDate = time.strftime("%Y-%m-%d %H:%M:%S")
                if str(timetoken) >= str(currentDate) :
                    record = json.loads(request.data)
                    id = record["id"]
                    sqlcheck= "select * from service_config where id ='"+str(id)+"'"
                    with engine.connect() as con:
                        df = pd.read_sql(sqlcheck,con)
                        con.close()
                    if df.empty:
                        return jsonify({'OK': 'id không tồn tại'})
                    sql="delete from service_config where id = '"+str(id)+"'"
                    sqllog = "insert into log (name,api,content,type) value ('"+str(nameuser)+"','delete_service_config','id = "+str(id)+"','DELETE')"
                    with engine.connect() as con:
                        con.execute(sql)
                        con.execute(sqllog)
                        con.close()
                    logging.info(str(session) +"|response|Thành công")
                    engine.dispose()
                    return jsonify({'OK': 'OK'})
                else :
                    logging.error(str(session) +"|response|token hết hạn")
                    return jsonify({'NOTOK': 'token hết hạn'})
            else :
                logging.error(str(session) +"|response|Không có quyền")
                return jsonify({'NOTOK': 'khong co quyen'})
    except Exception as e:
        logging.error(str(session) +"|delete_service_config error |"+str(traceback.format_exc()))
        return jsonify({'Lỗi': str(traceback.format_exc())})

@service_config.route('/update_service_config', methods=['POST'])
def update_service_config():
    session=datetime.datetime.now()
    logging.info(str(session) +"|update_service_config")
    try :
        db_connection = create_engine(dbconfig)
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
                    id = record["id"]
                    logging.info(str(session) +"|input|"+str(record))
                    info = record["test"]
                    for i in range(len(info)):
                        nickname = info[i]["nickname"]
                        packetName = info[i]["packetName"]
                        callType = info[i]["callType"]
                        price = info[i]["price"]
                        startDuration = info[i]["startDuration"]
                        endDuration = info[i]["endDuration"]
                        isBrand = info[i]["isBrand"]
                        if str(isBrand) == "Brand":
                          isBrand = 0
                        else:
                          isBrand = 1
                    # current_time = now.strftime("%Y-%m-%d %H:%M:%S")
                    info = str(info).replace("[","").replace("]","")
                    sql="update service_config set partnerCode = (select partner_code from partner where nickname = '"+str(nickname)+"'), packetId =(select id from service_packet where packetName= '"+str(packetName)+"'),callTypeId =(select id from service_type where callType= '"+str(callType)+"'), price = '"+str(price)+"',startDuration ='"+str(startDuration)+"', endDuration ='"+str(endDuration)+"',isBrand='"+str(isBrand)+"',info=\""+str(info)+"\" where id ='"+str(id)+"'"
                    logging.info(str(session) +"|sql|"+str(sql))
                    db_connection.execute(sql)
                    logging.info(str(session) +"| update ok")
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

@service_config.route('/find_service_config', methods=['GET'])
def find_service_config():
    session=datetime.datetime.now()
    logging.info(str(session) +"|update_service_config")
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
            logging.info(str(session) +"|find_service_config|"+str(data))
            if True:
                currentDate = time.strftime("%Y-%m-%d %H:%M:%S")
                print(currentDate)
                if str(timetoken) >= str(currentDate) :
                    logging.info(str(session) +"|input|"+str(request.args))
                    callType = request.args.get('callType')
                    isBrand = request.args.get('isBrand')
                    nickname = request.args.get('nickname')
                    packet = request.args.get('packetName')
                    sql_partner_code = ""
                    if str(nickname) != "":
                        sqlcheck_partner = "select partner_code from partner where nickname = '"+str(nickname)+"'"
                        df_check_partner = pd.read_sql(sqlcheck_partner,db)
                        partner_code = df_check_partner.iloc[0]['partner_code']
                        sql_partner_code = " and a.partnerCode = '"+str(partner_code)+"'"
                    sql_packet = ""
                    if str(packet) != "":
                        take_packet = "select id from service_packet where packetName = '"+str(packet)+"'"
                        df_take_packet = pd.read_sql(take_packet,db)
                        if df_take_packet.empty:
                            sql_packet = ""
                        else:
                            packet_id = df_take_packet.iloc[0]['id']
                            sql_packet = " and a.packetId = '"+str(packet_id)+"'"
                    sql_callType =""
                    if str(callType) != "" and str(callType) != "undefined":
                        sql_callType = "select id from service_type where callType = '"+str(callType)+"'"
                        df_callType = pd.read_sql(sql_callType,db)
                        if df_callType.empty:
                            return jsonify({'NOTOK': 'Không tìm thấy callType'}),400
                        callTypeId = df_callType.iloc[0]['id']
                        sql_callType = " and a.callTypeId = '"+str(callTypeId)+"'"
                    sql_isBrand = ""
                    if str(isBrand) == "Sip Trunk":
                        sql_isBrand = " and a.isBrand = 1"
                    elif str(isBrand) == "Brand":
                        sql_isBrand = " and a.isBrand = 0"
                    sql ="SELECT a.id,a.info 'test',left(a.createAt,10)'createAt',b.nickname,c.packetName,d.callType,a.price,a.startDuration,a.endDuration, case when a.isBrand=0 then 'Brand' else 'Sip Trunk' end as isBrand from service_config a, partner b, service_packet c, service_type d where a.partnerCode = b.partner_code and a.packetId = c.id and a.callTypeId = d.id "+sql_isBrand+sql_callType+sql_partner_code+sql_packet+" order by a.createAt desc"
                    logging.info(str(session) +"|sql_select|"+str(sql))
                    df = pd.read_sql(sql,db)
                    df['createAt'] = df['createAt'].astype('str')
                    df['test'] = "["+df['test']+"]"
                    df['test'] = df['test'].apply(ast.literal_eval)
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
        return jsonify({'NOTOK': str(traceback.format_exc())}),400
#Packet
@service_config.route('/service_packet', methods=['GET'])
def service_packet():
    session=datetime.datetime.now()
    logging.info(str(session) +"|service_packet")
    try :
        db_connection = create_engine(dbconfig)
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
                    sql ="SELECT a.id, a.createdAt, a.packetName,a.description, case when a.status=0 then 'Tạm dừng' else 'Hoạt động' end as status, case when a.blockType =0 then '1s + 1' when a.blockType=1 then '6s + 1' when a.blockType=2 then '1ph + 1ph' else '15s + 15' end as blockType from service_packet a"
                    df = pd.read_sql(sql,db_connection)
                    df['createdAt'] = df['createdAt'].astype('str')
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

@service_config.route('/insert_service_packet', methods=['POST'])
def insert_service_packet():
    session=datetime.datetime.now()
    logging.info(str(session) +"|insert_service_packet")
    try :
        db_connection = create_engine(dbconfig)
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
                    packetName = record["packetName"]
                    description = record["description"]
                    status = record["status"]
                    blockType = record["blockType"]
                    if str(status) == "Hoạt động":
                      status =1
                    else :
                      status =0
                    if blockType == "1s + 1":
                      blockType =0
                    elif blockType == "6s + 1":
                      blockType =1
                    elif blockType == "1ph + 1ph":
                      blockType =2
                    elif blockType == "15s + 15":
                      blockType =3
                    sql ="insert into service_packet (packetName,description,blockType,status) value ('"+str(packetName)+"','"+str(description)+"','"+str(blockType)+"','"+str(status)+"')"
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

#delete packet
@service_config.route('/delete_service_packet', methods=['DELETE'])
def delete_service_packet():
    try :
        session=datetime.datetime.now()
        engine = create_engine(dbconfig)
        data = group()
        if str(data)== "chua truyen token" :
            return jsonify({'NOTOK': str(data)})
        else:
            timetoken = data["expireddate"]
            group_name = data["group_name"]
            nameuser = data["user"]
            if True:
                currentDate = time.strftime("%Y-%m-%d %H:%M:%S")
                if str(timetoken) >= str(currentDate) :
                    record = json.loads(request.data)
                    id = record["id"]
                    sqlcheck= "select * from service_packet where id ='"+str(id)+"'"
                    with engine.connect() as con:
                        df = pd.read_sql(sqlcheck,con)
                        con.close()
                    if df.empty:
                        return jsonify({'OK': 'id không tồn tại'})
                    sql="delete from service_packet where id = '"+str(id)+"'"
                    sqllog = "insert into log (name,api,content,type) value ('"+str(nameuser)+"','delete_service_config','id = "+str(id)+"','DELETE')"
                    with engine.connect() as con:
                        con.execute(sql)
                        con.execute(sqllog)
                        con.close()
                    logging.info(str(session) +"|response|Thành công")
                    engine.dispose()
                    return jsonify({'OK': 'OK'})
                else :
                    logging.error(str(session) +"|response|token hết hạn")
                    return jsonify({'NOTOK': 'token hết hạn'})
            else :
                logging.error(str(session) +"|response|Không có quyền")
                return jsonify({'NOTOK': 'khong co quyen'})
    except Exception as e:
        logging.error(str(session) +"|delete_service_config error |"+str(traceback.format_exc()))
        return jsonify({'Lỗi': str(traceback.format_exc())})


#update packet
@service_config.route('/update_service_packet', methods=['POST'])
def update_service_packet():
    session=datetime.datetime.now()
    logging.info(str(session) +"|update_service_packet")
    try :
        db_connection = create_engine(dbconfig)
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
                    print(record)
                    id = record["id"]
                    packetName = record["packetName"]
                    description = record["description"]
                    status = record["status"]
                    blockType = record["blockType"]
                    if str(status) == "Hoạt động":
                      status =1
                    else :
                      status =0
                    if blockType == "1s + 1":
                      blockType =0
                    elif blockType == "6s + 1":
                      blockType =1
                    elif blockType == "1ph + 1ph":
                      blockType =2
                    elif blockType == "15s + 15":
                      blockType =3
                    # current_time = now.strftime("%Y-%m-%d %H:%M:%S")
                    sql="update service_packet set packetName = '"+str(packetName)+"',description  = '"+str(description )+"',status  = '"+str(status)+"',blockType='"+str(blockType)+"' where id ='"+str(id)+"'"
                    logging.info(str(session) +"|sql|"+str(sql))
                    db_connection.execute(sql)
                    logging.info(str(session) +"| update ok")
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

@service_config.route('/saovang_service_config_list', methods=['GET'])
def saovang_service_config_list():
    session=datetime.datetime.now()
    logging.info(str(session) +"|saovang_service_config_list")
    try :
        db_connection = create_engine(dbconfig)
        data = group()
        if str(data)== "chua truyen token" :
            return jsonify({'NOTOK': str(data)})
        else:
            timetoken = data["expireddate"]
            group_name = data["group_name"]
            nameuser = data["user"]
            logging.info(str(session) +"|service_config_list|"+str(data))
            if True:
                currentDate = time.strftime("%Y-%m-%d %H:%M:%S")
                print(currentDate)
                if str(timetoken) >= str(currentDate) :
                    sql ="SELECT a.id,a.info 'test',left(a.createAt,10)'createAt',b.nickname,c.packetName,d.callType,a.price,a.startDuration,a.endDuration, case when a.isBrand=0 then 'Brand' else 'Sip Trunk' end as isBrand from service_config a, partner b, service_packet c, service_type d where b.agency_id =1 AND a.partnerCode = b.partner_code and a.packetId = c.id and a.callTypeId = d.id order by a.createAt desc"
                    df = pd.read_sql(sql,db_connection)
                    df['createAt'] = df['createAt'].astype('str')
                    df['test'] = "["+df['test']+"]"
                    df['test'] = df['test'].apply(ast.literal_eval)
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