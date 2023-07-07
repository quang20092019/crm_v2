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
sms_brandname = Blueprint('sms_brandname', __name__)
@sms_brandname.route('/customer_sms_brandname', methods=['GET'])
def customer_sms_brandname():
    session=datetime.datetime.now()
    logging.info(str(session) +"|report_sms_vendor")
    try :
        db_connection_str = 'mysql+pymysql://crmleeon:BNAie92839470938888@172.17.0.5/sms_brandname'
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
                    sql ="SELECT * from customer"
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
@sms_brandname.route('/insert_customer_sms_brandname', methods=['POST'])
def insert_customer_sms_brandname():
    session=datetime.datetime.now()
    logging.info(str(session) +"|insert_sender_vender")
    try :
        db_connection_str = 'mysql+pymysql://crmleeon:BNAie92839470938888@172.17.0.5/sms_brandname'
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
                    customer_name = record["customer_name"]
                    customer_code = record["customer_code"]
                    address = record["address"]
                    phone = record["phone"]
                    email = record["email"]
                    token = record["token"]
                    sql ="insert into customer (customer_name,customer_code,address,phone,email,token) value ('"+str(customer_name)+"','"+str(customer_code)+"','"+str(address)+"','"+str(phone)+"','"+str(email)+"','"+str(token)+"')"
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

@sms_brandname.route('/update_smsbrandname', methods=['POST'])
def update_smsbrandname():
    session=datetime.datetime.now()
    logging.info(str(session) +"|insert_sender_vender")
    try :
        db_connection_str = 'mysql+pymysql://crmleeon:BNAie92839470938888@172.17.0.5/sms_brandname'
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
                    print(record)
                    id = record["id"]
                    customer_name = record["customer_name"]
                    customer_code = record["customer_code"]
                    address = record["address"]
                    phone = record["phone"]
                    email = record["email"]
                    token = record["token"]
                    # current_time = now.strftime("%Y-%m-%d %H:%M:%S")
                    sql="update customer set customer_name = '"+str(customer_name)+"', customer_code = '"+str(customer_code)+"',address   = '"+str(address  )+"', phone = '"+str(phone)+"',email ='"+str(email)+"', token ='"+str(token)+"' where id ='"+str(id)+"'"
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

@sms_brandname.route('/topup_customer_sms_brandname', methods=['POST'])
def topup_customer_sms_brandname():
    session=datetime.datetime.now()
    logging.info(str(session) +"|topup_customer_sms_brandname")
    try :
        db_connection_str = 'mysql+pymysql://crmleeon:BNAie92839470938888@172.17.0.5/sms_brandname'
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
                    customer_code = record["customer_code"]
                    balance = record["balance"]
                    #check customer_code
                    sql_check_customer_code ="select * from customer where customer_code = '"+str(customer_code)+"'"
                    logging.info(str(session) +"|sql_check_customer_code|"+str(sql_check_customer_code))
                    df = pd.read_sql(sql_check_customer_code,db_connection)
                    if df.empty:
                        return jsonify({'NOTOK': 'customer_code không tồn tại'})
                    sql ="update customer set balance_limit = balance_limit + "+str(balance)+" where customer_code = '"+str(customer_code)+"'"
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


#ngoc làm
@sms_brandname.route('/customer_service_sms_brand', methods=['GET'])
def customer_service_sms_brand():
    session=datetime.datetime.now()
    logging.info(str(session) +"|report_sms_vendor")
    try :
        db_connection_str = 'mysql+pymysql://crmleeon:BNAie92839470938888@172.17.0.5/sms_brandname'
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
                    sql ="SELECT b.id,a.customer_code,b.brand, c.name as brand_type,case when b.service_type =0 then 'Quảng cáo' else  'CSKH' end as service_type,b.price, case when b.status =0 then 'Tạm dừng' else  'Hoạt động' end as status FROM customer a, customer_brand b, brand_type c where a. id = b.customer_id and b.brand_type = c.id"
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


@sms_brandname.route('/insert_customer_service_smsbrand', methods=['POST'])
def insert_customer_service_smsbrand():
    session=datetime.datetime.now()
    logging.info(str(session) +"|insert_sender_vender")
    try :
        db_connection_str = 'mysql+pymysql://crmleeon:BNAie92839470938888@172.17.0.5/sms_brandname'
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
                    customer_code = record["customer_code"]
                    brand = record["brand"]
                    service_type = record["service_type"]
                    brand_type = record["brand_type"]
                    price = record["price"]
                    status = record["status"]
                    if str(status) == "Hoạt động":
                      status =1
                    else :
                      status =0
                    if str(service_type) == "CSKH":
                      service_type =1
                    else :
                      service_type =0
                    if brand_type == "Y Te, Giao duc" :
                        brand_type = "1"
                    elif brand_type == "Dien luc" :
                        brand_type = "2"
                    elif brand_type == "Ngan hang" :
                        brand_type = "3"
                    elif brand_type == "Tai chinh, Chung khoan" :
                        brand_type = "4"
                    elif brand_type == "Thuong mai dien tu" :
                        brand_type = "5"
                    elif brand_type == "Hanh chinh cong" :
                        brand_type = "6"
                    elif brand_type == "Linh vuc khac" :
                        brand_type = "7"
                    elif brand_type == "Quoc te, OTT, MXH" :
                        brand_type = "8"
                    sql ="insert into customer_brand (customer_id,service_type,brand,brand_type,price,status) value ((select id from customer where customer_code = '"+str(customer_code)+"' ),'"+str(service_type)+"','"+str(brand)+"' , '"+str(brand_type)+"','"+str(price)+"','"+str(status)+"')"
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

@sms_brandname.route('/update_customerservice_brand', methods=['POST'])
def update_customerservice_brand():
    session=datetime.datetime.now()
    logging.info(str(session) +"|insert_sender_vender")
    try :
        db_connection_str = 'mysql+pymysql://crmleeon:BNAie92839470938888@172.17.0.5/sms_brandname'
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
                    print(record)
                    id = record["id"]
                    customer_code = record["customer_code"]
                    service_type = record["service_type"]
                    brand_type = record["brand_type"]
                    brand = record["brand"]
                    price = record["price"]
                    status = record["status"]
                    if str(status) == "Hoạt động":
                      status =1
                    else :
                      status =0
                    if str(service_type) == "CSKH":
                      service_type =1
                    else :
                      service_type =0
                    if brand_type == "Y Te, Giao duc" :
                        brand_type = "1"
                    elif brand_type == "Dien luc" :
                        brand_type = "2"
                    elif brand_type == "Ngan hang" :
                        brand_type = "3"
                    elif brand_type == "Tai chinh, Chung khoan" :
                        brand_type = "4"
                    elif brand_type == "Thuong mai dien tu" :
                        brand_type = "5"
                    elif brand_type == "Hanh chinh cong" :
                        brand_type = "6"
                    elif brand_type == "Linh vuc khac" :
                        brand_type = "7"
                    elif brand_type == "Quoc te, OTT, MXH" :
                        brand_type = "8"
                    # current_time = now.strftime("%Y-%m-%d %H:%M:%S")
                    sql="update customer_brand set customer_id = (select id from customer where customer_code = '"+str(customer_code)+"'), brand ='"+str(brand)+"', brand_type = '"+str(brand_type)+"', service_type  = '"+str(service_type)+"',price  = '"+str(price )+"',status='"+str(status)+"' where id ='"+str(id)+"'"
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


# @sms_adv.route('/deletesmsadv', methods=['DELETE'])
# def deletesmsadv():
#     session=datetime.datetime.now()
#     logging.info(str(session) +"|deletesmsadv")
#     try :
#         # db_connection_str = 'mysql+pymysql://gmbsmsadv:AKJshd019203910288@127.0.0.1/gmb_smsadv'
#         # db_connection = create_engine(db_connection_str)
#         db = pymysql.connect(host="127.0.0.1",    # your host, usually localhost
#                      user="gmbsmsadv",         # your username
#                      passwd="AKJshd019203910288",  # your password
#                      db="gmb_smsadv",
#                      port=3306
#                      )
#         data = group()
#         if str(data)== "chua truyen token" :
#             return jsonify({'NOTOK': str(data)})
#         else:
#             timetoken = data["expireddate"]
#             group_name = data["group_name"]
#             nameuser = data["user"]
#             logging.info(str(session) +"|deletesmsadv|"+str(data))
#             if True:
#                 currentDate = time.strftime("%Y-%m-%d %H:%M:%S")
#                 print(currentDate)
#                 if str(timetoken) >= str(currentDate) :
#                     record = json.loads(request.data)
#                     print(record)
#                     id = record["id"]
#                     logging.info(str(session) +"|delete id|"+str(id))
#                     sqlcontent= "select * from customer_service where id = '"+str(id)+"'"
#                     dfcontent = pd.read_sql(sqlcontent,db)
#                     if not dfcontent.empty:
#                         dfcontent=dfcontent.values.tolist()
#                         logging.info(str(session) +"|content|"+str(dfcontent))
#                         sql="delete from customer_service where id = '"+str(id)+"'"
#                         cursor = db.cursor()
#                         cursor.execute(sql)
#                         # sqllog = "insert into log (name,api,content,type) value ('"+str(nameuser)+"','deletebsmsadv','"+str(dfcontent).replace("'","")+"','DELETE')"
#                         # cursor = db.cursor()
#                         # cursor.execute(sqllog)
#                         db.commit()
#                         db.close()
#                         logging.info(str(session) +"|response|Thành công")
#                         return jsonify({'OK': 'OK'})
#                     else :
#                         db.close()
#                         logging.error(str(session) +"|response|id không tồn tại")
#                         return jsonify({'OK': 'id không tồn tại'})
#                 else :
#                     db.close()
#                     logging.error(str(session) +"|response|token hết hạn")
#                     return jsonify({'NOTOK': 'token hết hạn','code':401}),401
#             else :
#                 db.close()
#                 logging.error(str(session) +"|response|Không có quyền")
#                 return jsonify({'NOTOK': 'khong co quyen'})   
#     except Exception as e:
#         db.close()
#         logging.error(str(session) +"|response|" +str(traceback.format_exc()))
#         return jsonify({'NOTOK': str(e)}),400

#API get telco
@sms_brandname.route('/get_telco_brand', methods=['GET'])
def get_telco_brand():
    session=datetime.datetime.now()
    logging.info(str(session) +"|report_sms_vendor")
    try :
        db_connection_str = 'mysql+pymysql://crmleeon:BNAie92839470938888@172.17.0.5/sms_brandname'
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
                    sql ="SELECT * from telco"
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



# API Mesage template
@sms_brandname.route('/message_template_brand', methods=['GET'])
def message_template_brand():
    session=datetime.datetime.now()
    logging.info(str(session) +"|report_sms_vendor")
    try :
        db_connection_str = 'mysql+pymysql://crmleeon:BNAie92839470938888@172.17.0.5/sms_brandname'
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
                    sql ="SELECT b.id,a.customer_name,b.message, b.service_type,c.telco_name as telco, case when b.status =0 then 'Tạm dừng' else  'Hoạt động' end as status FROM customer a, message_template b, telco c where a.id = b.customer_id and b.telco_id = c.id"
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


@sms_brandname.route('/insert_message_template_brand', methods=['POST'])
def insert_message_template_brand():
    session=datetime.datetime.now()
    logging.info(str(session) +"|insert_message_template_brand")
    try :
        db_connection_str = 'mysql+pymysql://crmleeon:BNAie92839470938888@172.17.0.5/sms_brandname'
        db_connection = create_engine(db_connection_str)
        data = group()
        if str(data)== "chua truyen token" :
            return jsonify({'NOTOK': str(data)})
        else:
            timetoken = data["expireddate"]
            group_name = data["group_name"]
            nameuser = data["user"]
            logging.info(str(session) +"|insert_message_template|"+str(data))
            if True:
                currentDate = time.strftime("%Y-%m-%d %H:%M:%S")
                print(currentDate)
                if str(timetoken) >= str(currentDate) :
                    record = json.loads(request.data)
                    logging.info(str(session) +"|input|"+str(record))
                    customer_name = record["customer_name"]
                    message = record["message"]
                    service_type = record["service_type"]
                    telco = record["telco"]
                    status = record["status"]
                    if str(status) == "Hoạt động":
                      status =1
                    else :
                      status =0
                    if telco == "Mobifone" :
                        telco = "1"
                    elif telco == "Vinaphone" :
                        telco = "2"
                    elif telco == "Viettel" :
                        telco = "4"
                    elif telco == "Vietnamobile" :
                        telco = "5"
                    elif telco == "Gmobile" :
                        telco = "7"
                    elif telco == "Indochina" :
                        telco = "8"
                    sql ="insert into message_template (message,customer_id,service_type,telco_id,status) value ('"+str(message)+"',(select id from customer where customer_name = '"+str(customer_name)+"' ),'"+str(service_type)+"',(select id from telco where id ='"+str(telco)+"'),'"+str(status)+"')"
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

@sms_brandname.route('/update_messagetemplate_brand', methods=['POST'])
def update_messagetemplate():
    session=datetime.datetime.now()
    logging.info(str(session) +"|update_messagetemplate")
    try :
        db_connection_str = 'mysql+pymysql://crmleeon:BNAie92839470938888@172.17.0.5/sms_brandname'
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
                    print(record)
                    id = record["id"]
                    customer_name = record["customer_name"]
                    service_type = record["service_type"]
                    message = record["message"]
                    telco = record["telco"]
                    status = record["status"]
                    if str(status) == "Hoạt động":
                      status =1
                    else :
                      status =0
                    if telco == "Mobifone" :
                        telco = "1"
                    elif telco == "Vinaphone" :
                        telco = "2"
                    elif telco == "Viettel" :
                        telco = "4"
                    elif telco == "Vietnamobile" :
                        telco = "5"
                    elif telco == "Gmobile" :
                        telco = "7"
                    elif telco == "Indochina" :
                        telco = "8"
                    # current_time = now.strftime("%Y-%m-%d %H:%M:%S")
                    sql="update message_template set customer_id = (select id from customer where customer_name = '"+str(customer_name)+"'),service_type  = '"+str(service_type )+"',telco_id  = (select id from telco where id = '"+str(telco)+"'),status='"+str(status)+"',message='"+str(message)+"' where id ='"+str(id)+"'"
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

@sms_brandname.route('/delete_messagetemplate_brand', methods=['DELETE'])
def delete_messagetemplate_brand():
    session=datetime.datetime.now()
    logging.info(str(session) +"|delete_messagetemplate_brand")
    try :
        # db_connection_str = 'mysql+pymysql://gmbsmsadv:AKJshd019203910288@127.0.0.1/gmb_smsadv'
        # db_connection = create_engine(db_connection_str)
        db = pymysql.connect(host="172.17.0.5",    # your host, usually localhost
                     user="crmleeon",         # your username
                     passwd="BNAie92839470938888",  # your password
                     db="sms_brandname",
                     port=3306
                     )
        data = group()
        if str(data)== "chua truyen token" :
            return jsonify({'NOTOK': str(data)})
        else:
            timetoken = data["expireddate"]
            group_name = data["group_name"]
            nameuser = data["user"]
            logging.info(str(session) +"|delete_messagetemplate|"+str(data))
            if True:
                currentDate = time.strftime("%Y-%m-%d %H:%M:%S")
                print(currentDate)
                if str(timetoken) >= str(currentDate):
                    logging.info(str(session) +"|input|"+str(request.data))
                    record = json.loads(request.data)
                    print(record)
                    id = record["id"]
                    logging.info(str(session) +"|delete id|"+str(id))
                    sqlcontent= "select * from message_template where id = '"+str(id)+"'"
                    dfcontent = pd.read_sql(sqlcontent,db)
                    if not dfcontent.empty:
                        dfcontent=dfcontent.values.tolist()
                        logging.info(str(session) +"|content|"+str(dfcontent))
                        sql="delete from message_template where id = '"+str(id)+"'"
                        cursor = db.cursor()
                        cursor.execute(sql)
                        # sqllog = "insert into log (name,api,content,type) value ('"+str(nameuser)+"','deletebsmsadv','"+str(dfcontent).replace("'","")+"','DELETE')"
                        # cursor = db.cursor()
                        # cursor.execute(sqllog)
                        db.commit()
                        db.close()
                        logging.info(str(session) +"|response|Thành công")
                        return jsonify({'OK': 'OK'})
                    else :
                        db.close()
                        logging.error(str(session) +"|response|id không tồn tại")
                        return jsonify({'OK': 'id không tồn tại'})
                else :
                    db.close()
                    logging.error(str(session) +"|response|token hết hạn")
                    return jsonify({'NOTOK': 'token hết hạn','code':401}),401
            else :
                db.close()
                logging.error(str(session) +"|response|Không có quyền")
                return jsonify({'NOTOK': 'khong co quyen'})   
    except Exception as e:
        db.close()
        logging.error(str(session) +"|response|" +str(traceback.format_exc()))
        return jsonify({'NOTOK': str(e)}),400


#SMS Log
@sms_brandname.route('/sms_log_brand', methods=['GET'])
def sms_log():
    session=datetime.datetime.now()
    logging.info(str(session) +"|sms_log")
    try :
        db_connection_str = 'mysql+pymysql://crmleeon:BNAie92839470938888@172.17.0.5/sms_brandname'
        db_connection = create_engine(db_connection_str)
        data = group()
        if str(data)== "chua truyen token" :
            return jsonify({'NOTOK': str(data)})
        else:
            timetoken = data["expireddate"]
            group_name = data["group_name"]
            nameuser = data["user"]
            logging.info(str(session) +"|message_template|"+str(data))
            if True:
                currentDate = time.strftime("%Y-%m-%d %H:%M:%S")
                print(currentDate)
                if str(timetoken) >= str(currentDate):
                    sql ="SELECT b.id,b.created_time,a.customer_name,b.message, b.brand,b.isdn,d.telco_name as telco,b.transaction_id,b.brand_price,e.name as brand_type,b.requestId,c.partner_name,case when b.unicode=0 then 'Không unicode' else 'Có unicode' end as unicode, case when b.status =0 then 'Thất bại' else  'Thành công' end as status FROM customer a, sms_log202302 b, partner c, telco d, brand_type e where a.id = b.customer_id and b.partner_id = c.id and b.telco = d.code and b.brand_type = e.id"
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


#SMS find log
@sms_brandname.route('/find_smslog_brand', methods=['GET'])
def find_smslog_brand():
    session=datetime.datetime.now()
    logging.info(str(session) +"|find_smslog_brand")
    try :
        db_connection_str = 'mysql+pymysql://crmleeon:BNAie92839470938888@172.17.0.5/sms_brandname'
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
                    print(request.args)
                    starttime = request.args.get('starttime')
                    endtime = request.args.get('endtime')
                    endtime = str(endtime) + " 23:59:59"
                    status = request.args.get('status')
                    # sta = request.arg.get('sta')
                    isdn = request.args.get('isdn')
                    if str(starttime) == "" :
                        timestartquery = ""
                    else :
                        timestartquery = " and b.created_time >= '"+ str(starttime) +"'"
                    if str(endtime) == "" :
                        timeendquery = ""
                    else :
                        timeendquery = " and b.created_time <= '"+ str(endtime) +"'"
                    if str(isdn) == "" :
                        isdnquery = ""
                    else :
                        isdnquery = " and isdn = '"+ str(isdn)+"'"
                    if str(status) == "" :
                        statusquery = ""
                    else :
                        statusquery = " and b.status = '"+ str(status)+"'"
                    query = "b.id is not null" +timestartquery + timeendquery + isdnquery + statusquery
                    query1 = "b.id is not null" +timestartquery + timeendquery + isdnquery
                    if str(status) == "0":
                        sql ="SELECT b.id,b.created_time,a.customer_name,b.message, b.brand,b.isdn,d.telco_name as telco,b.transaction_id,b.brand_price,e.name as brand_type,b.requestId,c.partner_name,case when b.unicode=0 then 'Không unicode' else 'Có unicode' end as unicode, case when b.status =0 then 'Thất bại' else  'Thành công' end as status FROM customer a, sms_log202302 b, partner c, telco d, brand_type e where a.id = b.customer_id and b.partner_id = c.id and b.telco = d.code and b.brand_type = e.id and "+str(query)+""
                    elif str(status) == "1":
                        sql ="SELECT b.id,b.created_time,a.customer_name,b.message, b.brand,b.isdn,d.telco_name as telco,b.transaction_id,b.brand_price,e.name as brand_type,b.requestId,c.partner_name,case when b.unicode=0 then 'Không unicode' else 'Có unicode' end as unicode, case when b.status =0 then 'Thất bại' else  'Thành công' end as status FROM customer a, sms_log202302 b, partner c, telco d, brand_type e where a.id = b.customer_id and b.partner_id = c.id and b.telco = d.code and b.brand_type = e.id and "+str(query)+""
                    elif str(status) == "2":
                        sql ="SELECT b.id,b.created_time,a.customer_name,b.message, b.brand,b.scheduled_time,b.isdn,d.telco_name as telco_id,b.transaction_id,b.brand_price,e.name as brand_type,b.request_id,c.partner_name,case when b.unicode=0 then 'Không unicode' else 'Có unicode' end as unicode, case when b.status =1 then 'Chưa gửi' else  'Thành công' end as status FROM customer a, sms_scheduled b, partner c, telco d, brand_type e where a.id = b.customer_id and b.partner_id = c.id and b.telco_id = d.code and b.brand_type = e.id and "+str(query1)+""
                    print(sql)
                    df = pd.read_sql(sql,db_connection)
                    df['created_time'] = df['created_time'].astype('str')
                    json_records = df.to_json(orient ='records')
                    data = []
                    data = json.loads(json_records)
                    #df_total = pd.read_sql(sql_total,db_connection)
                    #json_records_total = df_total.to_json(orient ='records')
                    #data_total = []
                    #data_total = json.loads(json_records_total)
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
        print(str(traceback.format_exc()))
        logging.error(str(session) + "| error : " + str(traceback.format_exc()))
        return jsonify({'NOTOK': str(e)}),400