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
sms_adv = Blueprint('sms_adv', __name__)
@sms_adv.route('/customer_sms_adv', methods=['GET'])
def customer_sms_adv():
    session=datetime.datetime.now()
    logging.info(str(session) +"|report_sms_vendor")
    try :
        db_connection_str = 'mysql+pymysql://gmbsmsadv:AKJshd019203910288@171.244.56.178/gmb_smsadv'
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
# @sms_brandname.route('/insert_customer_sms_brandname', methods=['POST'])
# def insert_customer_sms_brandname():
#     session=datetime.datetime.now()
#     logging.info(str(session) +"|insert_sender_vender")
#     try :
#         db_connection_str = 'mysql+pymysql://crmleeon:BNAie92839470938888@172.17.0.5/sms_brandname'
#         db_connection = create_engine(db_connection_str)
#         data = group()
#         if str(data)== "chua truyen token" :
#             return jsonify({'NOTOK': str(data)})
#         else:
#             timetoken = data["expireddate"]
#             group_name = data["group_name"]
#             nameuser = data["user"]
#             logging.info(str(session) +"|find_sender_vender|"+str(data))
#             if True:
#                 currentDate = time.strftime("%Y-%m-%d %H:%M:%S")
#                 print(currentDate)
#                 if str(timetoken) >= str(currentDate) :
#                     record = json.loads(request.data)
#                     logging.info(str(session) +"|input|"+str(record))
#                     customer_name = record["customer_name"]
#                     customer_code = record["customer_code"]
#                     address = record["address"]
#                     phone = record["phone"]
#                     email = record["email"]
#                     token = record["token"]
#                     sql ="insert into customer (customer_name,customer_code,address,phone,email,token) value ('"+str(customer_name)+"','"+str(customer_code)+"','"+str(address)+"','"+str(phone)+"','"+str(email)+"','"+str(token)+"')"
#                     logging.info(str(session) +"|sql|"+str(sql))
#                     db_connection.execute(sql)
#                     return jsonify({'OK': 'OK'})
#                 else :
#                     logging.error(str(session) +"|response|token hết hạn")
#                     return jsonify({'NOTOK': 'token hết hạn','code':401}),401
#             else :
#                 logging.error(str(session) +"|response|Không có quyền")
#                 return jsonify({'NOTOK': 'khong co quyen'}) 
#     except Exception as e:
#         logging.error(str(session) + "| error : " + str(traceback.format_exc()))
#         return jsonify({'NOTOK': str(e)}),400

# @sms_brandname.route('/topup_customer_sms_brandname', methods=['POST'])
# def topup_customer_sms_brandname():
#     session=datetime.datetime.now()
#     logging.info(str(session) +"|topup_customer_sms_brandname")
#     try :
#         db_connection_str = 'mysql+pymysql://crmleeon:BNAie92839470938888@172.17.0.5/sms_brandname'
#         db_connection = create_engine(db_connection_str)
#         data = group()
#         if str(data)== "chua truyen token" :
#             return jsonify({'NOTOK': str(data)})
#         else:
#             timetoken = data["expireddate"]
#             group_name = data["group_name"]
#             nameuser = data["user"]
#             logging.info(str(session) +"|find_sender_vender|"+str(data))
#             if True:
#                 currentDate = time.strftime("%Y-%m-%d %H:%M:%S")
#                 print(currentDate)
#                 if str(timetoken) >= str(currentDate) :
#                     record = json.loads(request.data)
#                     logging.info(str(session) +"|input|"+str(record))
#                     customer_code = record["customer_code"]
#                     balance = record["balance"]
#                     #check customer_code
#                     sql_check_customer_code ="select * from customer where customer_code = '"+str(customer_code)+"'"
#                     logging.info(str(session) +"|sql_check_customer_code|"+str(sql_check_customer_code))
#                     df = pd.read_sql(sql_check_customer_code,db_connection)
#                     if df.empty:
#                         return jsonify({'NOTOK': 'customer_code không tồn tại'})
#                     sql ="update customer set balance_limit = balance_limit + "+str(balance)+" where customer_code = '"+str(customer_code)+"'"
#                     logging.info(str(session) +"|sql|"+str(sql))
#                     db_connection.execute(sql)
#                     return jsonify({'OK': 'OK'})
#                 else :
#                     logging.error(str(session) +"|response|token hết hạn")
#                     return jsonify({'NOTOK': 'token hết hạn','code':401}),401
#             else :
#                 logging.error(str(session) +"|response|Không có quyền")
#                 return jsonify({'NOTOK': 'khong co quyen'}) 
#     except Exception as e:
#         logging.error(str(session) + "| error : " + str(traceback.format_exc()))
#         return jsonify({'NOTOK': str(e)}),400