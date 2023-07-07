from flask import Blueprint
from flask import request, jsonify,Response
import json
# from exchangelib import DELEGATE, Account, Credentials,Configuration,HTMLBody
# from exchangelib import Message, Mailbox
import requests
from requests.auth import HTTPDigestAuth
import pandas as pd
from pandas import json_normalize
import pymysql
from flask_jsonpify import jsonpify
from datetime import datetime,timedelta
import datetime
from function import group
import time
from sqlalchemy import create_engine
import logging
import traceback
vendor = Blueprint('vendor', __name__)
@vendor.route('/listvendor', methods=['GET'])
def listvendor():
    try :
        session=datetime.datetime.now()
        logging.info(str(session) + "| listvendor")
        db_connection_str = 'mysql+pymysql://leeoncrm:41XmKsO3NBgHPwv@172.17.0.1/leeon_crm'
        db_connection = create_engine(db_connection_str)
        logging.info(str(session) + "| connect db ok")
        data = group()
        if str(data)== "chua truyen token" :
            logging.info(str(session) + "| chua truyen token")
            return jsonify({'NOTOK': str(data)})
        else:
            timetoken = data["expireddate"]
            group_name = data["group_name"]
            name = data["user"]
            #write log timetoken group_name name
            logging.info(str(session) + "| " +str(timetoken) + " | " + str(group_name) + " | " + str(name))
            sqllog = "insert into log (name,api,content) value ('"+str(name)+"','listvendor','view')"
            # execute the INSERT statement
            db_connection.execute(sqllog)
            if True:
                currentDate = time.strftime("%Y-%m-%d %H:%M:%S")
                if str(timetoken) >= str(currentDate) :
                    sql ="SELECT * FROM vendor"
                    df=pd.read_sql(sql, con=db_connection)
                    df['createdtime'] = df['createdtime'].astype('str')
                    df['updatedtime'] = df['updatedtime'].astype('str')
                    json_records = df.to_json(orient ='records')
                    data = []
                    data = json.loads(json_records)
                    context = {'data': data,'code': 'OK'}
                    logging.info(str(session) + "| reutrn ok")
                    return context
                else :
                    logging.info(str(session) + "| token het han")
                    return jsonify({'NOTOK': 'token hết hạn','code':401}),401
            else :
                    logging.info(str(session) + "| khong co quyen")
                    return jsonify({'NOTOK': 'khong co quyen'}),400
    except Exception as e:
        #write log with traceback
        logging.info(str(session) + "| loi | " + str(traceback.format_exc()))
        return jsonify({'NOTOK': 'NOTOK'}),400
@vendor.route('/deletevendor', methods=['DELETE'])
def deletevendor():
    try :
        session=datetime.datetime.now()
        logging.info(str(session) + "| deletevendor")
        db_connection_str = 'mysql+pymysql://leeoncrm:41XmKsO3NBgHPwv@172.17.0.1/leeon_crm'
        db_connection = create_engine(db_connection_str)
        logging.info(str(session) + "| connect db ok")
        data = group()
        if str(data)== "chua truyen token" :
            logging.info(str(session) + "| chua truyen token")
            return jsonify({'NOTOK': str(data)})
        else:
            timetoken = data["expireddate"]
            group_name = data["group_name"]
            name = data["user"]
            logging.info(str(session) + "| " +str(timetoken) + " | " + str(group_name) + " | " + str(name))
            if True:
                currentDate = time.strftime("%Y-%m-%d %H:%M:%S")
                print(currentDate)
                if str(timetoken) >= str(currentDate) :
                    record = json.loads(request.data)
                    id = record["id"]
                    sqlcheck= "select count(*) from vendor where id ='"+str(id)+"'"
                    df = pd.read_sql(sqlcheck, con=db_connection)
                    sl = df.iloc[0,0]
                    if int(sl) > 0 :
                        sql="delete from vendor where id = '"+str(id)+"'"
                        #execute sql
                        db_connection.execute(sql)
                        logging.info(str(session) + "| delete ok")
                        sqllog = "insert into log (name,api,content) value ('"+str(name)+"','deletevendor','delete id = "+str(id)+"')"
                        db_connection.execute(sqllog)
                        logging.info(str(session) + "| insert log ok")
                        return jsonify({'OK': 'OK'})
                    else :
                        logging.info(str(session) + "| id khong ton tai")
                        return jsonify({'OK': 'id không tồn tại'}),400
                else :
                    logging.info(str(session) + "| token het han")
                    return jsonify({'NOTOK': 'token hết hạn','code':401}),401
            else :
                logging.info(str(session) + "| khong co quyen")
                return jsonify({'NOTOK': 'khong co quyen'}),400   
    except Exception:
        logging.info(str(session) + "| loi | " + str(traceback.format_exc()))
        return jsonify({'NOTOK': 'NOTOK'}),400
@vendor.route('/insertvendor', methods=['POST'])
def insertvendor():
    try :
        session=datetime.datetime.now()
        logging.info(str(session) + "| insertvendor")
        db_connection_str = 'mysql+pymysql://leeoncrm:41XmKsO3NBgHPwv@172.17.0.1/leeon_crm'
        db_connection = create_engine(db_connection_str)
        logging.info(str(session) + "| connect db ok")
        data = group()
        if str(data)== "chua truyen token" :
            logging.info(str(session) + "| chua truyen token")
            return jsonify({'NOTOK': str(data)})
        else:
            timetoken = data["expireddate"]
            group_name = data["group_name"]
            nameuser = data["user"]
            logging.info(str(session) + "| " +str(timetoken) + " | " + str(group_name) + " | " + str(nameuser))
            if True:
                currentDate = time.strftime("%Y-%m-%d %H:%M:%S")
                print(currentDate)
                if str(timetoken) >= str(currentDate) :
                    record = json.loads(request.data)
                    name = record["name"]
                    nickname = record["nickname"]
                    address1 = record["address1"]
                    address2 = record["address2"]
                    phone = record["phone"]
                    email = record["email"]
                    bank = record["bank"]
                    bankaccount = record["bankaccount"]
                    bankbranch = record["bankbranch"]
                    logging.info(str(session) + "| " +str(name) + " | " + str(nickname) + " | " + str(address1) + " | " + str(address2) + " | " + str(phone) + " | " + str(email) + " | " + str(bank) + " | " + str(bankaccount) + " | " + str(bankbranch))
                    now = datetime.datetime.now()
                    current_time = now.strftime("%Y-%m-%d %H:%M:%S")
                    sqlcheck= "select count(*) from vendor where nickname ='"+str(nickname)+"'"
                    df = pd.read_sql(sqlcheck, con=db_connection)
                    sl = df.iloc[0,0]
                    if int(sl) > 0 :
                        logging.info(str(session) + "| nickname da ton tai")
                        return jsonify({'OK': 'nickname đã tồn tại'})
                    else :
                        sql="insert into vendor(name,nickname,address1,address2,phone,email,bank,bankaccount,bankbranch,createdtime,updatedtime) value ('"+str(name)+"','"+str(nickname)+"','"+str(address1)+"','"+str(address2)+"','"+str(phone)+"','"+str(email)+"','"+str(bank)+"','"+str(bankaccount)+"','"+str(bankbranch)+"','"+str(current_time)+"','"+str(current_time)+"')"
                        #execute sql
                        db_connection.execute(sql)
                        logging.info(str(session) + "| insert ok")
                        sqllog = "insert into log (name,api,content) value ('"+str(name)+"','insertvendor','"+str(record).replace("'","")+"')"
                        db_connection.execute(sqllog)
                        logging.info(str(session) + "| insert log ok")
                        return jsonify({'OK': 'OK'})
                else :
                    logging.info(str(session) + "| token het han")
                    return jsonify({'NOTOK': 'token hết hạn','code':401}),401
            else :
                logging.info(str(session) + "| khong co quyen")
                return jsonify({'NOTOK': 'khong co quyen'})   
    except Exception:
        logging.info(str(session) + "| loi | " + str(traceback.format_exc()))
        return jsonify({'NOTOK': 'NOTOK'}),400
@vendor.route('/updatevendor', methods=['POST'])
def updatevendor():
    try :
        session=datetime.datetime.now()
        logging.info(str(session) + "| updatevendor")
        db_connection_str = 'mysql+pymysql://leeoncrm:41XmKsO3NBgHPwv@172.17.0.1/leeon_crm'
        db_connection = create_engine(db_connection_str)
        logging.info(str(session) + "| connect db ok")
        data = group()
        if str(data)== "chua truyen token" :
            logging.info(str(session) + "| chua truyen token")
            return jsonify({'NOTOK': str(data)})
        else:
            timetoken = data["expireddate"]
            group_name = data["group_name"]
            nameuser = data["user"]
            logging.info(str(session) + "| " +str(timetoken) + " | " + str(group_name) + " | " + str(nameuser))
            if True:
                currentDate = time.strftime("%Y-%m-%d %H:%M:%S")
                print(currentDate)
                if str(timetoken) >= str(currentDate) :
                    record = json.loads(request.data)
                    name = record["name"]
                    id = record["id"]
                    nickname = record["nickname"]
                    address1 = record["address1"]
                    address2 = record["address2"]
                    phone = record["phone"]
                    email = record["email"]
                    bank = record["bank"]
                    bankaccount = record["bankaccount"]
                    bankbranch = record["bankbranch"]
                    logging.info(str(session) + "| " +str(name) + " | " + str(nickname) + " | " + str(address1) + " | " + str(address2) + " | " + str(phone) + " | " + str(email) + " | " + str(bank) + " | " + str(bankaccount) + " | " + str(bankbranch))
                    now = datetime.datetime.now()
                    current_time = now.strftime("%Y-%m-%d %H:%M:%S")
                    sqlcheck= "select count(*) from vendor where id ='"+str(id)+"'"
                    df = pd.read_sql(sqlcheck, con=db_connection)
                    sl = df.iloc[0,0]
                    if int(sl) > 0 :
                        sql="update vendor set name = '"+str(name)+"',nickname = '"+str(nickname)+"',address1 = '"+str(address1)+"',address2 = '"+str(address2)+"',phone = '"+str(phone)+"',email = '"+str(email)+"',bank = '"+str(bank)+"',bankaccount = '"+str(bankaccount)+"',bankbranch = '"+str(bankbranch)+"',updatedtime='"+str(current_time)+"' where id ='"+str(id)+"'"
                        db_connection.execute(sql)
                        logging.info(str(session) + "| update ok")
                        sqllog = "insert into log (name,api,content) value ('"+str(nameuser)+"','updatevendor','"+str(record).replace("'","")+"')"
                        db_connection.execute(sqllog)
                        logging.info(str(session) + "| insert log ok")
                        return jsonify({'OK': 'OK'})
                    else :
                        logging.info(str(session) + "| id khong ton tai")
                        return jsonify({'OK': 'id không tồn tại'})
                else :
                    logging.info(str(session) + "| token het han")
                    return jsonify({'NOTOK': 'token hết hạn','code':401}),401
            else :
                logging.info(str(session) + "| khong co quyen")
                return jsonify({'NOTOK': 'khong co quyen'}),400   
    except Exception:
        logging.info(str(session) + "| loi | " + str(traceback.format_exc()))
        return jsonify({'NOTOK': 'NOTOK'}),400
@vendor.route('/listvendorname', methods=['GET'])
def listvendorname():
    try :
        db = pymysql.connect(host="172.17.0.1",    # your host, usually localhost
                     user="leeoncrm",         # your username
                     passwd="41XmKsO3NBgHPwv",  # your password
                     db="leeon_crm",
                     port=3306
                     )
        sql ="SELECT id ,name FROM vendor"
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