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
import datetime
from function import group
import time
import logging
import traceback
from function import connect_db
account = Blueprint('account', __name__)
session=datetime.datetime.now()
@account.route('/listaccount', methods=['GET'])
def listaccount():
    logging.info(str(session) +"|--------------listaccount-----------------------")
    try :
        db = connect_db()
        data = group()
        if str(data)== "chua truyen token" :
            return jsonify({'NOTOK': str(data)})
        else:
            timetoken = data["expireddate"]
            group_name = data["group_name"]
            nameuser = data["user"]
            logging.info(str(session) +"|listaccount|"+str(data))
            if True:
                currentDate = time.strftime("%Y-%m-%d %H:%M:%S")
                if str(timetoken) >= str(currentDate) :
                    sql ="SELECT a.createdtime,a.id,a.name,b.ip , CASE WHEN TYPE =0 THEN 'Agent' WHEN TYPE =1 THEN 'Payment' END AS type FROM (SELECT * FROM account) a LEFT JOIN (SELECT * FROM vos) b ON a.vosid = b.id order by createdtime desc"
                    logging.info(str(session) +"|query|"+str(sql))
                    df = pd.read_sql(sql,db)
                    df['createdtime'] = df['createdtime'].astype('str')
                    json_records = df.to_json(orient ='records')
                    data = []
                    data = json.loads(json_records)
                    context = {'data': data,'code': 'OK'}
                    sqllog = "insert into log (name,api,content,type) value ('"+str(nameuser)+"','listaccount','view','SELECT')"
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
        logging.error(str(session) +"|response|" +str(traceback.format_exc()))
        return jsonify({'NOTOK': str(e)}),400
@account.route('/insertaccount', methods=['POST'])
def insertaccount():
    logging.info(str(session) +"----------------insertaccount------------------")
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
            logging.info(str(session) +"|insertaccount|"+str(data))
            if True:
                currentDate = time.strftime("%Y-%m-%d %H:%M:%S")
                print(currentDate)
                if str(timetoken) >= str(currentDate) :
                    logging.info(str(session) +"|input|"+str(request.data))
                    record = json.loads(request.data)
                    ip = record["ip"]
                    name = record["name"]
                    type = record["type"]
                    now = datetime.datetime.now()
                    current_time = now.strftime("%Y-%m-%d %H:%M:%S")
                    sqlcheck= "select count(*) from account where name ='"+str(name)+"'"
                    df = pd.read_sql(sqlcheck,db)
                    sl = df.iloc[0,0]
                    if int(sl) > 0 :
                        db.close()
                        logging.error(str(session) +"|response|name đã tồn tại")
                        return jsonify({'OK': 'name đã tồn tại'})
                    else :
                        sqlvos= "select id from vos where ip = '"+str(ip)+"'"
                        dfvos = pd.read_sql(sqlvos,db)
                        if dfvos.empty :
                            db.close()
                            logging.error(str(session) +"|response|Sai IP")
                            return jsonify({'NOTOK': 'Sai IP','code':400}),400
                        vos = dfvos.iloc[0,0]
                        if type == 'Agent' :
                            sql="insert into account(name,type,vosid,createdtime,updatedtime) value ('"+str(name)+"','0','"+str(vos)+"','"+str(current_time)+"','"+str(current_time)+"')"
                        else :
                            sql="insert into account(name,type,vosid,createdtime,updatedtime) value ('"+str(name)+"','1','"+str(vos)+"','"+str(current_time)+"','"+str(current_time)+"')"
                        logging.info(str(session) +"|query insert |"+str(sql))
                        cursor = db.cursor()
                        cursor.execute(sql)
                        db.commit()
                        sqllog = "insert into log (name,api,content,type) value ('"+str(nameuser)+"','insertaccount','"+str(record).replace("'","")+"','INSERT')"
                        cursor = db.cursor()
                        cursor.execute(sqllog)
                        db.commit()
                        logging.info(str(session) +"|response|Thành công")
                        return jsonify({'OK': 'OK'})
                    db.close()
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
        return jsonify({'NOTOK': str(e)})
@account.route('/deteleaccount', methods=['DELETE'])
def deteleacount():
    logging.info(str(session) +"|----------------------deteleacount----------------------")
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
                    sqlcontent= "select * from account where id = '"+str(id)+"'"
                    dfcontent = pd.read_sql(sqlcontent,db)
                    if dfcontent.empty :
                        db.close()
                        logging.error(str(session) +"|response|id không tồn tại")
                        return jsonify({'OK': 'id không tồn tại'}),400
                    dfcontent=dfcontent.values.tolist()
                    logging.info(str(session) +"|bản ghi |"+ str(dfcontent))
                    sql="delete from account where id = '"+str(id)+"'"
                    cursor = db.cursor()
                    cursor.execute(sql)
                    sqllog = "insert into log (name,api,content,type) value ('"+str(nameuser)+"','deteleacount','"+str(dfcontent).replace("'","")+"','DELETE')"
                    cursor = db.cursor()
                    cursor.execute(sqllog)
                    db.commit()
                    logging.info(str(session) +"|response | Thành công")
                    return jsonify({'OK': 'OK'})       
                    db.close()
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
@account.route('/updateaccount', methods=['POST'])
def updateaccount():
    logging.info(str(session) +"|updateaccount")
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
            logging.info(str(session) +"|updateaccount|"+str(data))
            if True:
                currentDate = time.strftime("%Y-%m-%d %H:%M:%S")
                print(currentDate)
                if str(timetoken) >= str(currentDate) :
                    logging.info(str(session) +"|input|"+str(request.data))
                    record = json.loads(request.data)
                    id = record["id"]
                    ip = record["ip"]
                    name = record["name"]
                    type = record["type"]
                    now = datetime.datetime.now()
                    current_time = now.strftime("%Y-%m-%d %H:%M:%S")
                    sqlcheck= "select count(*) from account where id ='"+str(id)+"'"
                    df = pd.read_sql(sqlcheck,db)
                    sl = df.iloc[0,0]
                    if int(sl) > 0 :
                        sqlvos= "select id from vos where ip = '"+str(ip)+"'"
                        dfvos = pd.read_sql(sqlvos,db)
                        vos = dfvos.iloc[0,0]
                        if type == 'Agent' :
                            sql="update account set name = '"+str(name)+"',type = '0',vosid = '"+str(vos)+"',updatedtime='"+str(current_time)+"' where id ='"+str(id)+"'"
                            cursor = db.cursor()
                            cursor.execute(sql)
                            db.commit()
                        else :
                            sql="update account set name = '"+str(name)+"',type = '1',vosid = '"+str(vos)+"',updatedtime='"+str(current_time)+"' where id ='"+str(id)+"'"
                            cursor = db.cursor()
                            cursor.execute(sql)
                            db.commit()
                        sqllog = "insert into log (name,api,content,type) value ('"+str(nameuser)+"','updateaccount','"+str(record).replace("'","")+"','UPDATE')"
                        cursor = db.cursor()
                        cursor.execute(sqllog)
                        db.commit()
                        return jsonify({'OK': 'OK'})
                    else :
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
        logging.error(str(session) +"|response|" +str(e))
        return jsonify({'NOTOK': str(e)}),400
@account.route('/select_account', methods=['GET'])
def select_account():
    logging.info(str(session) +"|select_account")
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
                    nickname = request.args.get('partner')
                    sql ="select name from account where id in (SELECT distinct(accountid) FROM leeon_crm.partnerdetail where partnerid = (select id from partner where nickname = '"+str(nickname)+"'))"
                    df = pd.read_sql(sql,db)
                    json_records = df.to_json(orient ='records')
                    data = []
                    data = json.loads(json_records)
                    context = {'data': data,'code': 'OK'}
                    db.commit()
                    db.close()
                    logging.info(str(session) +"|response | Thành công")
                    return context
                else :
                    logging.error(str(session) +"|response|token hết hạn")
                    return jsonify({'NOTOK': 'token hết hạn','code':401}),401
            else :
                logging.error(str(session) +"|response|Không có quyền")
                return jsonify({'NOTOK': 'khong co quyen'}),400
    except Exception as e:
        db.close()
        logging.error(str(session) +"|response|" +str(e))
        return jsonify({'NOTOK': str(e)}),400