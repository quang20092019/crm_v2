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
mapping = Blueprint('mapping', __name__)
@mapping.route('/listmaping', methods=['GET'])
def listmaping():
    session=datetime.datetime.now()
    logging.info(str(session) +"|listmaping")
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
            logging.info(str(session) +"|listmaping|"+str(data))
            if True:
                currentDate = time.strftime("%Y-%m-%d %H:%M:%S")
                print(currentDate)
                if str(timetoken) >= str(currentDate) :
                    sql ="SELECT a.createdtime,a.id,a.name,b.ip,c.name'telco' FROM (SELECT * FROM mapping) a LEFT JOIN (SELECT * FROM vos) b ON a.vosid = b.id LEFT JOIN (SELECT * FROM telco) c ON a.telcoid = c.id order by createdtime desc"
                    df = pd.read_sql(sql,db)
                    df['createdtime'] = df['createdtime'].astype('str')
                    json_records = df.to_json(orient ='records')
                    data = []
                    data = json.loads(json_records)
                    context = {'data': data,'code': 'OK'}
                    sqllog = "insert into log (name,api,content,type) value ('"+str(nameuser)+"','listmaping','view','SELECT')"
                    cursor = db.cursor()
                    cursor.execute(sqllog)
                    db.commit()
                    logging.info(str(session) +"|response|Thành công")
                    db.close() 
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
@mapping.route('/insertmaping', methods=['POST'])
def insertmaping():
    session=datetime.datetime.now()
    logging.info(str(session) +"|insertmaping")
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
            logging.info(str(session) +"|insertmaping|"+str(data))
            if True:
                currentDate = time.strftime("%Y-%m-%d %H:%M:%S")
                print(currentDate)
                if str(timetoken) >= str(currentDate) :
                    record = json.loads(request.data)
                    logging.info(str(session) +"|input|"+str(request.data))
                    ip = record["ip"]
                    name = record["name"]
                    telco = record["telco"]
                    now = datetime.datetime.now()
                    current_time = now.strftime("%Y-%m-%d %H:%M:%S")
                    sqlcheck= "select count(*) from mapping where name ='"+str(name)+"'"
                    df = pd.read_sql(sqlcheck,db)
                    sl = df.iloc[0,0]
                    if int(sl) > 0 :
                        logging.error(str(session) +"|response|name đã tồn tại")
                        db.close()
                        return jsonify({'OK': 'name đã tồn tại'}),400
                    else :
                        sqlvos= "select id from vos where ip = '"+str(ip)+"'"
                        dfvos = pd.read_sql(sqlvos,db)
                        vos = dfvos.iloc[0,0]
                        try:
                            sqltelco= "select id from telco where name = '"+str(telco)+"'"
                            dftelco = pd.read_sql(sqltelco,db)
                            telco = dftelco.iloc[0,0]
                        except Exception as e:
                            print(str(e))
                            logging.error(str(session) +"|response|không có telco này")
                            db.close()
                            return jsonify({'NOT': 'ko co telco nay'}),400
                        sql="insert into mapping(name,vosid,telcoid,createdtime,updatedtime) value ('"+str(name)+"','"+str(vos)+"','"+str(telco)+"','"+str(current_time)+"','"+str(current_time)+"')"
                        cursor = db.cursor()
                        cursor.execute(sql)
                        logging.info(str(session) +"|insert OK |"+str(sql))
                        sqllog = "insert into log (name,api,content,type) value ('"+str(nameuser)+"','insertmaping','"+str(record).replace("'","")+"','INSERT')"
                        cursor = db.cursor()
                        cursor.execute(sqllog)
                        db.commit()
                        db.close()
                        logging.info(str(session) +"|response|Thành công")
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
        return jsonify({'NOTOK': str(e)})
@mapping.route('/detelemaping', methods=['DELETE'])
def detelemaping():
    session=datetime.datetime.now()
    logging.info(str(session) +"|detelemaping")
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
            logging.info(str(session) +"|detelemaping|"+str(data))
            if True:
                currentDate = time.strftime("%Y-%m-%d %H:%M:%S")
                print(currentDate)
                if str(timetoken) >= str(currentDate) :
                    record = json.loads(request.data)
                    logging.info(str(session) +"|input|"+str(request.data))
                    id = record["id"]
                    sqlcheck= "select * from mapping where id ='"+str(id)+"'"
                    df = pd.read_sql(sqlcheck,db)
                    if not df.empty :
                        sql="delete from mapping where id = '"+str(id)+"'"
                        cursor = db.cursor()
                        cursor.execute(sql)
                        logging.info(str(session) +"|delete OK |"+str(sql))
                        sqllog = "insert into log (name,api,content,type) value ('"+str(nameuser)+"','detelemaping','"+str(df.values.tolist()).replace("'","")+"','DELETE')"
                        cursor = db.cursor()
                        cursor.execute(sqllog)
                        db.commit()
                        db.close()
                        logging.info(str(session) +"|response|Thành công")
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
        return jsonify({'NOTOK': str(e)})
@mapping.route('/updatemaping', methods=['POST'])
def updatemaping():
    session=datetime.datetime.now()
    logging.info(str(session) +"|updatemaping")
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
            logging.info(str(session) +"|updatemaping|"+str(data))
            if True:
                currentDate = time.strftime("%Y-%m-%d %H:%M:%S")
                print(currentDate)
                if str(timetoken) >= str(currentDate) :
                    record = json.loads(request.data)
                    logging.info(str(session) +"|input|"+str(request.data))
                    id = record["id"]
                    ip = record["ip"]
                    name = record["name"]
                    telco = record["telco"]
                    now = datetime.datetime.now()
                    current_time = now.strftime("%Y-%m-%d %H:%M:%S")
                    sqlcheck= "select count(*) from mapping where id ='"+str(id)+"'"
                    df = pd.read_sql(sqlcheck,db)
                    sl = df.iloc[0,0]
                    if int(sl) > 0 :
                        sqlvos= "select id from vos where ip = '"+str(ip)+"'"
                        dfvos = pd.read_sql(sqlvos,db)
                        vos = dfvos.iloc[0,0]
                        sqltelco= "select id from telco where name = '"+str(telco)+"'"
                        dftelco = pd.read_sql(sqltelco,db)
                        telco = dftelco.iloc[0,0]
                        sql="update mapping set name = '"+str(name)+"',vosid = '"+str(vos)+"',telcoid = '"+str(telco)+"',updatedtime='"+str(current_time)+"' where id ='"+str(id)+"'"
                        cursor = db.cursor()
                        cursor.execute(sql)
                        logging.info(str(session) +"|update |"+str(sql))
                        sqllog = "insert into log (name,api,content,type) value ('"+str(nameuser)+"','updatemaping','"+str(record).replace("'","")+"','UPDATE')"
                        cursor = db.cursor()
                        cursor.execute(sqllog)
                        db.commit()
                        db.close()
                        logging.info(str(session) +"|response|Thành công")
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
        return jsonify({'NOTOK': str(e)})
