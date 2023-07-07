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
from function import group
import time
import re
import logging
partner = Blueprint('partner', __name__)
@partner.route('/listpartner', methods=['GET'])
def listpartner():
    session=datetime.now()
    logging.info(str(session) +"|listpartner")
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
            partner = data["partner"]
            logging.info(str(session) +"|listpartner|"+str(data))
            partner=str(partner).replace("[","").replace("]","")
            if True:
                currentDate = time.strftime("%Y-%m-%d %H:%M:%S")
                if str(timetoken) >= str(currentDate) :
                    if str(partner) =="":
                        sql ="select * from partner order by createdtime desc"
                    else:
                        sql ="select * from partner where nickname in ("+str(partner)+") order by createdtime desc"
                    df = pd.read_sql(sql,db)
                    df['createdtime'] = df['createdtime'].astype('str')
                    ip =[]
                    i = 0
                    while i < len(df):
                        find = str(df.iloc[i]['vosid']).find(",")
                        if find == -1 :
                            sqlip = "select ip from vos where id = '"+str(df.iloc[i]['vosid'])+"'"
                            dfip = pd.read_sql(sqlip,db)
                            if dfip.empty:
                                ip.append("")
                            else :
                                ip.append(str(dfip.iloc[0,0]))
                        else :
                            print(str(df.iloc[i]['vosid']))
                            vosid = str(str(df.iloc[i]['vosid']).split(",")).replace("[","").replace("]","")
                            sqlvos = "select ip from vos where id in ("+str(vosid)+")"
                            dfvos = pd.read_sql(sqlvos,db)
                            vos = str(dfvos.values.tolist()).replace("[","").replace("]","").replace("'","").replace(" ","")
                            ip.append(vos)
                        i=i+1
                    df['ip'] = ip
                    json_records = df.to_json(orient ='records')
                    data = []
                    data = json.loads(json_records)
                    context = {'data': data,'code': 'OK'}
                    logging.info(str(session) +"|response | Thành công")
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
        logging.error(str(session) +"|response|" +str(e))
        return jsonify({'NOTOK': str(e)}),400
@partner.route('/deletepartner', methods=['DELETE'])
def deletepartner():
    session=datetime.now()
    logging.info(str(session) +"|deletepartner")
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
                    sqlcheck="select * from partner where id = '"+str(id)+"'"
                    dfcontent = pd.read_sql(sqlcheck,db)
                    if dfcontent.empty :
                        db.close()
                        logging.error(str(session) +"|response|id không tồn tại")
                        return jsonify({'OK': 'id không tồn tại'}),400
                    sql ="delete from partner where id = '"+str(id)+"'"
                    cursor = db.cursor()
                    cursor.execute(sql)
                    db.commit()
                    sqllog = "insert into log (name,api,content,type) value ('"+str(nameuser)+"','deteleacount','"+str(dfcontent).replace("'","")+"','DELETE')"
                    cursor = db.cursor()
                    cursor.execute(sqllog)
                    db.commit()
                    logging.info(str(session) +"|response | Thành công")
                    return jsonify({'OK': id})
                else :
                    db.close()
                    logging.error(str(session) +"|response|token hết hạn")
                    return jsonify({'NOTOK': 'token hết hạn'}),401
            else :
                db.close()
                logging.error(str(session) +"|response|Không có quyền")
                return jsonify({'NOTOK': 'khong co quyen'}),400
    except Exception as e:
        db.close()
        logging.error(str(session) +"|response|" +str(e))
        return jsonify({'NOTOK': str(e)}),400
@partner.route('/insertpartner', methods=['POST'])
def insertpartner():
    session=datetime.now()
    logging.info(str(session) +"|insertpartner")
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
            logging.info(str(session) +"|listaccount|"+str(data))
            if True:
                currentDate = time.strftime("%Y-%m-%d %H:%M:%S")
                print(currentDate)
                if str(timetoken) >= str(currentDate) :
                    record = json.loads(request.data)
                    logging.info(str(session) +"|input|"+str(request.data))
                    address = record["address"]
                    company = record["company"]
                    email = record["email"]
                    nickname = record["nickname"]
                    phone = record["phone"]
                    represent = record["represent"]
                    website = record["website"]
                    vosid= record["ip"]
                    vosid = str(vosid.split(",")).replace("[","").replace("]","")
                    sqlvosid ="select id from vos where ip in ("+str(vosid)+")"
                    dfvosid = pd.read_sql(sqlvosid,db)
                    vosid=str(dfvosid.values.tolist()).replace("[","").replace("]","")
                    print(vosid)
                    taxcode= record["taxcode"]
                    bankaccount= record["bankaccount"]
                    bank= record["bank"]
                    bankbranch= record["bankbranch"]
                    now = datetime.now()
                    current_time = now.strftime("%Y-%m-%d %H:%M:%S")
                    sqlcheck= "select count(*) from partner where nickname ='"+str(nickname)+"'"
                    df = pd.read_sql(sqlcheck,db)
                    sl = df.iloc[0,0]
                    if int(sl) > 0 :
                        db.close()
                        logging.error(str(session) +"|response|Nickname đã tồn tại")
                        return jsonify({'OK': 'Nickname đã tồn tại'})
                    else :
                        sql="insert into partner(company,nickname,address,represent,phone,email,website,createdtime,updatedtime,vosid,taxcode,bankaccount,bank,bankbranch) value ('"+str(company)+"','"+str(nickname)+"','"+str(address)+"','"+str(represent)+"','"+str(phone)+"','"+str(email)+"','"+str(website)+"','"+str(current_time)+"','"+str(current_time)+"','"+str(vosid)+"','"+str(taxcode)+"','"+str(bankaccount)+"','"+str(bank)+"','"+str(bankbranch)+"')"
                        logging.info(str(session) +"|query|"+str(sql))
                        cursor = db.cursor()
                        cursor.execute(sql)
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
        return jsonify({'NOTOK': str(e)}),400
@partner.route('/updatepartner', methods=['POST'])
def updatepartner():
    session=datetime.now()
    logging.info(str(session) +"|updatepartner")
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
            logging.info(str(session) +"|updatepartner|"+str(data))
            if True:
                currentDate = time.strftime("%Y-%m-%d %H:%M:%S")
                print(currentDate)
                if str(timetoken) >= str(currentDate) :
                    record = json.loads(request.data)
                    print(record)
                    id = record["id"]
                    address = record["address"]
                    company = record["company"]
                    email = record["email"]
                    nickname = record["nickname"]
                    phone = record["phone"]
                    represent = record["represent"]
                    website = record["website"]
                    vosid= record["ip"]
                    logging.info(str(session) +"|input|"+str(request.data))
                    vosid = str(vosid.split(",")).replace("[","").replace("]","")
                    sqlvosid ="select id from vos where ip in ("+str(vosid)+")"
                    dfvosid = pd.read_sql(sqlvosid,db)
                    vosid=str(dfvosid.values.tolist()).replace("[","").replace("]","")
                    taxcode= record["taxcode"]
                    bankaccount= record["bankaccount"]
                    bank= record["bank"]
                    bankbranch= record["bankbranch"]
                    now = datetime.now()
                    current_time = now.strftime("%Y-%m-%d %H:%M:%S")
                    sqlcheck= "select count(*) from partner where id ='"+str(id)+"'"
                    df = pd.read_sql(sqlcheck,db)
                    sl = df.iloc[0,0]
                    if int(sl) > 0 :
                        sql="update partner set company ='"+str(company)+"',address ='"+str(address)+"',email = '"+str(email)+"',nickname = '"+str(nickname)+"',phone = '"+str(phone)+"',represent = '"+str(represent)+"',website = '"+str(website)+"',vosid = '"+str(vosid)+"',taxcode='"+str(taxcode)+"',bankaccount='"+str(bankaccount)+"',bank='"+str(bank)+"',bankbranch='"+str(bankbranch)+"' where id ='"+str(id)+"'"
                        logging.info(str(session) +"|query|"+str(sql))
                        cursor = db.cursor()
                        cursor.execute(sql)
                        sqllog = "insert into log (name,api,content,type) value ('"+str(nameuser)+"','updateaccount','"+str(record).replace("'","")+"','UPDATE')"
                        cursor = db.cursor()
                        cursor.execute(sqllog)
                        db.commit()
                        logging.info(str(session) +"|response|Thành công")
                        return jsonify({'OK': 'OK'})
                    else :
                        db.close()
                        logging.error(str(session) +"|response|Không tồn tại id này")
                        return jsonify({'NOTOK': 'Không tồn tại id này'})
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