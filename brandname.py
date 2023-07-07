from flask import Blueprint,send_from_directory,send_file
from flask import request, jsonify
import json

import requests
from requests.auth import HTTPDigestAuth
import pandas as pd
from pandas import json_normalize
import pymysql
from flask_jsonpify import jsonpify
from datetime import datetime
import datetime
import os
from function import group
import time
import logging
import traceback
session=datetime.datetime.now()
brandname = Blueprint('brandname', __name__)
@brandname.route('/listbrandnames', methods=['GET'])
def listbrandnames():
    logging.info(str(session) +"|listbrandnames")
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
            logging.info(str(session) +"|listaccount|"+str(data))
            if True:
                currentDate = time.strftime("%Y-%m-%d %H:%M:%S")
                print(currentDate)
                if str(timetoken) >= str(currentDate) :
                    sql ="SELECT brandname_owner,id,name,registerby,address,registrationnumber,begintime,createdtime,expiretime,updatedtime,case when type = '1'  then 'Không Quảng Cáo' when type = '2'  then 'Quảng Cáo' end as loai ,case when register = '1'  then 'Voice Brandname' when register = '2'  then 'Đăng ký định danh' end as register FROM brandname"
                    df = pd.read_sql(sql,db)
                    # cắt 10 ký tự đầu tiên begintime
                    df['begintime'] = df['begintime'].astype('str').str[:10]
                    df['createdtime'] = df['createdtime'].astype('str').str[:10]
                    df['expiretime'] = df['expiretime'].astype('str').str[:10]
                    df['updatedtime'] = df['updatedtime'].astype('str').str[:10]
                    json_records = df.to_json(orient ='records')
                    data = []
                    data = json.loads(json_records)
                    context = {'data': data,'code': 'OK'}
                    sqllog = "insert into log (name,api,content,type) value ('"+str(nameuser)+"','listbrandnames','view','SELECT')"
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
        logging.error(str(session) +"|response|" +str(e))
        return jsonify({'NOTOK': str(e)}),400
@brandname.route('/deletebrandnames', methods=['DELETE'])
def deletebrandnames():
    logging.info(str(session) +"|deletebrandnames")
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
            logging.info(str(session) +"|listaccount|"+str(data))
            if True:
                currentDate = time.strftime("%Y-%m-%d %H:%M:%S")
                print(currentDate)
                if str(timetoken) >= str(currentDate) :
                    record = json.loads(request.data)
                    id = record["id"]
                    logging.info(str(session) +"|delete id|"+str(id))
                    sqlcontent= "select * from brandname where id = '"+str(id)+"'"
                    dfcontent = pd.read_sql(sqlcontent,db)
                    if not dfcontent.empty:
                        dfcontent=dfcontent.values.tolist()
                        logging.info(str(session) +"|content|"+str(dfcontent))
                        sql="delete from brandname where id = '"+str(id)+"'"
                        cursor = db.cursor()
                        cursor.execute(sql)
                        sqllog = "insert into log (name,api,content,type) value ('"+str(nameuser)+"','deletebrandnames','"+str(dfcontent).replace("'","")+"','DELETE')"
                        cursor = db.cursor()
                        cursor.execute(sqllog)
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
        logging.error(str(session) +"|response|" +str(e))
        return jsonify({'NOTOK': str(e)}),400
@brandname.route('/insertbrandnames', methods=['POST'])
def insertbrandnames():
    logging.info(str(session) +"|insertbrandnames")
    try:
        db = pymysql.connect(host="172.17.0.1",    # your host, usually localhost
                     user="leeoncrm",         # your username
                     passwd="41XmKsO3NBgHPwv",  # your password
                     db="leeon_crm",
                     port=3306
                     )
        cursor = db.cursor()
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
                    record = json.loads(request.data)
                    logging.info(str(session) +"|input|"+str(record))
                    name = record["name"]
                    begintime = record["begintime"]
                    expiretime = record["expiretime"]
                    registerby = record["registerby"]
                    address = record["address"]
                    register = record["register"]
                    brandname_owner = record["brandname_owner"]
                    #get partnercode
                    sqlpartnercode = "select partner_code from partner where nickname = '"+str(registerby)+"'"
                    df = pd.read_sql(sqlpartnercode,db)
                    partner_code = df.iloc[0]["partner_code"]
                    if str(register) == "Voice Brandname" :
                        register = 1
                    else :
                        register = 2
                    registrationnumber = record["registrationnumber"]
                    now = datetime.datetime.now()
                    current_time = now.strftime("%Y-%m-%d %H:%M:%S")
                    sql="insert into brandname(register,name,registerby,address,registrationnumber,begintime,expiretime,createdtime,updatedtime,partner_code,brandname_owner) value ('"+str(register)+"','"+str(name)+"','"+str(registerby)+"','"+str(address)+"','"+str(registrationnumber)+"','"+str(begintime)+"','"+str(expiretime)+"','"+str(current_time)+"','"+str(current_time)+"','"+str(partner_code)+"','"+str(brandname_owner)+"')"
                    logging.info(str(session) +"|query|"+str(sql))
                    cursor.execute(sql)
                    sqllog = "insert into log (name,api,content,type) value ('"+str(nameuser)+"','insertbrandnames','"+str(record).replace("'","")+"','INSERT')"
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
        logging.error(str(session) +"|response|" +str(traceback.format_exc()))
        return jsonify({'NOTOK': str(e)}),400
@brandname.route('/updatebrandname', methods=['POST'])
def updatebrandname():
    logging.info(str(session) +"|updatebrandname")
    try:
        db = pymysql.connect(host="172.17.0.1",    # your host, usually localhost
                     user="leeoncrm",         # your username
                     passwd="41XmKsO3NBgHPwv",  # your password
                     db="leeon_crm",
                     port=3306
                     )
        cursor = db.cursor()
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
                if str(timetoken) >= str(currentDate) :
                    record = json.loads(request.data)
                    logging.info(str(session) +"|input|"+str(request.data))
                    id = record["id"]
                    record = json.loads(request.data)
                    address = record["address"]
                    name = record["name"]
                    register = record["register"]
                    if str(register) == "Voice Brandname" :
                        register = 1
                    else :
                        register = 2
                    begintime = record["begintime"]
                    expiretime = record["expiretime"]
                    registerby = record["registerby"]
                    brandname_owner = record["brandname_owner"]
                    type = record["type"]
                    if str(type) == "Không quảng cáo" :
                        type = 1
                    else :
                        type = 2
                    registrationnumber = record["registrationnumber"]
                    now = datetime.datetime.now()
                    current_time = now.strftime("%Y-%m-%d %H:%M:%S")
                    sql="update brandname set name = '"+str(name)+"',registerby = '"+str(registerby)+"',address = '"+str(address)+"',registrationnumber = '"+str(registrationnumber)+"',begintime = '"+str(begintime)+"',expiretime = '"+str(expiretime)+"',updatedtime='"+str(current_time)+"',type='"+str(type)+"',register= '"+str(register)+"',brandname_owner= '"+str(brandname_owner)+"' where id ='"+str(id)+"'"
                    logging.info(str(session) +"|query|"+str(sql))
                    cursor.execute(sql)
                    sqllog = "insert into log (name,api,content,type) value ('"+str(nameuser)+"','updatebrandname','"+str(record).replace("'","")+"','UPDATE')"
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
                return jsonify({'NOTOK': 'khong co quyen'})     
    except Exception as e:
        db.close()
        logging.error(str(session) +"|response|" +str(e))
        return jsonify({'NOTOK': str(e)})
@brandname.route('/download_brandname', methods=['POST'])
def download_brandname():
    logging.info("------------download_brandname--------------")
    try:
        db = pymysql.connect(host="172.17.0.1",    # your host, usually localhost
                     user="leeoncrm",         # your username
                     passwd="41XmKsO3NBgHPwv",  # your password
                     db="leeon_crm",
                     port=3306
                     )
        sql ="SELECT * FROM brandname"
        df = pd.read_sql(sql,db)
        session=datetime.datetime.now()
        filename = "brandname_"+str(session)+".csv"
        path = os.path.join(os.getcwd(),'filedownload',filename)
        df.to_csv(path,encoding='utf-8-sig')
        db.close()
        logging.info(str(session) +"|response|Thành công")
        return send_file(path, as_attachment=True)
    except Exception as e:
        db.close()
        logging.error(str(session) +"|response|" +str(traceback.format_exc()))
        return jsonify({'NOTOK': str(e)})
@brandname.route('/report_brandnames', methods=['GET'])
def report_brandnames():
    logging.info(str(session) +"|listbrandnames")
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
            partner = data["partner"]
            partner_code = data["partner_code"]
            logging.info(str(session) +"|listaccount|"+str(data))
            if True:
                currentDate = time.strftime("%Y-%m-%d %H:%M:%S")
                if str(timetoken) >= str(currentDate) :
                    if str(partner_code) != "None": #khách hàng
                        sql ="SELECT a.id,a.brandName,LEFT(a.createdtime,10)'createdtime',(SUM(a.callsuccess)+SUM(a.callmiss))'calltotal',SUM(a.callsuccess)'callsuccess',SUM(a.callmiss) 'callmiss',ROUND(SUM(a.voicetime)/60,0)'voicetime',SUM(a.revenue)'revenue',a.vosIp,b.nickname,a.telco,CONCAT(ROUND((SUM(a.callmiss)/(SUM(a.callsuccess)+SUM(a.callmiss)))*100,2),' %')'callrate' FROM report a,partner b WHERE a.isBrand =0 AND a.createdtime >= CURDATE() AND a.partner_code like '%"+str(partner_code)+"%' AND RIGHT(a.partner_code,5) = b.partner_code GROUP BY a.partner_code,a.brandName"
                    elif str(partner) != "": # nội bộ
                        partner_code_noibos =[]
                        list_nickname = str(partner).replace("[","").replace("]","").replace("'","").replace(" ","").split(",")
                        for i in list_nickname:
                            # get partner_code
                            sql_nickname = "select partner_code from partner where nickname = '"+str(i)+"'"
                            df_nickname = pd.read_sql(sql_nickname,db)
                            partner_code_noibo = df_nickname.iloc[0]['partner_code']
                            partner_code_noibos.append(partner_code_noibo)
                        partner_code_noibos = str(partner_code_noibos).replace(" ","").replace("'","").replace(",","|").replace("[","'").replace("]","'")
                        sql ="SELECT a.id,a.brandName,LEFT(a.createdtime,10)'createdtime',(SUM(a.callsuccess)+SUM(a.callmiss))'calltotal',SUM(a.callsuccess)'callsuccess',SUM(a.callmiss) 'callmiss',ROUND(SUM(a.voicetime)/60,0)'voicetime',SUM(a.revenue)'revenue',a.vosIp,b.nickname,a.telco,CONCAT(ROUND((SUM(a.callmiss)/(SUM(a.callsuccess)+SUM(a.callmiss)))*100,2),' %')'callrate' FROM report a,partner b WHERE a.isBrand =0 AND a.createdtime >= CURDATE() and a.partner_code REGEXP "+str(partner_code_noibos)+"AND RIGHT(a.partner_code,5) = b.partner_code GROUP BY a.partner_code,a.brandName"
                    else :# admin
                        sql ="SELECT a.id,a.brandName,LEFT(a.createdtime,10)'createdtime',(SUM(a.callsuccess)+SUM(a.callmiss))'calltotal',SUM(a.callsuccess)'callsuccess',SUM(a.callmiss) 'callmiss',ROUND(SUM(a.voicetime)/60,0)'voicetime',SUM(a.revenue)'revenue',a.vosIp,b.nickname,a.telco,CONCAT(ROUND((SUM(a.callmiss)/(SUM(a.callsuccess)+SUM(a.callmiss)))*100,2),' %')'callrate' FROM report a,partner b WHERE a.isBrand =0 AND a.createdtime >= CURDATE() AND RIGHT(a.partner_code,5) = b.partner_code GROUP BY a.partner_code,a.brandName"
                    df = pd.read_sql(sql,db)
                    # cắt 10 ký tự đầu tiên begintime
                    df['createdtime'] = df['createdtime'].astype('str').str[:10]
                    json_records = df.to_json(orient ='records')
                    data = []
                    data = json.loads(json_records)
                    context = {'data': data,'code': 'OK'}
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
        logging.error(str(session) +"|response|" +str(e))
        return jsonify({'NOTOK': str(e)}),400
@brandname.route('/find_report_brandnames', methods=['GET'])
def find_report_brandnames():
    logging.info(str(session) +"|find_report_brandnames")
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
            partner = data["partner"]
            partner_code = data["partner_code"]
            logging.info(str(session) +"|find_report_brandnames|"+str(data))
            logging.info(str(session) +"|find_report_brandnames|"+str(request.args))
            starttime = request.args.get('starttime')
            endtime = request.args.get('endtime')
            endtime = str(endtime) + " 23:59:59"
            brandName = request.args.get('brandName')
            if str(brandName) == "":
                sql_brandName = ""
            else :
                sql_brandName = "AND a.brandName = '"+str(brandName)+"'"
            telco = request.args.get('telco')
            if str(telco) == "":
                sql_telco = ""
            else :
                sql_telco = "AND a.telco = '"+str(telco)+"'"
            nickname = request.args.get('nickname')
            if str(nickname) == "":
                sql_partner_code_find = ""
            else :
                sql_get_partner_code = "select partner_code from partner where nickname = '"+str(nickname)+"'"
                df_get_partner_code = pd.read_sql(sql_get_partner_code,db)
                partner_code_find = df_get_partner_code.iloc[0]['partner_code']
                sql_partner_code_find = "AND a.partner_code like '%"+str(partner_code_find)+"%'"
            sql_find = " and a.createdtime >= '"+str(starttime)+"' AND a.createdtime <= '"+str(endtime)+"'" +sql_brandName+sql_telco+sql_partner_code_find
            if True:
                currentDate = time.strftime("%Y-%m-%d %H:%M:%S")
                if str(timetoken) >= str(currentDate) :
                    if str(partner_code) != "None": #khách hàng
                        sql ="SELECT a.id,a.brandName,LEFT(a.createdtime,10)'createdtime',(SUM(a.callsuccess)+SUM(a.callmiss))'calltotal',SUM(a.callsuccess)'callsuccess',SUM(a.callmiss) 'callmiss',ROUND(SUM(a.voicetime)/60,0)'voicetime',SUM(a.revenue)'revenue',a.vosIp,b.nickname,a.telco,CONCAT(ROUND((SUM(a.callmiss)/(SUM(a.callsuccess)+SUM(a.callmiss)))*100,2),' %')'callrate' FROM report a,partner b WHERE a.isBrand =0 "+sql_find+" AND a.partner_code like '%"+str(partner_code)+"%' AND RIGHT(a.partner_code,5) = b.partner_code GROUP BY a.partner_code,a.brandName,LEFT(a.createdtime,10)"
                    elif str(partner) != "": # nội bộ
                        partner_code_noibos =[]
                        list_nickname = str(partner).replace("[","").replace("]","").replace("'","").replace(" ","").split(",")
                        for i in list_nickname:
                            # get partner_code
                            sql_nickname = "select partner_code from partner where nickname = '"+str(i)+"'"
                            df_nickname = pd.read_sql(sql_nickname,db)
                            partner_code_noibo = df_nickname.iloc[0]['partner_code']
                            partner_code_noibos.append(partner_code_noibo)
                        partner_code_noibos = str(partner_code_noibos).replace(" ","").replace("'","").replace(",","|").replace("[","'").replace("]","'")
                        sql ="SELECT a.id,a.brandName,LEFT(a.createdtime,10)'createdtime',(SUM(a.callsuccess)+SUM(a.callmiss))'calltotal',SUM(a.callsuccess)'callsuccess',SUM(a.callmiss) 'callmiss',ROUND(SUM(a.voicetime)/60,0)'voicetime',SUM(a.revenue)'revenue',a.vosIp,b.nickname,a.telco,CONCAT(ROUND((SUM(a.callmiss)/(SUM(a.callsuccess)+SUM(a.callmiss)))*100,2),' %')'callrate' FROM report a,partner b WHERE a.isBrand =0 "+sql_find+" and a.partner_code REGEXP "+str(partner_code_noibos)+"AND RIGHT(a.partner_code,5) = b.partner_code GROUP BY a.partner_code,a.brandName,LEFT(a.createdtime,10)"
                    else :# admin
                        sql ="SELECT a.id,a.brandName,LEFT(a.createdtime,10)'createdtime',(SUM(a.callsuccess)+SUM(a.callmiss))'calltotal',SUM(a.callsuccess)'callsuccess',SUM(a.callmiss) 'callmiss',ROUND(SUM(a.voicetime)/60,0)'voicetime',SUM(a.revenue)'revenue',a.vosIp,b.nickname,a.telco,CONCAT(ROUND((SUM(a.callmiss)/(SUM(a.callsuccess)+SUM(a.callmiss)))*100,2),' %')'callrate' FROM report a,partner b WHERE a.isBrand =0 "+sql_find+" AND RIGHT(a.partner_code,5) = b.partner_code GROUP BY a.partner_code,a.brandName,LEFT(a.createdtime,10)"
                    df = pd.read_sql(sql,db)
                    # cắt 10 ký tự đầu tiên begintime
                    df['createdtime'] = df['createdtime'].astype('str').str[:10]
                    json_records = df.to_json(orient ='records')
                    data = []
                    data = json.loads(json_records)
                    context = {'data': data,'code': 'OK'}
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
        logging.error(str(session) +"|response|" +str(e))
        return jsonify({'NOTOK': str(e)}),400
@brandname.route('/get_brandname_from_nickname', methods=['GET'])
def get_brandname_from_nickname():
    logging.info(str(session) +"|listbrandnames")
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
            partner = data["partner"]
            partner_code = data["partner_code"]
            logging.info(str(session) +"|listaccount|"+str(data))
            nickname = request.args.get('nickname')
            sql= "select name from brandname where registerby = '"+str(nickname)+"'"
            df = pd.read_sql(sql,db)
            json_records = df.to_json(orient ='records')
            data = []
            data = json.loads(json_records)
            context = {'data': data,'code': 'OK'}
            db.close()
            logging.info(str(session) +"|response|Thành công")
            return context
    except Exception as e:
        db.close()
        logging.error(str(session) +"|response|" +str(e))
        return jsonify({'NOTOK': str(e)}),400

@brandname.route('/get_nickname_co_brandname', methods=['GET'])
def get_nickname_co_brandname():
    logging.info(str(session) +"|listbrandnames")
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
            partner = data["partner"]
            partner_code = data["partner_code"]
            logging.info(str(session) +"|listaccount|"+str(data))
            if str(partner) != "": # nội bộ
                list_nickname = str(partner).replace("[","").replace("]","").replace("'","").replace(" ","").replace(",","|")
                sql= "select DISTINCT registerby from brandname where registerby REGEXP '"+str(list_nickname)+"'"
            else :# admin"
                sql= "select DISTINCT registerby from brandname "
            logging.info(str(session) +str(sql))
            df = pd.read_sql(sql,db)
            json_records = df.to_json(orient ='records')
            data = []
            data = json.loads(json_records)
            context = {'data': data,'code': 'OK'}
            db.close()
            logging.info(str(session) +"|response|Thành công")
            return context
    except Exception as e:
        db.close()
        logging.error(str(session) +"|response|" +str(traceback.format_exc()))
        return jsonify({'NOTOK': str(e)}),400