from flask import Blueprint,send_from_directory,send_file
from flask import request, jsonify
import json
import random
import string
import requests
from requests.auth import HTTPDigestAuth
import pandas as pd
from pandas import json_normalize
import pymysql
from flask_jsonpify import jsonpify
from datetime import datetime,date, timedelta
import os
import ast
import time
import numpy as np
import smtplib
import email
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from email import encoders
from email.header import Header
from itertools import chain
import traceback
import logging
import configparser
import threading
from function import *
from sqlalchemy import create_engine
config = configparser.ConfigParser()
config.read('config.conf')
dbconfig=config['db_config']['db_config']
ticket = Blueprint('ticket', __name__)
@ticket.route('/listticket', methods=['GET'])
def listticket():
    session=datetime.now()
    logging.info(str(session) +"|listticket")
    try :
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
                print(currentDate)
                if str(timetoken) >= str(currentDate) :
                    sql ="SELECT a.info 'test',a.ticket_id,a.telco,a.user_create,a.nickname1,a.nickname2,a.nickname3,a.partner,a.note,a.id,a.partner_code,a.ip,a.number'number',CASE WHEN a.callin = 0 THEN 'OFF' ELSE 'ON' END AS callin ,CASE WHEN a.callout = 0 THEN 'OFF' ELSE 'ON' END AS callout,CASE WHEN a.onnet = 0 THEN 'OFF' ELSE 'ON'  END AS onnet ,CASE WHEN a.offnet = 0 THEN 'OFF' ELSE 'ON' END AS offnet ,CASE WHEN a.status = 0 THEN 'Chờ xác nhận' WHEN a.status = 1 THEN 'Hoàn Thành' ELSE 'Hủy' END AS status FROM ticket a GROUP BY ticket_id"
                    with engine.connect() as con:
                        df = pd.read_sql(sql,con)
                        con.close()
                    df = df.astype(object).mask(df.isna(), np.nan)
                    df = df.fillna("")
                    # replace column value
                    df['test'] = df['test'].apply(lambda x: x.replace("&", '/n'))
                    df['ip'] = df['ip'].apply(lambda x: x.replace("&", '/n'))
                    df['test'] = df['test'].apply(ast.literal_eval)
                    json_records = df.to_json(orient ='records')
                    data = []
                    data = json.loads(json_records)
                    context = {'data': data,'code': 'OK'}
                    logging.info(str(session) +"|response|Thành công")
                    engine.dispose()
                    return context
                else :
                    logging.error(str(session) +"|response|token hết hạn")
                    engine.dispose()
                    return jsonify({'NOTOK': 'token hết hạn'})
            else :
                logging.error(str(session) +"|response|Không có quyền")
                engine.dispose()
                return jsonify({'NOTOK': 'khong co quyen'})
    except Exception as e:
        logging.error(str(session) +"|listticket error |"+str(traceback.format_exc()))
        return jsonify({'NOTOK': str(traceback.format_exc())}),400
@ticket.route('/deleteticket', methods=['DELETE'])
def deleteticket():
    try :
        session=datetime.now()
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
                    sqlcheck= "select * from ticket where id ='"+str(id)+"'"
                    with engine.connect() as con:
                        df = pd.read_sql(sqlcheck,con)
                        con.close()
                    if df.empty:
                        return jsonify({'OK': 'id không tồn tại'})
                    sql="delete from ticket where id = '"+str(id)+"'"
                    sqllog = "insert into log (name,api,content,type) value ('"+str(nameuser)+"','deleteticket','id = "+str(id)+"','DELETE')"
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
        logging.error(str(session) +"|deleteticket error |"+str(traceback.format_exc()))
        return jsonify({'Lỗi': str(traceback.format_exc())})
@ticket.route('/insertticket', methods=['POST'])
def insertticket():
    try:
        session=datetime.now()
        engine = create_engine(dbconfig)
        data = group()
        if str(data)== "chua truyen token" :
            logging.error(str(session) +"|response|chua truyen token")
            return jsonify({'NOTOK': str(data)})
        else:
            timetoken = data["expireddate"]
            group_name = data["group_name"]
            nameuser = data["user"]
            if True:
                currentDate = time.strftime("%Y-%m-%d %H:%M:%S")
                if str(timetoken) >= str(currentDate) :
                    record = json.loads(request.data)
                    logging.info(str(session) +"|insertticket|"+str(record))
                    nickname1 = record["nickname1"]
                    nickname2 = record["nickname2"]
                    nickname3 = record["nickname3"]
                    ip = record["ip"]
                    # ip = ip.replace("\n",",")
                    info = record["test"]
                    logging.info(str(session) +"|info|"+str(info))
                    # info = str(info).replace("\n", "")
                    #convert info to list with ast
                    # info = ast.literal_eval(info)
                    note = record["note"]
                    # xử lý partner code
                    partner_code1=""
                    partner_code2=""
                    partner_code3=""
                    if str(nickname1) != "":
                        try:
                            sql_check_partner1 = "select * from partner where nickname = '"+str(nickname1)+"'"
                        except Exception as e:
                            logging.error(str(session) +"|sql_check_partner1|"+str(traceback.format_exc()))
                            return jsonify({'NOTOK': str(traceback.format_exc())})
                        logging.info(str(session) +"|sql_check_partner1|"+str(sql_check_partner1))
                        with engine.connect() as con:
                            df1 = pd.read_sql(sql_check_partner1,con)
                            con.close()
                        if df1.empty:
                            return jsonify({'NOTOK': 'nickname1 không tồn tại'})
                        partner_code1 = df1.iloc[0]['partner_code']
                        # nickname1= df1.iloc[0]['nickname']
                        logging.info(str(session) +"|partner_code1|"+str(partner_code1))
                    if str(nickname2) != "":
                        sql_check_partner2 = "select * from partner where nickname = '"+str(nickname2)+"'"
                        logging.info(str(session) +"|sql_check_partner2|"+str(sql_check_partner2))
                        with engine.connect() as con:
                            df2 = pd.read_sql(sql_check_partner2,con)
                            con.close()
                        if df2.empty:
                            return jsonify({'NOTOK': 'nickname2 không tồn tại'})
                        partner_code2 = df2.iloc[0]['partner_code']
                        nickname2= df2.iloc[0]['nickname']
                        logging.info(str(session) +"|partner_code2|"+str(partner_code2))
                    if str(nickname3) != "":
                        sql_check_partner3 = "select * from partner where nickname = '"+str(nickname3)+"'"
                        logging.info(str(session) +"|sql_check_partner3|"+str(sql_check_partner3))
                        with engine.connect() as con:
                            df3 = pd.read_sql(sql_check_partner3,con)
                            con.close()
                        if df3.empty:
                            return jsonify({'NOTOK': 'nickname3 không tồn tại'})
                        partner_code3 = df3.iloc[0]['partner_code']
                        nickname3= df3.iloc[0]['nickname']
                        logging.info(str(session) +"|partner_code3|"+str(partner_code3))
                    chuoi_partner_code2 =""
                    if str(partner_code2) != "":
                        chuoi_partner_code2 = "-"+str(partner_code2)
                    chuoi_partner_code3 =""
                    if str(partner_code3) != "":
                        chuoi_partner_code3 = "-"+str(partner_code3)
                    partner_code_full = str(partner_code1)+str(chuoi_partner_code2)+str(chuoi_partner_code3)
                    # check partner group có tồn tại hay không
                    sql_check_partner_group = "select * from partner_group where partner1 = '"+str(partner_code1)+"' and partner2 = '"+str(partner_code2)+"' and partner3 = '"+str(partner_code3)+"'"
                    logging.info(str(session) +"|sql_check_partner_group|"+str(sql_check_partner_group))
                    with engine.connect() as con:
                        df = pd.read_sql(sql_check_partner_group,con)
                        con.close()
                    if df.empty:
                        # partner_code_full = str(partner_code1)+"-"+str(partner_code2)+"-"+str(partner_code3)
                        sql_insert_partner_group = "insert into partner_group (partner1,partner2,partner3,partner_code) value ('"+str(partner_code1)+"','"+str(partner_code2)+"','"+str(partner_code3)+"','"+str(partner_code_full)+"')"
                        logging.info(str(session) +"|sql_insert_partner_group|"+str(sql_insert_partner_group))
                        with engine.connect() as con:
                            con.execute(sql_insert_partner_group)
                            logging.info(str(session) +"|sql_insert_partner_group|thành công")
                            con.close()
                    sql_take_partner_code = "select partner_code from partner_group where partner1 = '"+str(partner_code1)+"' and partner2 = '"+str(partner_code2)+"' and partner3 = '"+str(partner_code3)+"'"
                    with engine.connect() as con:
                        df = pd.read_sql(sql_take_partner_code,con)
                        con.close()
                    partner_code = df.iloc[0]['partner_code']
                    chuoi_nickname2 =""
                    if str(nickname2) != "":
                        chuoi_nickname2 = "-"+str(nickname2)
                    chuoi_nickname3 =""
                    if str(nickname3) != "":
                        chuoi_nickname3 = "-"+str(nickname3)
                    nickname_full = str(nickname1) +str(chuoi_nickname2)+str(chuoi_nickname3)
                    logging.info(str(session) +"|nickname_full|"+str(nickname_full))
                    #sinh chuỗi ngẫu nhiên 10 ký tự
                    def randomStringDigits(stringLength=10):
                        """Generate a random string of letters and digits """
                        lettersAndDigits = string.ascii_letters + string.digits
                        return ''.join(random.choice(lettersAndDigits) for i in range(stringLength))
                    ticket_id = randomStringDigits(10)
                    partmail = ""
                    for i in range(len(info)):
                    #lấy brandname
                        brand_name = info[i]["brand_name"]
                        number = info[i]["number"]
                        telco = info[i]["telco"]
                        callins = info[i]["callin"]
                        if str(callins) =="ON":
                            callin = "1"
                        else :
                            callin = "0"
                        callouts = info[i]["callout"]
                        if str(callouts) =="ON":
                            callout = "1"
                        else :
                            callout = "0"
                        onnets = info[0]["onnet"]
                        if str(onnets) =="ON":
                            onnet = "1"
                        else :
                            onnet = "0"
                        offnets = info[i]["offnet"]
                        if str(offnets) =="ON":
                            offnet = "1"
                        else :
                            offnet = "0"
                        partmail =str(partmail) + """<tr>
                                 <td style="border: 1px solid black;">""" +str(telco)+ """</td>
                                 <td style="border: 1px solid black;">""" +str(number)+ """</td>
                                 <td style="border: 1px solid black;">""" +str(brand_name)+ """</td>
                                 <td style="border: 1px solid black;">""" +str(callouts)+ """</td>
                                 <td style="border: 1px solid black;">""" +str(callins)+ """</td>
                                 <td style="border: 1px solid black;">""" +str(onnets)+ """</td>
                                 <td style="border: 1px solid black;">""" +str(offnets)+ """</td>
                                 </tr>"""
                        for i in info :
                            i['number']=i['number'].replace('\n','&')
                        ip = str(ip).replace("\n","&")
                        sql="insert into ticket(info,telco,brand_name,number,user_create,ticket_id,nickname1,nickname2,nickname3,partner,partner_code,note,ip,callin,callout,onnet,offnet,status,createdtime,updatedtime) value (\""+str(info)+"\",'"+str(telco)+"','"+str(brand_name)+"','"+str(number)+"','"+str(nameuser)+"','"+str(ticket_id)+"','"+str(nickname1)+"','"+str(nickname2)+"','"+str(nickname3)+"','"+str(nickname_full)+"','"+str(partner_code_full)+"','"+str(note)+"','"+str(ip)+"','"+str(callin)+"','"+str(callout)+"','"+str(onnet)+"','"+str(offnet)+"','0','"+str(session)+"','"+str(session)+"')"
                        logging.info(str(session) +"|sql|"+str(sql))
                        with engine.connect() as con:
                            con.execute(sql)
                            logging.info(str(session) +"|insert ok|")
                            con.close()
                    engine.dispose()
                    # send mail
                    body = """<html><body><h1>Dear các bạn N.O.C </h1><h2>Thông tin ticket : """ +str(ticket_id)+ """</h2>
                             <table style="border-collapse: collapse;width:100%">
                                 <tbody>
                                 <tr>
                                 <th rowspan="1" colspan="8" style="border: 1px solid black;">A .THÔNG TIN KẾT NỐI</th>
                                 </tr>
                                 <tr>
                                 <td style="border: 1px solid black;">1</td>
                                 <th rowspan="1" colspan="1" style="border: 1px solid black;">Mã khách hàng</th>
                                 <th rowspan="1" colspan="5" style="border: 1px solid black;">""" +str(partner_code_full)+ """</th>
                                 </tr>
 								<tr>
                                 <td style="border: 1px solid black;">2</td>
                                 <th rowspan="1" colspan="1" style="border: 1px solid black;">Tên khách hàng</th>
                                 <th rowspan="1" colspan="5" style="border: 1px solid black;">""" +str(nickname_full)+ """</th>
                                 </tr>
                                 <tr>
                                 <td style="border: 1px solid black;">3</td>
                                 <th colspan="1" style="border: 1px solid black;">Người tạo Ticket</th>
                                 <th colspan="5" style="border: 1px solid black;">""" +str(nameuser)+ """</th>
                                 </tr>
                                 <tr>
                                 <th rowspan="1" colspan="8" style="border: 1px solid black;">B .THÔNG SỐ KHAI BÁO</th>
                                 </tr>
                                 <tr>
                                 <td style="border: 1px solid black;">1</td>
                                 <th rowspan="1" colspan="1" style="border: 1px solid black;">Ngày đấu nối</th>
                                 <th rowspan="1" colspan="5" style="border: 1px solid black;">""" +str(session)+ """</th>
                                 </tr>
                                 <tr>
                                 <td style="border: 1px solid black;">2</td>
                                 <th rowspan="1" colspan="1" style="border: 1px solid black;">IP</th>
                                 <th rowspan="1" colspan="5" style="border: 1px solid black;">""" +str(ip)+ """</th>
                                 </tr>
                                 <tr>
                                 <th rowspan="1" colspan="8" style="border: 1px solid black;">B .THÔNG TIN KÍCH HOẠT</th>
                                 </tr>
                                 <tr>
									<th rowspan="1" colspan="1" style="border: 1px solid black;">Hướng kết nối</th>
                                 <th rowspan="1" colspan="1" style="border: 1px solid black;">Số thuê bao</th>
                                 <th rowspan="1" colspan="1" style="border: 1px solid black;">Brandname</th>
                                 <th rowspan="1" colspan="1" style="border: 1px solid black;">Call-out</th>
                                 <th rowspan="1" colspan="1" style="border: 1px solid black;">Call-in</th>
                                 <th rowspan="1" colspan="1" style="border: 1px solid black;">On-net</th>
                                 <th rowspan="1" colspan="1" style="border: 1px solid black;">Off-net</th>
                                 </tr>
                                 """ + str(partmail)+ """                                
                             </tbody>
                             </table>
                             </body>
                             </html>"""
                    subject = "Thông tin ticket :" +str(ticket_id) 
                    t1 = threading.Thread(target=sendmail, args=(subject,body,))
                    t1.start()
                    return jsonify({'OK': 'OK'})
                else :
                    engine.dispose()
                    return jsonify({'NOTOK': 'token hết hạn','code':401}),401
            else :
                engine.dispose()
                return jsonify({'NOTOK': 'khong co quyen'})  
    except Exception as e:
        # engine.dispose()
        logging.info(str(session) +"|Lỗi|"+str(traceback.format_exc()))
        return jsonify({'Lỗi': str(traceback.format_exc())}),400
@ticket.route('/updateticket', methods=['POST'])
def updateticket():
    try:
        session=datetime.now()
        engine = create_engine(dbconfig)
        data = group()
        if str(data)== "chua truyen token" :
            return jsonify({'NOTOK': str(data)})
        else:
            timetoken = data["expireddate"]
            group_name = data["group_name"]
            nameuser = data["user"]
            logging.info(str(session) +"|group_name|"+str(group_name))
            if True:
                currentDate = time.strftime("%Y-%m-%d %H:%M:%S")
                print(currentDate)
                if str(timetoken) >= str(currentDate) :
                    record = json.loads(request.data)
                    logging.info(str(session) +"|record|"+str(record))
                    ticket_id = record["ticket_id"]
                    status = record["status"]
                    if str(status) == "Hoàn thành":
                        status = 1
                    elif str(status) == "Hủy":
                        status = 2
                    # check ticket
                    sql_check_ticket = "select * from ticket where ticket_id = '"+str(ticket_id)+"' limit 1"
                    logging.info(str(session) +"|sql_check_ticket|"+str(sql_check_ticket))
                    with engine.connect() as con:
                        df = pd.read_sql(sql_check_ticket,con)
                        con.close()
                    if df.empty:
                        logging.info(str(session) +"|Lỗi|"+str("ticket không tồn tại"))
                    user_create = df.iloc[0]["user_create"]
                    ticket_id= df.iloc[0]["ticket_id"]
                    #check email user create
                    sql_check_email = "select email from user where username = '"+str(user_create)+"'"
                    with engine.connect() as con:
                        df_email = pd.read_sql(sql_check_email,con)
                        con.close()
                    email = df_email.iloc[0]["email"]
                    sql_update = "update ticket set status = '"+str(status)+"' where ticket_id = '"+str(ticket_id)+"'"
                    logging.info(str(session) +"|sql_update|"+str(sql_update))
                    with engine.connect() as con:
                        try:
                            con.execute(sql_update)
                            logging.info(str(session) +"|OK|"+str("update ticket thành công"))
                            con.close()
                        except Exception as e:
                            logging.info(str(session) +"|Lỗi|"+str(traceback.format_exc()))
                            return jsonify({'NOTOK': str(traceback.format_exc())}),400
                    # send mail
                    if str(status) == "1":
                        body = """<html><body><h1>Dear bạn """ +str(user_create)+ """ </h1><h2>Thông tin ticket : """ +str(ticket_id)+ """</h2>
                                <h2>Ticket : """ +str(ticket_id)+ """ của bạn đã được cập nhật trạng thái thành công</h2>
                                </body>
                                </html>"""
                    elif str(status) == "2":
                        body = """<html><body><h1>Dear bạn """ +str(user_create)+ """ </h1><h2>Thông tin ticket : """ +str(ticket_id)+ """</h2>
                                <h2>Ticket : """ +str(ticket_id)+ """ của bạn đã bị hủy </h2>
                                </body>
                                </html>"""
                    subject = "Cập nhật thông tin ticket :" +str(ticket_id) 
                    t1 = threading.Thread(target=sendmail_update, args=(subject,email,body,))
                    t1.start()
                    return jsonify({'OK': 'OK'})
                else :
                    engine.dispose()
                    return jsonify({'NOTOK': 'token hết hạn','code':401}),401
            else :
                engine.dispose()
                return jsonify({'NOTOK': 'khong co quyen'})  
    except Exception as e:
        # engine.dispose()
        logging.info(str(session) +"|error|"+str(traceback.format_exc()))
        return jsonify({'Lỗi': str(traceback.format_exc())}),400
@ticket.route('/find_ticket', methods=['GET'])
def find_ticket():
    db = pymysql.connect(host="172.17.0.1",    # your host, usually localhost
                     user="leeoncrm",         # your username
                     passwd="41XmKsO3NBgHPwv",  # your password
                     db="leeon_crm",
                     port=3306
                     )
    isdn = request.args.get('isdn')
    telcoid = request.args.get('telcoid')
    partnerid = request.args.get('partnerid')
    try:
        if str(isdn)=="" :
            isdn_query=""
        else:
            isdn_query="isdn = '"+str(isdn) +"' and "
        if str(telcoid)=="" :
            telcoid_query=""
        else:
            telcoid_query="telcoid = '"+str(telcoid) +"' and "
        if str(partnerid)=="" :
            partnerid_query=""
        else:
            partnerid_query="partnerid = '"+str(partnerid)+"' and "
        if str(isdn)=="" and str(telcoid)=="" and str(partnerid)=="" :
            query= ""
        else :
            query= "where "
        query = query+isdn_query+telcoid_query+partnerid_query+"id is not null"
        sql ="SELECT f.name'brandname',a.id,a.isdn,case when a.isdntype = '1' then 'Sip Trunk' when a.isdntype ='2' then 'Proconnect' when a.isdntype = '3' then '3C' else 'ko biet' end as isdntype,a.note,a.dateassignedtoleeon,a.dateassignedtopartner,a.cfu,b.name'vosip',c.nickname,d.name'telco',e.isdn'numberowner',case when a.status = '1'  then 'Hoạt động' when a.status = '2'  then 'Khóa lần 1' when a.status = '3'  then 'Khóa lần 2' end as status FROM (select * from ticket "+str(query)+") a left join (select * from vos) b on a.vosip=b.id left join (select * from partner) c on a.partnerid=c.id left join (select * from telco) d on a.telcoid=d.id left join (select * from number_owner) e on a.numberownerid=e.id left join (select * from brandname) f on a.brandnameid=f.id"
        df = pd.read_sql(sql,db)
    except:
        return jsonify({'NOTOK': 'chua dien partner'})
    print(sql)
    df['dateassignedtoleeon'] = df['dateassignedtoleeon'].astype('str')
    df['dateassignedtopartner'] = df['dateassignedtopartner'].astype('str')
    json_records = df.to_json(orient ='records')
    data = []
    data = json.loads(json_records)
    context = {'data': data,'code': 'OK'}
    db.close() 
    return context
@ticket.route('/select_ticket_taxcode', methods=['GET'])
def select_ticket_taxcode():

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
            if True:
                currentDate = time.strftime("%Y-%m-%d %H:%M:%S")
                print(currentDate)
                if str(timetoken) >= str(currentDate) :
                    partner = request.args.get('nickname')
                    sqlpartner = "select id from partner where nickname = '"+str(partner)+"'"
                    df = pd.read_sql(sqlpartner,db)
                    if df.empty :
                        partnerid= ""
                    else :
                        partnerid= df.iloc[0,0]
                    sql ="SELECT taxcode FROM partner WHERE nickname = '"+str(partner)+"'"
                    df = pd.read_sql(sql,db)
                    df = df.astype(object).mask(df.isna(), np.nan)
                    df = df.fillna("")
                    json_records = df.to_json(orient ='records')
                    data = []
                    data = json.loads(json_records)
                    context = {'data': data,'code': 'OK'}
                    db.commit()
                    db.close() 
                    return context
                else :
                    return jsonify({'NOTOK': 'token hết hạn'})
            else :
                return jsonify({'NOTOK': 'khong co quyen'})
@ticket.route('/select_ticket_ip', methods=['GET'])
def select_ticket_ip():

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
            if True:
                currentDate = time.strftime("%Y-%m-%d %H:%M:%S")
                print(currentDate)
                if str(timetoken) >= str(currentDate) :
                    partner = request.args.get('nickname')
                    sqlpartner = "select id from partner where nickname = '"+str(partner)+"'"
                    df = pd.read_sql(sqlpartner,db)
                    if df.empty :
                        partnerid= ""
                    else :
                        partnerid= df.iloc[0,0]
                    sql ="SELECT b.ip FROM (SELECT * FROM partner WHERE nickname = '"+str(partner)+"') a LEFT JOIN ip b ON a.id = b.partnerid"
                    df = pd.read_sql(sql,db)
                    df = df.astype(object).mask(df.isna(), np.nan)
                    df = df.fillna("")
                    json_records = df.to_json(orient ='records')
                    data = []
                    data = json.loads(json_records)
                    context = {'data': data,'code': 'OK'}
                    db.commit()
                    db.close() 
                    return context
                else :
                    return jsonify({'NOTOK': 'token hết hạn'})
            else :
                return jsonify({'NOTOK': 'khong co quyen'})
@ticket.route('/select_ticket_isdn', methods=['GET'])
def select_ticket_isdn():

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
            if True:
                currentDate = time.strftime("%Y-%m-%d %H:%M:%S")
                print(currentDate)
                if str(timetoken) >= str(currentDate) :
                    partner = request.args.get('nickname')
                    sqlpartner = "select id from partner where nickname = '"+str(partner)+"'"
                    df = pd.read_sql(sqlpartner,db)
                    if df.empty :
                        partnerid= ""
                    else :
                        partnerid= df.iloc[0,0]
                    sql ="SELECT c.isdn FROM (SELECT * FROM partner WHERE nickname = '"+str(partner)+"') a LEFT JOIN number_member c ON c.partnerid =a.id"
                    df = pd.read_sql(sql,db)
                    df = df.astype(object).mask(df.isna(), np.nan)
                    df = df.fillna("")
                    json_records = df.to_json(orient ='records')
                    data = []
                    data = json.loads(json_records)
                    context = {'data': data,'code': 'OK'}
                    db.commit()
                    db.close() 
                    return context
                else :
                    return jsonify({'NOTOK': 'token hết hạn'})
            else :
                return jsonify({'NOTOK': 'khong co quyen'})