from flask import Blueprint,request, jsonify
import json
import requests
import pandas as pd
from pandas import json_normalize
import pymysql
from flask_jsonpify import jsonpify
from datetime import datetime,date, timedelta
from function import group
import time
import re
import logging
import configparser
import traceback
import random
from sqlalchemy import create_engine ,text
partner = Blueprint('partner', __name__)
config = configparser.ConfigParser()
config.read('config.conf')
dbconfig=config['db_config']['db_config']
@partner.route('/listpartner', methods=['GET'])
def listpartner():
    session=datetime.now()
    session = str(session).replace(" ","").replace(":","").replace(".","")
    logging.info(str(session) +"|---------------listpartner--------------------")
    try:
        engine = create_engine(dbconfig)
        data = group()
        if str(data)== "chua truyen token" :
            return jsonify({'NOTOK': str(data)})
        else:
            timetoken = data["expireddate"]
            group_name = data["group_name"]
            nameuser = data["user"]
            partner = data["partner"]
            partner_code = data["partner_code"]
            logging.info(str(session) +"|data|"+str(data))
            logging.info(str(session) +"|partner_code|"+str(partner_code))
            if str(partner) != "":
                partner= str(partner).replace("[","").replace("]","").replace("'","")
                partner =re.split('[,;/ ]+', partner)
                print("---------")
                nickname=""
                for i in partner:
                    nickname=nickname+",'"+str(i)+"'"
                nickname=nickname[1:len(nickname)]
                logging.info(session + " | nickname |" + nickname)
            if True:
                currentDate = time.strftime("%Y-%m-%d %H:%M:%S")
                if str(timetoken) >= str(currentDate) :
                    if str(partner) =="" and str(partner_code) =="None":
                        # sql ="select *, 'Click'`a` from partner order by createdtime desc"
                        sql = "SELECT a.*,b.nickname'daily','Click'`a` FROM (SELECT * FROM partner ORDER BY createdtime DESC) a LEFT JOIN (SELECT * FROM agency) b ON a.agency_id = b.id"
                    elif str(partner) !="" and str(partner_code) =="None":
                        # sql ="select *, 'Click'`a` from partner where nickname in ("+str(nickname)+")"
                        sql = "SELECT a.*,b.nickname'daily','Click'`a` FROM (SELECT * FROM partner where nickname in ("+str(nickname)+")) a LEFT JOIN (SELECT * FROM agency) b ON a.agency_id = b.id"
                    else :
                        #lấy 5 số cuối của partner_code
                        partner_code =str(partner_code)[-5:]
                        sql ="(select *, 'Click'`a` from partner where partner_code in (select partner2 from partner_group where partner1 = ("+str(partner_code)+"))) union (select *, 'Click'`a` from partner where partner_code in (select partner3 from partner_group where partner1 = ("+str(partner_code)+"))) union (select *, 'Click'`a` from partner where partner_code ='"+str(partner_code)+"')"
                    logging.info(str(session) +"|listpartner|"+str(sql))
                    with engine.connect() as con:
                        df = pd.read_sql(sql,con)
                        con.close()
                    df['createdtime'] = df['createdtime'].astype('str')
                    # lấy 10 ký tự đầu cột createdtime
                    df['createdtime'] = df['createdtime'].str[:10]
                    json_records = df.to_json(orient ='records')
                    data = []
                    data = json.loads(json_records)
                    context = {'data': data,'code': 'OK'}
                    logging.info(str(session) +"|response | Thành công")
                    engine.dispose()
                    return context
                else :
                    engine.dispose()
                    logging.error(str(session) +"|response|token hết hạn")
                    return jsonify({'NOTOK': 'token hết hạn','code':401}),401
            else :
                engine.dispose()
                logging.error(str(session) +"|response|Không có quyền")
                return jsonify({'NOTOK': 'khong co quyen'}),400
    except Exception as e:
        logging.error(str(session) +"|response|" +str(traceback.format_exc()))
        return jsonify({'NOTOK': str(traceback.format_exc())}),400
@partner.route('/saovang_listpartner', methods=['GET'])
def saovang_listpartner():
    session=datetime.now()
    session = str(session).replace(" ","").replace(":","").replace(".","")
    logging.info(str(session) +"|---------------listpartner--------------------")
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
            partner_code = data["partner_code"]
            logging.info(str(session) +"|data|"+str(data))
            logging.info(str(session) +"|partner_code|"+str(partner_code))
            if True:
                currentDate = time.strftime("%Y-%m-%d %H:%M:%S")
                if str(timetoken) >= str(currentDate) :
                    if str(group_name) == "CUSTOMER":
                        sql ="select *, 'Click'`a` from partner where partner_code = "+str(partner_code)
                    else :
                        sql_partner_code = "SELECT partner_code from partner WHERE agency_id= 1"
                        df_partnercode = pd.read_sql(sql_partner_code,db)
                        # lưu tất cả partner1 , partner2 , partner3 vào 1 list
                        list_partner = df_partnercode['partner_code'].tolist()
                        logging.info("list_partner : "+ str(list_partner))
                        list_partner = str(list_partner).replace("[","(").replace("]",")")
                        sql ="select *, 'Click'`a` from partner where partner_code in "+list_partner+" order by createdtime desc"
                    logging.info(str(session) +"|listpartner|"+str(sql))
                    df = pd.read_sql(sql,db)
                    df['createdtime'] = df['createdtime'].astype('str')
                    json_records = df.to_json(orient ='records')
                    data = []
                    data = json.loads(json_records)
                    context = {'data': data,'code': 'OK'}
                    logging.info(str(session) +"|response | Thành công")
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
        logging.error(str(session) +"|response|" +str(traceback.format_exc()))
        return jsonify({'NOTOK': str(traceback.format_exc())}),400
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
            logging.info(str(session) +"|deletepartner|"+str(data))
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
    db = pymysql.connect(host="172.17.0.1",    # your host, usually localhost
                     user="leeoncrm",         # your username
                     passwd="41XmKsO3NBgHPwv",  # your password
                     db="leeon_crm",
                     port=3306
                     )
    cursor = db.cursor()
    session=datetime.now()
    logging.info(str(session) +"|insertpartner")
    try:
        engine = create_engine(dbconfig)
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
                    company = record["company"]
                    taxcode = record["taxcode"]
                    nickname = record["nickname"]
                    represent = record["represent"]
                    email = record["email"]
                    address = record["address"]
                    phone = record["phone"]
                    website= record["website"]
                    bank= record["bank"]
                    bankbranch= record["bankbranch"]
                    bankaccount= record["bankaccount"]
                    ip= record["ip"]
                    logging.info(str(session) +"|IP|"+str(ip))
                    ipss = ip.split(":")
                    ips = ipss[0]
                    try :
                        port = ipss[1]
                    except :
                        port = ""
                    logging.info(str(session) +"|IP|"+str(ips))
                    logging.info(str(session) +"|port|"+str(port))
                    name_contact= record["name_contact"]
                    email_contact= record["email_contact"]
                    phone_contact= record["phone_contact"]
                    position_contact= record["position_contact"]
                    department= record["department"]
                    daily= record["daily"]
                    if str(daily) !="":
                        sqldaily = "SELECT id FROM agency WHERE company = '"+str(daily)+"'"
                        df_daily = pd.read_sql(sqldaily,db)
                        id_daily = df_daily.iloc[0]["id"]
                    else :
                        id_daily =""
                    #partner_code
                    start=10001
                    stop=99999
                    while True :
                        partner_code= random.randint(start, stop)
                        sqlcheck_partner_code = "select * from number_member where partner_code = '"+str(partner_code)+"'"
                        with engine.connect() as con:
                            try:
                                df_check_partner_code = pd.read_sql(sqlcheck_partner_code,con)
                                con.close()
                            except Exception as e:
                                engine.dispose()
                                logging.error(str(session) +"|response|" +str(traceback.format_exc()))
                                return jsonify({'NOTOK': str(e)}),400
                        if df_check_partner_code.empty:
                            break
                    now = datetime.now()
                    current_time = now.strftime("%Y-%m-%d %H:%M:%S")
                    sqlcheck= "select * from partner where nickname ='"+str(nickname)+"'"
                    sqlcheck = text(sqlcheck)
                    logging.info(str(session) +"|sqlcheck|"+str(sqlcheck))
                    with engine.connect() as con:
                        try:
                            df = pd.read_sql(sqlcheck,con)
                            con.close()
                        except Exception as e:
                            engine.dispose()
                            logging.error(str(session) +"|response|" +str(traceback.format_exc()))
                            return jsonify({'NOTOK': str(e)}),400
                    if not df.empty:
                        engine.dispose()
                        logging.error(str(session) +"|response|Nickname đã tồn tại")
                        return jsonify({'NOTOK': 'Nickname đã tồn tại'}),400
                    sql="insert into partner(company,nickname,taxcode,address,represent,phone,email,website,createdtime,updatedtime,bankaccount,bank,bankbranch,partner_code,ip,agency_id) value ('"+str(company)+"','"+str(nickname)+"','"+str(taxcode)+"','"+str(address)+"','"+str(represent)+"','"+str(phone)+"','"+str(email)+"','"+str(website)+"','"+str(current_time)+"','"+str(current_time)+"','"+str(bankaccount)+"','"+str(bank)+"','"+str(bankbranch)+"','"+str(partner_code)+"','"+str(ip)+"','"+str(id_daily)+"')"
                    logging.info(str(session) +"|query|"+str(sql))
                    try:
                        cursor.execute(sql)
                        db.commit()
                        db.close()
                    except Exception as e:
                        engine.dispose()
                        logging.error(str(session) +"|response|" +str(traceback.format_exc()))
                        return jsonify({'NOTOK': str(e)}),400
                    sql_take_id = "select id from partner where nickname ='"+str(nickname)+"'"
                    sql_take_id = text(sql_take_id)
                    with engine.connect() as con:
                        try:
                            df_take_id = pd.read_sql(sql_take_id,con)
                            con.close()
                        except Exception as e:
                            engine.dispose()
                            logging.error(str(session) +"|response|" +str(traceback.format_exc()))
                            return jsonify({'NOTOK': str(e)}),400
                    id = df_take_id.iloc[0]['id']
                    sql_insert_contact="insert into contact(partnerid,name,email,phone,position,department) value ('"+str(id)+"','"+str(name_contact)+"','"+str(email_contact)+"','"+str(phone_contact)+"','"+str(position_contact)+"','"+str(department)+"')"
                    sql_insert_contact = text(sql_insert_contact)
                    with engine.connect() as con:
                        try:
                            con.execute(sql_insert_contact)
                            con.close()
                        except Exception as e:
                            engine.dispose()
                            logging.error(str(session) +"|response|" +str(traceback.format_exc()))
                            return jsonify({'NOTOK': str(e)}),400
                    #insert bảng ip
                    sql_insert_ip="insert into ip(partnerid,ip,port) value ('"+str(id)+"','"+str(ips)+"','"+str(port)+"')"
                    sql_insert_ip = text(sql_insert_ip)
                    with engine.connect() as con:
                        try:
                            con.execute(sql_insert_ip)
                            con.close()
                        except Exception as e:
                            engine.dispose()
                            logging.error(str(session) +"|response|" +str(traceback.format_exc()))
                            return jsonify({'NOTOK': str(e)}),400
                    logging.info(str(session) +"|response|Thành công")
                    return jsonify({'OK': 'OK'})
                    engine.dispose()
                else :
                    logging.error(str(session) +"|response|token hết hạn")
                    return jsonify({'NOTOK': 'token hết hạn','code':401}),401
            else :
                logging.error(str(session) +"|response|Không có quyền")
                return jsonify({'NOTOK': 'khong co quyen'}),400
    except Exception as e:
        logging.error(str(session) +"|response|" +str(traceback.format_exc()))
        return jsonify({'NOTOK': str(e)}),400
@partner.route('/updatepartner', methods=['POST'])
def updatepartner():
    session=datetime.now()
    logging.info(str(session) +"|updatepartner")
    try :
        engine = create_engine(dbconfig)
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
                    company = record["company"]
                    taxcode = record["taxcode"]
                    nickname = record["nickname"]
                    represent = record["represent"]
                    email = record["email"]
                    address = record["address"]
                    phone = record["phone"]
                    website= record["website"]
                    bank= record["bank"]
                    bankbranch= record["bankbranch"]
                    bankaccount= record["bankaccount"]
                    ip= record["ip"]
                    logging.info(str(session) +"|IP|"+str(ip))
                    ipss = ip.split(":")
                    ips = ipss[0]
                    port = ipss[1]
                    logging.info(str(session) +"|IP|"+str(ips))
                    logging.info(str(session) +"|port|"+str(port))
                    name_contact= record["name_contact"]
                    email_contact= record["email_contact"]
                    phone_contact= record["phone_contact"]
                    position_contact= record["position_contact"]
                    department= record["department"]
                    daily= record["daily"]
                    sqldaily = "SELECT id FROM agency WHERE partner_code = (SELECT partner_code FROM partner WHERE company = '"+str(daily)+"')"
                    df_daily = pd.read_sql(sqldaily,db)
                    id_daily = df_daily.iloc[0]["id"]
                    now = datetime.now()
                    current_time = now.strftime("%Y-%m-%d %H:%M:%S")
                    sqlcheck= "select * from partner where id ='"+str(id)+"'"
                    logging.info(str(session) +"|query|"+str(sqlcheck))
                    with engine.connect() as con:
                        try:
                            df = pd.read_sql(sqlcheck,con)
                            con.close()
                            logging.info(str(session) +"|query|"+str(df))
                        except Exception as e:
                            engine.dispose()
                            logging.error(str(session) +"|response|" +str(traceback.format_exc()))
                            return jsonify({'NOTOK': str(e)}),400
                    if df.empty:
                        logging.error(str(session) +"|response|Không tìm thấy partner")
                        return jsonify({'NOTOK': 'Không tìm thấy partner'}),400
                    #update bảng ip
                    sql_update_ip="update ip set ip ='"+str(ips)+"',port ='"+str(port)+"' where partnerid ='"+str(id)+"'"
                    logging.info(str(session) +"|query|"+str(sql_update_ip))
                    with engine.connect() as con:
                        try:
                            con.execute(sql_update_ip)
                            con.close()
                        except Exception as e:
                            engine.dispose()
                            logging.error(str(session) +"|response|" +str(traceback.format_exc()))
                            return jsonify({'NOTOK': str(e)}),400
                    sql="update partner set company ='"+str(company)+"',address ='"+str(address)+"',email = '"+str(email)+"',nickname = '"+str(nickname)+"',phone = '"+str(phone)+"',represent = '"+str(represent)+"',website = '"+str(website)+"',ip = '"+str(ip)+"',taxcode='"+str(taxcode)+"',bankaccount='"+str(bankaccount)+"',bank='"+str(bank)+"',bankbranch='"+str(bankbranch)+"',agency_id = '"+str(id_daily)+"' where id ='"+str(id)+"'"
                    logging.info(str(session) +"|query|"+str(sql))
                    with engine.connect() as con:
                        try:
                            con.execute(sql)
                            con.close()
                        except Exception as e:
                            engine.dispose()
                            logging.error(str(session) +"|response|" +str(traceback.format_exc()))
                            return jsonify({'NOTOK': str(e)}),400
                    # sqllog = "insert into log (name,api,content,type) value ('"+str(nameuser)+"','updateaccount','"+str(record).replace("'","")+"','UPDATE')"
                    # cursor = db.cursor()
                    # cursor.execute(sqllog)
                    # db.commit()
                    logging.info(str(session) +"|response|Thành công")
                    return jsonify({'OK': 'OK'})
                else :
                    engine.dispose()
                    logging.error(str(session) +"|response|token hết hạn")
                    return jsonify({'NOTOK': 'token hết hạn','code':401}),401
            else :
                engine.dispose()
                logging.error(str(session) +"|response|Không có quyền")
                return jsonify({'NOTOK': 'khong co quyen'}),400  
    except Exception as e:
        logging.error(str(session) +"|response|" +str(traceback.format_exc()))
        return jsonify({'NOTOK': str(traceback.format_exc())}),400

@partner.route('/list_partner_group', methods=['GET'])
def list_partner_group():
    session=datetime.now()
    session = str(session).replace(" ","").replace(":","").replace(".","")
    logging.info(str(session) +"|---------------list_partner_group--------------------")
    try:
        engine = create_engine(dbconfig)
        data = group()
        if str(data)== "chua truyen token" :
            return jsonify({'NOTOK': str(data)})
        else:
            timetoken = data["expireddate"]
            group_name = data["group_name"]
            nameuser = data["user"]
            partner = data["partner"]
            partner_code = data["partner_code"]
            logging.info(str(session) +"|data|"+str(data))
            logging.info(str(session) +"|partner_code|"+str(partner_code))
            if str(partner) != "":
                partner= str(partner).replace("[","").replace("]","").replace("'","")
                partner =re.split('[,;/ ]+', partner)
                print("---------")
                nickname=""
                for i in partner:
                    nickname=nickname+",'"+str(i)+"'"
                nickname=nickname[1:len(nickname)]
                logging.info(session + " | nickname |" + nickname)
            if True:
                currentDate = time.strftime("%Y-%m-%d %H:%M:%S")
                if str(timetoken) >= str(currentDate) :
                    sql = "SELECT a.id,a.createdtime,a.partner_code,b.nickname'nickname1',c.nickname'nickname2',d.nickname'nickname3' FROM partner_group a LEFT JOIN partner b ON a.partner1 = b.partner_code LEFT JOIN partner c ON a.partner2 = c.partner_code LEFT JOIN partner d ON a.partner3 = d.partner_code"
                    logging.info(str(session) +"|"+str(sql))
                    with engine.connect() as con:
                        df = pd.read_sql(sql,con)
                        con.close()
                    df['createdtime'] = df['createdtime'].astype('str')
                    # lấy 10 ký tự đầu cột createdtime
                    df['createdtime'] = df['createdtime'].str[:10]
                    json_records = df.to_json(orient ='records')
                    data = []
                    data = json.loads(json_records)
                    context = {'data': data,'code': 'OK'}
                    logging.info(str(session) +"|response | Thành công")
                    engine.dispose()
                    return context
                else :
                    engine.dispose()
                    logging.error(str(session) +"|response|token hết hạn")
                    return jsonify({'NOTOK': 'token hết hạn','code':401}),401
            else :
                engine.dispose()
                logging.error(str(session) +"|response|Không có quyền")
                return jsonify({'NOTOK': 'khong co quyen'}),400
    except Exception as e:
        logging.error(str(session) +"|response|" +str(traceback.format_exc()))
        return jsonify({'NOTOK': str(traceback.format_exc())}),400
@partner.route('/insert_partner_group', methods=['POST'])
def insert_partner_group():
    try:
        db = pymysql.connect(host="172.17.0.1",    # your host, usually localhost
                        user="leeoncrm",         # your username
                        passwd="41XmKsO3NBgHPwv",  # your password
                        db="leeon_crm",
                        port=3306
                        )
        session=datetime.now()
        logging.info(str(session) +"|insertpartner") 
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
                    nickname1 = record["nickname1"]
                    if str(nickname1) == "":
                        partner1 = ""
                    else:
                        sql_partner1 = "select partner_code from partner where nickname ='"+str(nickname1)+"'"
                        logging.info(str(session) +"|sql_partner1|"+str(sql_partner1))
                        df_partner1 = pd.read_sql(sql_partner1,db)
                        if df_partner1.empty:
                            logging.error(str(session) +"|response|partner1 không tồn tại")
                            return jsonify({'NOTOK': 'partner1 không tồn tại'}),400
                        partner1 = df_partner1.iloc[0]['partner_code']
                    nickname2 = record["nickname2"]
                    if str(nickname2) == "":
                        partner2 = ""
                    else :
                        sql_partner2 = "select partner_code from partner where nickname ='"+str(nickname2)+"'"
                        logging.info(str(session) +"|sql_partner2|"+str(sql_partner2))
                        df_partner2 = pd.read_sql(sql_partner2,db)
                        if df_partner2.empty:
                            logging.error(str(session) +"|response|partner2 không tồn tại")
                            return jsonify({'NOTOK': 'partner2 không tồn tại'}),400
                        partner2 = df_partner2.iloc[0]['partner_code']
                    nickname3 = record["nickname3"]
                    if str(nickname3) == "":
                        partner3 = ""
                    else :
                        sql_partner3 = "select partner_code from partner where nickname ='"+str(nickname3)+"'"
                        logging.info(str(session) +"|sql_partner3|"+str(sql_partner3))
                        df_partner3 = pd.read_sql(sql_partner3,db)
                        if df_partner3.empty:
                            logging.error(str(session) +"|response|partner3 không tồn tại")
                            return jsonify({'NOTOK': 'partner3 không tồn tại'}),400
                        partner3 = df_partner3.iloc[0]['partner_code']
                    partner_code = str(partner1)+"-"+str(partner2)+"-"+str(partner3)
                    # bỏ ký tự cuối cùng nếu là dấu -
                    if partner_code[len(partner_code)-2] == "--":
                        partner_code = partner_code[:-1]
                    if partner_code[len(partner_code)-1] == "-":
                        partner_code = partner_code[:-1]
                    now = datetime.now()
                    current_time = now.strftime("%Y-%m-%d %H:%M:%S")
                    sql = "INSERT INTO partner_group (partner1,partner2,partner3,createdtime,partner_code) VALUES ('"+str(partner1)+"','"+str(partner2)+"','"+str(partner3)+"','"+str(current_time)+"','"+str(partner_code)+"')"
                    logging.info(str(session) +"|sql|"+str(sql))
                    cursor=db.cursor()
                    cursor.execute(sql)
                    db.commit()
                    logging.info(str(session) +"|response| Thành công")
                    db.close()
                    return jsonify({'OK': 'Thành công'}),200
                else :
                    logging.error(str(session) +"|response|token hết hạn")
                    return jsonify({'NOTOK': 'token hết hạn','code':401}),401
            else :
                logging.error(str(session) +"|response|Không có quyền")
                return jsonify({'NOTOK': 'khong co quyen'}),400
    except Exception as e:
        logging.error(str(traceback.format_exc()))
        return jsonify({'NOTOK': str(e)}),400
@partner.route('/update_partner_group', methods=['POST'])
def update_partner_group():
    db = pymysql.connect(host="172.17.0.1",    # your host, usually localhost
                     user="leeoncrm",         # your username
                     passwd="41XmKsO3NBgHPwv",  # your password
                     db="leeon_crm",
                     port=3306
                     )
    cursor = db.cursor()
    session=datetime.now()
    logging.info(str(session) +"|insertpartner")
    try:
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
                    id = record["id"]
                    nickname1 = record["nickname1"]
                    if str(nickname1) == "":
                        partner1 = ""
                    else :
                        sql_partner1 = "select partner_code from partner where nickname ='"+str(nickname1)+"'"
                        logging.info(str(session) +"|sql_partner1|"+str(sql_partner1))
                        df_partner1 = pd.read_sql(sql_partner1,db)
                        if df_partner1.empty:
                            logging.error(str(session) +"|response|partner1 không tồn tại")
                            return jsonify({'NOTOK': 'partner1 không tồn tại'}),400
                        partner1 = df_partner1.iloc[0]['partner_code']
                    nickname2 = record["nickname2"]
                    if str(nickname2) == "":
                        partner2 = ""
                    else :
                        sql_partner2 = "select partner_code from partner where nickname ='"+str(nickname2)+"'"
                        logging.info(str(session) +"|sql_partner2|"+str(sql_partner2))
                        df_partner2 = pd.read_sql(sql_partner2,db)
                        if df_partner2.empty:
                            logging.error(str(session) +"|response|partner2 không tồn tại")
                            return jsonify({'NOTOK': 'partner2 không tồn tại'}),400
                        partner2 = df_partner2.iloc[0]['partner_code']
                    nickname3 = record["nickname3"]
                    if str(nickname3) == "":
                        partner3 = ""
                    else :
                        sql_partner3 = "select partner_code from partner where nickname ='"+str(nickname3)+"'"
                        logging.info(str(session) +"|sql_partner3|"+str(sql_partner3))
                        df_partner3 = pd.read_sql(sql_partner3,db)
                        if df_partner3.empty:
                            logging.error(str(session) +"|response|partner3 không tồn tại")
                            return jsonify({'NOTOK': 'partner3 không tồn tại'}),400
                        partner3 = df_partner3.iloc[0]['partner_code']
                    partner_code = str(partner1)+"-"+str(partner2)+"-"+str(partner3)
                    # bỏ ký tự cuối cùng nếu là dấu -
                    if partner_code[len(partner_code)-2] == "--":
                        partner_code = partner_code[:-1]
                    if partner_code[len(partner_code)-1] == "-":
                        partner_code = partner_code[:-1]
                    now = datetime.now()
                    current_time = now.strftime("%Y-%m-%d %H:%M:%S")
                    sql = "update partner_group set partner1 = '"+str(partner1)+"',partner2 = '"+str(partner2)+"',partner3 = '"+str(partner3)+"',createdtime = '"+str(current_time)+"',partner_code = '"+str(partner_code)+"' where id = '"+str(id)+"'"
                    logging.info(str(session) +"|sql|"+str(sql))
                    cursor.execute(sql)
                    db.commit()
                    logging.info(str(session) +"|response| Thành công")
                    db.close()
                    return jsonify({'OK': 'Thành công'}),200
                else :
                    logging.error(str(session) +"|response|token hết hạn")
                    return jsonify({'NOTOK': 'token hết hạn','code':401}),401
            else :
                logging.error(str(session) +"|response|Không có quyền")
                return jsonify({'NOTOK': 'khong co quyen'}),400
    except Exception as e:
        logging.error(str(session) +"|response|" +str(traceback.format_exc()))
        return jsonify({'NOTOK': str(e)}),400