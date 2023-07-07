import base64
import json
import random
import string
import bcrypt
import time
from flask import Blueprint
from flask import request, jsonify
import json
import requests
from requests.auth import HTTPDigestAuth
import pandas as pd
from pandas import json_normalize
import pymysql
from flask_jsonpify import jsonpify
from datetime import datetime,timedelta
import datetime
import traceback
from function import group
import re
from sqlalchemy import create_engine
import smtplib
import email
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from email import encoders
from email.header import Header
from itertools import chain
import logging
login_saovang = Blueprint('login_saovang', __name__)
@login_saovang.route('/saovang_register', methods=['POST'])
def register():
    try :
        db = pymysql.connect(host="172.17.0.1",    # your host, usually localhost
                     user="leeoncrm",         # your username
                     passwd="41XmKsO3NBgHPwv",  # your password
                     db="leeon_crm",
                     port=3306
                     )
        record = json.loads(request.data)
        print(record)
        user = record["username"]
        passwords = record["password"]
        fullname = record["fullname"]
        phonenumber = record["phonenumber"]
        email = record["email"]
        group = record["group"]
        partner = record["partner"]
        password = passwords.encode('utf-8')
        sqlcheck ="select count(*) from user where username = '"+str(user)+"'"
        df = pd.read_sql(sqlcheck,db)
        sl = df.iloc[0,0]
        if sl == 0:
            hashedPassword = bcrypt.hashpw(password, bcrypt.gensalt())
            print(hashedPassword)
            currentDate = time.strftime("%Y-%m-%d %H:%M:%S")
            print(currentDate)
            sql = "insert into user (fullname,username,email,phonenumber,isActive,password,createdAt,updatedAt,agency_id) value ('"+str(fullname)+"','"+str(user)+"','"+str(email)+"','"+str(phonenumber)+"','1','"+str(hashedPassword.decode())+"',NOW(),NOW(),'1')"
            cursor = db.cursor()
            cursor.execute(sql)
            partner =re.split('[,;/ ]+', str(partner))
            for i in partner :
                insertpartner="insert into user_partner(userId,partnerId,type) value ((select id from user where username = '"+str(user)+"'),(select id from leeon_crm.partner where nickname = '"+str(i)+"'),1)"
                cursor = db.cursor()
                cursor.execute(insertpartner)
            try:
                insertgroup ="insert into leeon_crm.user_group(userId,groupId) value ((select id from user where username = '"+str(user)+"'),(select id from leeon_crm.group where name = '"+str(group)+"'))"
                cursor = db.cursor()
                cursor.execute(insertgroup)          
            except:
                return jsonify({'NOTOK': 'check ten group'})
            sqllog = "insert into log (name,api,content) value ('"+str(user)+"','register','"+str(record).replace("'","")+"')"
            cursor = db.cursor()
            cursor.execute(sqllog)
            db.commit()
            db.close()
            f = open("template_saovang.html", "r")
            html = f.read()
            html = str(html).replace("$account",str(user)).replace("$password",str(passwords))
            f.close()
            content = MIMEText(html, 'html')
            mailuser='doisoat@svtelecom.vn'
            mailpassword='Sur57262'
            mailserver='smtp.office365.com'
            subject="Thông báo tạo mới tài khoản CRM-SAO VÀNG"
            msg = MIMEMultipart()
            msg['From'] = 'doisoat@svtelecom.vn'
            msg['To'] = str(email)
            msg['Subject'] = "%s" % Header(subject, 'utf-8')
            msg.attach(content)
            mailServer = smtplib.SMTP(mailserver, 25)
            mailServer.ehlo()
            mailServer.starttls()
            mailServer.ehlo()
            mailServer.login(mailuser, mailpassword)
            mailServer.sendmail(mailuser, str(email), msg.as_string())
            mailServer.quit()
            logging.info("send_mail_ok")
            #insert log
            
            return jsonify({'OK': 'OK'})
        else :
            return jsonify({'NOTOK': 'Đã tồn tại username'}),400
    except :
        logging.error(str(traceback.format_exc()))
        db.close()  
        return jsonify({'Lỗi': 'Quá nhiều lỗi'}),400
@login_saovang.route('/saovang_dangnhap', methods=['POST'])
def dangnhap():
    try :
        session=datetime.datetime.now()
        logging.info(str(session) + "| dangnhap")
        db_connection_str = 'mysql+pymysql://leeoncrm:41XmKsO3NBgHPwv@172.17.0.1/leeon_crm'
        db_connection = create_engine(db_connection_str)
        logging.info(str(session) + "| connect db ok")
        record = json.loads(request.data)
        logging.info(str(session) + "| record | "+str(record))
        user = record["user"]
        sqlname = "select fullname ,username,isActive,partner_code from user where username ='"+str(user)+"'"
        dfname = pd.read_sql(sqlname,db_connection)
        if dfname.empty :
            return jsonify({'NOTOK': 'Tên đăng nhập không tồn tại'}),400
        logging.info(str(session) + "| isActive | "+str(dfname.iloc[0]['isActive']))
        if str(dfname.iloc[0]['isActive']) =="1":
            sqlgroup = "SELECT name FROM leeon_crm.group where id = (select groupId from user_group where userId = (select id from user where username ='"+str(user)+"'))"
            dfgroup = pd.read_sql(sqlgroup,db_connection)
            if dfgroup.empty :
                group_name=""
            else :
                group_name = dfgroup.iloc[0,0]
            logging.info(str(session) + "| group | "+str(group_name))
            sqlpartner = "select nickname from partner where id in (select partnerId from user_partner where userId = (select id from user where username ='"+str(user)+"'))"
            dfpartner = pd.read_sql(sqlpartner,db_connection)
            if dfpartner.empty :
                partner=""
            else :
                partner = dfpartner["nickname"]
                partner=partner.values.tolist()
            logging.info(str(session) + "| partner | "+str(partner))
            password = record["password"]
            password = password.encode('utf-8')
            now = datetime.datetime.now()
            expireddate = now + timedelta(minutes = 60)
            token1 = json.dumps({
                "group_name":str(group_name),
                "partner":str(partner),
                "partner_code" : str(dfname.iloc[0]['partner_code']),
                "user": str(user),
                "password": str(password),
                "expireddate" :str(expireddate)
                })
            message_bytes = token1.encode('ascii')
            base64_bytes = base64.b64encode(message_bytes)
            base64_message = base64_bytes.decode('ascii')
            logging.info(str(session) + "| token | "+str(base64_message))
            sqlcheck = "select count(*) from user where username = '"+str(user)+"'"
            df = pd.read_sql(sqlcheck,db_connection)
            sl = df.iloc[0,0]
            logging.info(str(session) + "| sl | "+str(sl))
            if sl == 0:
                return jsonify({'NOTOK': 'Không có user này'}),400
            else :
                sql = "select password from user where username = '"+str(user)+"'"
                df = pd.read_sql(sql,db_connection)
                pwd = df.iloc[0,0]
                print(pwd)
                logging.info(str(session) + "| pwd | "+str(pwd))
                pwd = pwd.encode('utf-8')
                if bcrypt.checkpw(password, pwd):
                    logging.info(str(session) + "| login success")
                    sqllog = "insert into log (name,api,content) value ('"+str(user)+"','dangnhap','login thanh cong')"
                    #execute query
                    db_connection.execute(sqllog)
                    return jsonify({'OK': 'login success','token' :str(base64_message),'group':group_name,'fullname':str(dfname.iloc[0,0]),'username':str(dfname.iloc[0,1])})
                else:
                    print("incorrect password")
                    return jsonify({'NOTOK': 'Sai Tài khoản , mật khẩu'}),400
        else :
            return jsonify({'NOTOK': 'account tạm dừng'}),400
    except Exception as e :
        #write log with traceback
        logging.error(str(session) + "| error : " + str(traceback.format_exc()))
        logging.info(str(session) + "| return error")
        return jsonify({'NOTOK': 'ERROR'}),400
@login_saovang.route('/saovang_list_group', methods=['GET'])
def list_group():
    try :
        db = pymysql.connect(host="172.17.0.1",    # your host, usually localhost
                     user="leeoncrm",         # your username
                     passwd="41XmKsO3NBgHPwv",  # your password
                     db="leeon_crm",
                     port=3306
                     )
        data = group()
        if str(data)== "chua truyen token" :
            print("chua truyen token")
            return jsonify({'NOTOK': str(data)})
        else:
            timetoken = data["expireddate"]
            group_name = data["group_name"]
            name = data["user"]
            sqllog = "insert into log (name,api,content) value ('"+str(name)+"','list_group','view')"
            cursor = db.cursor()
            cursor.execute(sqllog)
            db.commit()
            print(data)
            currentDate = time.strftime("%Y-%m-%d %H:%M:%S")
            print(currentDate)
            if str(timetoken) >= str(currentDate) :
                sql ="SELECT id ,name FROM leeon_crm.group"
                df = pd.read_sql(sql,db)
                json_records = df.to_json(orient ='records')
                data = []
                data = json.loads(json_records)
                context = {'data': data,'code': 'OK'}
                db.close() 
                return context
            else :
                return jsonify({'NOTOK': 'token hết hạn'}),401
    except :
        db.close()  
        return jsonify({'NOTOK': 'NOTOK'}),400
@login_saovang.route('/saovang_reset_password', methods=['POST'])
def reset_password():
    logging.info("|----------------reset_password---------------------")
    try :
        db = pymysql.connect(host="172.17.0.1",    # your host, usually localhost
                     user="leeoncrm",         # your username
                     passwd="41XmKsO3NBgHPwv",  # your password
                     db="leeon_crm",
                     port=3306
                     )
        record = json.loads(request.data)
        email = record["email"]
        letters = string.ascii_letters
        passwords = ''.join(random.choice(letters) for i in range(8))
        password = passwords.encode('utf-8')
        logging.info(password)
        hashedPassword = bcrypt.hashpw(password, bcrypt.gensalt())
        sqlupdate ="update user set password = '"+str(hashedPassword.decode())+"' where email = '"+str(email)+"'"
        cursor = db.cursor()
        cursor.execute(sqlupdate)
        db.commit()
        db.close()
        f = open("reset_saovang.html", "r")
        html = f.read()
        html = str(html).replace("$password",str(passwords))
        f.close()
        content = MIMEText(html, 'html')
        mailuser='doisoat@svtelecom.vn'
        mailpassword='Sur57262'
        mailserver='smtp.office365.com'
        subject="Đổi mật khẩu CRM-SAO VÀNG"
        msg = MIMEMultipart()
        msg['From'] = 'doisoat@svtelecom.vn'
        msg['To'] = str(email)
        msg['Subject'] = "%s" % Header(subject, 'utf-8')
        msg.attach(content)
        mailServer = smtplib.SMTP(mailserver, 25)
        mailServer.ehlo()
        mailServer.starttls()
        mailServer.ehlo()
        mailServer.login(mailuser, mailpassword)
        mailServer.sendmail(mailuser, str(email), msg.as_string())
        mailServer.quit()
        logging.info("| send mail OK")
        return jsonify({'OK': 'OK'})
    except:
        logging.error("| error : " + str(traceback.format_exc()))
        return jsonify({'NOTOK': 'NOTOK'})
@login_saovang.route('/saovang_register_v2', methods=['POST'])
def saovang_register_v2():
    try :
        db = pymysql.connect(host="172.17.0.1",    # your host, usually localhost
                     user="leeoncrm",         # your username
                     passwd="41XmKsO3NBgHPwv",  # your password
                     db="leeon_crm",
                     port=3306
                     )
        record = json.loads(request.data)
        logging.info(record)
        user = record["username"]
        passwords = record["password"]
        fullname = record["fullname"]
        phonenumber = record["phonenumber"]
        email = record["email"]
        group = record["group"]
        nickname1 = record["partner1"]
        if str(nickname1) == "":
            partner_code = ""
        else :
            sql_get_partner_code = "select partner_code from partner where nickname = '"+str(nickname1)+"'"
            df = pd.read_sql(sql_get_partner_code,db)
            if df.empty:
                partner1 =""
                partner_code = ""
            else :
                partner1 =str(df.iloc[0,0])
                partner_code = str(df.iloc[0,0])
        password = passwords.encode('utf-8')
        sqlcheck ="select count(*) from user where username = '"+str(user)+"'"
        df = pd.read_sql(sqlcheck,db)
        sl = df.iloc[0,0]
        if sl == 0:
            hashedPassword = bcrypt.hashpw(password, bcrypt.gensalt())
            print(hashedPassword)
            currentDate = time.strftime("%Y-%m-%d %H:%M:%S")
            print(currentDate)
            sql = "insert into user (partner1,fullname,username,email,phonenumber,isActive,password,createdAt,updatedAt,partner_code,agency_id) value ('"+str(nickname1)+"','"+str(fullname)+"','"+str(user)+"','"+str(email)+"','"+str(phonenumber)+"','1','"+str(hashedPassword.decode())+"',NOW(),NOW(),'"+str(partner_code)+"','1')"
            cursor = db.cursor()
            cursor.execute(sql)
            try:
                insertgroup ="insert into leeon_crm.user_group(userId,groupId) value ((select id from user where username = '"+str(user)+"'),(select id from leeon_crm.group where name = '"+str(group)+"'))"
                cursor = db.cursor()
                cursor.execute(insertgroup)          
            except:
                return jsonify({'NOTOK': 'check ten group'})
            sqllog = "insert into log (name,api,content) value ('"+str(user)+"','register','"+str(record).replace("'","")+"')"
            cursor = db.cursor()
            cursor.execute(sqllog)
            db.commit()
            db.close()
            #reead file template_saovang.html
            f = open("template_saovang.html", "r")
            html = f.read()
            html = str(html).replace("$account",str(user)).replace("$password",str(passwords))
            f.close()
            content = MIMEText(html, 'html')
            mailuser='doisoat@svtelecom.vn'
            mailpassword='Sur57262'
            mailserver='smtp.office365.com'
            subject="Thông báo tạo mới tài khoản CRM-SAO VÀNG"
            msg = MIMEMultipart()
            msg['From'] = 'doisoat@svtelecom.vn'
            msg['To'] = str(email)
            msg['Subject'] = "%s" % Header(subject, 'utf-8')
            msg.attach(content)
            mailServer = smtplib.SMTP(mailserver, 25)
            mailServer.ehlo()
            mailServer.starttls()
            mailServer.ehlo()
            mailServer.login(mailuser, mailpassword)
            mailServer.sendmail(mailuser, str(email), msg.as_string())
            mailServer.quit()
            return jsonify({'OK': 'OK'})
        else :
            return jsonify({'NOTOK': 'Đã tồn tại username'}),400
    except :
        logging.error(traceback.format_exc())
        db.close()  
        return jsonify({'Lỗi': 'Quá nhiều lỗi'}),400