from flask import Blueprint,send_from_directory,send_file
from flask import request, jsonify
import json
import logging
import requests
from requests.auth import HTTPDigestAuth
import pandas as pd
from pandas import json_normalize
import pymysql
from flask_jsonpify import jsonpify
from datetime import datetime
from function import group
import time
from datetime import datetime, timedelta
import calendar
import re
import os,zipfile
import traceback
import math
import random
import string
import smtplib
import email
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from email import encoders
from email.header import Header
from itertools import chain
from num2words import num2words
invoice = Blueprint('invoice', __name__)
def number_to_words(amount):
    try:
        words = num2words(amount, lang='vi')
        return words.replace('-', ' ').title()
    except ValueError:
        return 'Số tiền không hợp lệ'
@invoice.route('/create_invoice', methods=['POST'])
def create_invoice():
    logging.info("----------------------create_invoice-------------------------")
    try:
        db = pymysql.connect(host="172.17.0.1",    # your host, usually localhost
                     user="leeoncrm",         # your username
                     passwd="41XmKsO3NBgHPwv",  # your password
                     db="leeon_crm",
                     port=3306
                     )

        data = group()
        nicknames=""
        logging.info(data)
        if str(data)== "chua truyen token" :
            return jsonify({'NOTOK': str(data)})
        else:
            timetoken = data["expireddate"]
            group_name = data["group_name"]
            if True:
                currentDate = time.strftime("%Y-%m-%d %H:%M:%S")
                logging.info(currentDate)
                if str(timetoken) >= str(currentDate) :
                    record = json.loads(request.data)
                    logging.info(record)
                    month = record["month"]
                    months = str(month)[0:4]+"-"+str(month)[4:6]+"-01 00:00:00"
                    createdtime = str(month)[0:4]+"-"+str(month)[4:6]
                    nickname = record["nickname"]
                    #update status invoice
                    sql = "UPDATE inform_cdr SET status = 2 WHERE created_time = '"+str(months)+"' AND partner_name = '"+str(nickname)+"'"
                    cursor = db.cursor()
                    cursor.execute(sql)
                    db.commit()
                    # lấy thông tin invoice mới
                    # get partner_code
                    sql_partner_code = "select partner_code from partner where nickname = '"+str(nickname)+"'"
                    df_partnercode = pd.read_sql(sql_partner_code,db)
                    partner_code = df_partnercode.iloc[0]['partner_code']
                    sql = "SELECT SUM(voicetime)/60'voicetime',SUM(revenue)'revenue','"+str(partner_code)+"'`partner_code`,left(partner_code,5)'partner1',callType,'"+str(nickname)+"'`nickname` FROM report WHERE createdtime like '%"+str(createdtime)+"%' and partner_code like '%"+str(partner_code)+"%' and callType != 'other' GROUP BY callType"
                    logging.info(sql)
                    df = pd.read_sql(sql,db)
                    logging.info(df)
                    if df.empty:
                        return jsonify({'NOTOK': 'Đối tác không phát sinh cước'}),400
                    #lấy random 5 ký tự
                    tail = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(5))
                    for i in range(len(df)):
                        feeTime = df.iloc[i]['voicetime']
                        partner_code= df.iloc[i]['partner_code']
                        partner1= df.iloc[i]['partner1']
                        nickname = df.iloc[i]['nickname']
                        callType = df.iloc[i]['callType']
                        revenue = df.iloc[i]['revenue']
                        sql_price = "SELECT price FROM service_config WHERE partnerCode = '"+str(partner1)+"' AND callTypeId = (select id from service_type where callType = '"+str(callType)+"')"
                        logging.info(sql_price)
                        df_price = pd.read_sql(sql_price,db)
                        price = df_price.iloc[0]['price']
                        #insert invoice
                        inform_id = str(partner_code)+"-"+str(month)+"-"+str(tail)
                        sql = "INSERT INTO inform_cdr (partner_code,partner_name,created_time,call_type,total_minute,price,revenue,inform_id,updated_time) VALUES ('"+str(partner_code)+"','"+str(nickname)+"','"+str(months)+"','"+str(callType)+"','"+str(feeTime)+"','"+str(price)+"','"+str(revenue)+"','"+str(inform_id)+"','"+str(currentDate)+"')"
                        cursor.execute(sql)
                        db.commit()
                    db.close()
                    return jsonify({'OK': 'OK'}),200
                else :
                    return jsonify({'NOTOK': 'token hết hạn','code':401}),401
            else :
                return jsonify({'NOTOK': 'khong co quyen'}),400
    except Exception as e:
        logging.error(traceback.format_exc())
        return jsonify({'NOTOK': 'loi he thong'}),400
@invoice.route('/list_invoice', methods=['GET'])
def list_invoice():
    try:
        db = pymysql.connect(host="172.17.0.1",    # your host, usually localhost
                     user="leeoncrm",         # your username
                     passwd="41XmKsO3NBgHPwv",  # your password
                     db="leeon_crm",
                     port=3306
                     )

        data = group()
        print(data)
        if str(data)== "chua truyen token" :
            return jsonify({'NOTOK': str(data)})
        else:
            timetoken = data["expireddate"]
            group_name = data["group_name"]
            if True:
                currentDate = time.strftime("%Y-%m-%d %H:%M:%S")
                logging.info(currentDate)
                if str(timetoken) >= str(currentDate) :
                    sql = "SELECT id,inform_id,partner_code,partner_name,partner_name'nickname',created_time,created_time'month',case when status = 0 then 'Chờ gửi' when status = 1 then 'Đã gửi' when status = 2 then 'Đã hủy' end as STATUS,updated_time FROM inform_cdr where status != 2 GROUP BY inform_id"
                    df = pd.read_sql(sql,db)
                    df['created_time'] = df['created_time'].astype(str)
                    #convert cột created_time sang dạng chuỗi
                    df['month'] = df['month'].astype(str)
                    #replace cột created_time
                    df['month'] = df['month'].str[0:7].replace('-','')
                    df['month'] = df['month'].str.replace('-','')
                    json_records = df.to_json(orient ='records')
                    data = []
                    data = json.loads(json_records)
                    context = {'data': data,'code': 'OK'}
                    db.close()
                    return context
                else :
                    return jsonify({'NOTOK': 'token hết hạn','code':401}),401
            else :
                return jsonify({'NOTOK': 'khong co quyen'})
    except Exception as e:
        logging.error(traceback.format_exc())
        return jsonify({'NOTOK': 'loi he thong'}),400
@invoice.route('/invoice_detail', methods=['POST'])
def invoice_detail():
    logging.info("----------------------invoice_detail-------------------------")
    try:
        db = pymysql.connect(host="172.17.0.1",    # your host, usually localhost
                     user="leeoncrm",         # your username
                     passwd="41XmKsO3NBgHPwv",  # your password
                     db="leeon_crm",
                     port=3306
                     )
        record = json.loads(request.data)
        logging.info(record)
        month = record["month"]
        months = str(month)[0:4]+"-"+str(month)[4:6]+"-01 00:00:00"
        nickname = record["nickname"]
        #lấy thông tin invoice
        sql ="SELECT a.id,a.created_time,a.partner_code, a.partner_name, b.callType, a.total_minute, a.price,a.revenue,a.status,a.inform_id FROM inform_cdr a, service_type b WHERE  a.partner_name = '"+str(nickname)+"' AND a.created_time= '"+str(months)+"' and a.call_type=b.id"
        df = pd.read_sql(sql,db)
        df['created_time'] = df['created_time'].astype('str')
        # phân cách hàng nghìn bằng dấu phẩy
        df['revenue'] = df['revenue'].apply(lambda x: "{:,}".format(x))
        df['total_minute'] = df['total_minute'].apply(lambda x: "{:,}".format(x))
        json_records = df.to_json(orient ='records')
        data = []
        data = json.loads(json_records)
        #lấy thông tin partner
        sql_partner ="SELECT * FROM partner WHERE  nickname = '"+str(nickname)+"'"
        df_partner = pd.read_sql(sql_partner,db)
        json_records = df_partner.to_json(orient ='records')
        data_partner = []
        data_partner = json.loads(json_records)
        #lấy total revenue
        sql_total ="SELECT SUM(revenue) as total_revenue FROM inform_cdr WHERE  partner_name = '"+str(nickname)+"' AND created_time= '"+str(months)+"'"
        df_total = pd.read_sql(sql_total,db)
        df_total['total_revenue'] = df_total['total_revenue'].apply(lambda x: "{:,}".format(x))
        df_total['total_revenue'] = df_total['total_revenue'].astype(str).str[:-2]
        json_records = df_total.to_json(orient ='records')
        data_total = []
        data_total = json.loads(json_records)
        context = {'data': data,'data_partner': data_partner,'data_total': data_total,'code': 'OK'}
        db.close()
        return context
    except Exception as e:
        logging.error(traceback.format_exc())
        return jsonify({'NOTOK': 'NOTOK'}),400

@invoice.route('/update_invoice', methods=['POST'])
def update_invoice():
    logging.info("----------------------invoice_detail-------------------------")
    try:
        db = pymysql.connect(host="172.17.0.1",    # your host, usually localhost
                     user="leeoncrm",         # your username
                     passwd="41XmKsO3NBgHPwv",  # your password
                     db="leeon_crm",
                     port=3306
                     )
        data = group()
        logging.info(data)
        if str(data)== "chua truyen token" :
            return jsonify({'NOTOK': str(data)})
        else:
            timetoken = data["expireddate"]
            group_name = data["group_name"]
            if True:
                currentDate = time.strftime("%Y-%m-%d %H:%M:%S")
                logging.info(currentDate)
                if str(timetoken) >= str(currentDate) :
                    # lấy thời gian hiện tại
                    record = json.loads(request.data)
                    logging.info(record)
                    status = record["STATUS"]
                    if str(status) == "Chờ gửi" :
                        status = 0
                    elif str(status) == "Đã gửi" :
                        status = 1
                    else :
                        status = 2
                    month = record["month"]
                    months = str(month)[0:4]+"-"+str(month)[4:6]+"-01 00:00:00"
                    nickname = record["nickname"]
                    sql="UPDATE inform_cdr SET STATUS = '"+str(status)+"' WHERE partner_name = '"+str(nickname)+"' AND created_time= '"+str(months)+"'"
                    logging.info(sql)
                    cursor = db.cursor()
                    cursor.execute(sql)
                    db.commit()
                    return jsonify({'OK': 'OK'}),200
                else :
                    return jsonify({'NOTOK': 'token hết hạn','code':401}),401
            else :
                return jsonify({'NOTOK': 'khong co quyen'}),400
    except Exception as e:
        logging.error(traceback.format_exc())
        return jsonify({'NOTOK': 'NOTOK'}),400
@invoice.route('/download_invoice_detail', methods=['POST'])
def download_invoice_detail():
    logging.info("----------------------download_invoice_detail-------------------------")
    try:
        db = pymysql.connect(host="172.17.0.17",    # your host, usually localhost
                     user="itsinvoice",         # your username
                     passwd="KHASkd09239998",  # your password
                     db="its_billing",
                     port=3306
                     )

        data = group()
        logging.info(data)
        if str(data)== "chua truyen token" :
            return jsonify({'NOTOK': str(data)})
        else:
            timetoken = data["expireddate"]
            group_name = data["group_name"]
            if True:
                currentDate = time.strftime("%Y-%m-%d %H:%M:%S")
                logging.info(currentDate)
                if str(timetoken) >= str(currentDate) :
                    # lấy thời gian hiện tại
                    record = json.loads(request.data)
                    logging.info(record)
                    starttime = record["starttime"]
                    endtime = record["endtime"]
                    if endtime != "" :
                        endtime = str(endtime) + " 23:59:59"
                    nickname = record["selectedPartner"]
                    now = datetime.datetime.now()
                    table = "cdr_billing_"+str(now)[0:7].replace("-","")
                    logging.info("table : "+ str(table))
                    # lấy query đúng
                    if str(starttime) == "" :
                        timestartquery = ""
                    else :
                        timestartquery = " and startTime >= '"+ str(starttime) +"'"
                    if str(endtime) == "" :
                        timeendquery = ""
                    else :
                        timeendquery = " and startTime <= '"+ str(endtime) +"'"
                    if str(nickname) == "" :
                        nicknamequery = ""
                    else :
                        sql_partnercode = "SELECT partner_code FROM partner where nickname ='"+str(nickname)+"'"
                        df_partnercode = pd.read_sql(sql_partnercode,db)
                        if df_partnercode.empty :
                            db.close()
                            return jsonify({'NOTOK': 'Không có nickname này'}),400
                        partner_code =df_partnercode.iloc[0,0]
                        nicknamequery = " and partner01 = "+ str(partner_code)
                    sql="SELECT a.id,a.caller,b.telcoName'callerTelcoId',a.callee,c.telcoName'calleeTelcoid',a.callerRtpIp,a.startTime,a.stopTime,a.billTime,a.billAmount,a.callercallid,a.brandName,a.cdrId,CASE WHEN a.isMnp = 1 THEN 'TRUE' ELSE 'FALSE' END AS isMnp,d.callType,CASE WHEN a.isBrand =1 THEN 'BRANDNAME' ELSE 'SIP' END AS isBrand ,a.endReason,a.partnerTag FROM (select * from "+table+" where id is not null "+timestartquery+timeendquery+nicknamequery+" ) a,service_prefix b ,service_prefix c,service_type d WHERE a.callerTelcoId = b.id AND a.calleeTelcoid = c.id AND a.callType =d.id"
                    logging.info(sql)
                    df = pd.read_sql(sql,db)
                    df['startTime'] = df['startTime'].astype('str')
                    df['stopTime'] = df['stopTime'].astype('str')
                    session=datetime.datetime.now()
                    filename = "invoice"+str(session).replace(" ","")+".csv"
                    filepath = "filedownload/invoice"+str(session).replace(" ","")
                    # taọ thư mục filepath
                    if not os.path.exists(filepath):
                        os.mkdir(filepath)
                    path = os.path.join(os.getcwd(),filepath,filename)
                    path_export = os.path.join(os.getcwd(),filepath,'invoice')
                    df_size=len(df)
                    chunk_size=500000
                    for i, start in enumerate(range(0, df_size, chunk_size)):
                        df[start:start+chunk_size].to_csv(str(path_export)+'_{}.csv'.format(i),encoding='utf-8-sig')
                    name = filepath
                    zip_name = str(filepath) + '.zip'           
                    with zipfile.ZipFile(zip_name, 'w', zipfile.ZIP_DEFLATED) as zip_ref:
                        for folder_name, subfolders, filenames in os.walk(name):
                            for filename in filenames:
                                file_path = os.path.join(folder_name, filename)
                                zip_ref.write(file_path, arcname=os.path.relpath(file_path, name))
                    zip_ref.close()
                    db.close()
                    return send_file(zip_name, as_attachment=True)
                else :
                    return jsonify({'NOTOK': 'token hết hạn','code':401}),401
            else :
                return jsonify({'NOTOK': 'khong co quyen'}),400
    except Exception as e:
        logging.error(traceback.format_exc())
        return jsonify({'NOTOK': 'NOTOK'}),400

@invoice.route('/invoice_detail_bk', methods=['POST'])
def invoice_detail_bk():
    logging.info("----------------------invoice_detail-------------------------")
    try:
        db = pymysql.connect(host="172.17.0.1",    # your host, usually localhost
                     user="leeoncrm",         # your username
                     passwd="41XmKsO3NBgHPwv",  # your password
                     db="leeon_crm",
                     port=3306
                     )
        record = json.loads(request.data)
        logging.info(record)
        inform_id = record["inform_id"]
        # get month
        sql_month = "SELECT left(created_time,7)'month' FROM inform_cdr WHERE inform_id = '"+str(inform_id)+"' LIMIT 1"
        df_month = pd.read_sql(sql_month,db)
        month = df_month.iloc[0,0]
        nam = month[0:4]
        ngay = month[5:7]
        content = ({"year":str(nam),"date": str(ngay)})
        df_date = pd.DataFrame(content, index=[0])
        json_records = df_date.to_json(orient ='records')
        data_date = []
        data_date = json.loads(json_records)
        # lấy thông tin numbermember
        #viettel
        sql_number_vt = "SELECT  a.sl'sl_khoitao',b.sl'sl_duytri',a.telco,a.initialization_fee,b.maintaining_fee FROM (SELECT COUNT(*)'sl',b.name'telco',SUM(fee)'initialization_fee' FROM number_log a ,telco b WHERE a.telco_id = b.id AND a.action = 1 AND partner_code = (SELECT partner_code FROM inform_cdr WHERE inform_id = '"+str(inform_id)+"' LIMIT 1) AND b.name = 'Viettel' and a.created_time like '%"+str(month)+"%') a LEFT JOIN (SELECT COUNT(*)'sl',b.name'telco',SUM(fee)'maintaining_fee' FROM number_log a ,telco b WHERE a.telco_id = b.id AND a.action = 2 AND partner_code = (SELECT partner_code FROM inform_cdr WHERE inform_id = '"+str(inform_id)+"' LIMIT 1) AND b.name = 'Viettel' and a.created_time like '%"+str(month)+"%') b ON b.telco =a.telco"
        df_number_vt = pd.read_sql(sql_number_vt,db)
        # thay thế giá trị 'None' thành '0'
        df_number_vt = df_number_vt.fillna(0)
        sl_khoitao_vt = str(df_number_vt.iloc[0]['sl_khoitao'])
        sl_duytri_vt = str(df_number_vt.iloc[0]['sl_duytri'])
        fee_khoitao_vt = str(df_number_vt.iloc[0]['initialization_fee']).replace(".0","")
        fee_duytri_vt = str(df_number_vt.iloc[0]['maintaining_fee']).replace(".0","")
        try:
            price_khoitao_vt = int(fee_khoitao_vt) / int(sl_khoitao_vt)
        except :
            price_khoitao_vt = 0
        try:
            price_duytri_vt = int(fee_duytri_vt) / int(sl_duytri_vt)
        except :
            price_duytri_vt = 0
        price_khoitao_vt = str(price_khoitao_vt).replace(".0","")
        price_duytri_vt = str(price_duytri_vt).replace(".0","")
        fee_khoitao_vt = "{:,}".format(int(fee_khoitao_vt))
        fee_duytri_vt = "{:,}".format(int(fee_duytri_vt))
        price_khoitao_vt = "{:,}".format(int(price_khoitao_vt))
        price_duytri_vt = "{:,}".format(int(price_duytri_vt))
        content = ({"type": "Phí khởi tạo dịch vụ","count":str(sl_khoitao_vt),"revenue": str(fee_khoitao_vt),"price":str(price_khoitao_vt)},{"type": "Phí duy trì thuê bao tháng","count":str(sl_duytri_vt),"revenue":str(fee_duytri_vt),"price":str(price_duytri_vt)})
        df_numbermember_vt = pd.DataFrame(content)
        json_records = df_numbermember_vt.to_json(orient ='records')
        numbermember_vt = []
        numbermember_vt = json.loads(json_records)
        #Mobifone
        sql_number_mbf = "SELECT  a.sl'sl_khoitao',b.sl'sl_duytri',a.telco,a.initialization_fee,b.maintaining_fee FROM (SELECT COUNT(*)'sl',b.name'telco',SUM(fee)'initialization_fee' FROM number_log a ,telco b WHERE a.telco_id = b.id AND a.action = 1 AND partner_code = (SELECT partner_code FROM inform_cdr WHERE inform_id = '"+str(inform_id)+"' LIMIT 1) AND b.name = 'Mobifone' and a.created_time like '%"+str(month)+"%') a LEFT JOIN (SELECT COUNT(*)'sl',b.name'telco',SUM(fee)'maintaining_fee' FROM number_log a ,telco b WHERE a.telco_id = b.id AND a.action = 2 AND partner_code = (SELECT partner_code FROM inform_cdr WHERE inform_id = '"+str(inform_id)+"' LIMIT 1) AND b.name = 'Mobifone' and a.created_time like '%"+str(month)+"%') b ON b.telco =a.telco"
        df_number_mbf = pd.read_sql(sql_number_mbf,db)
        # thay thế giá trị 'None' thành '0'
        df_number_mbf = df_number_mbf.fillna(0)
        sl_khoitao_mbf = str(df_number_mbf.iloc[0]['sl_khoitao'])
        sl_duytri_mbf = str(df_number_mbf.iloc[0]['sl_duytri'])
        fee_khoitao_mbf = str(df_number_mbf.iloc[0]['initialization_fee']).replace(".0","")
        fee_duytri_mbf = str(df_number_mbf.iloc[0]['maintaining_fee']).replace(".0","")
        try:
            price_khoitao_mbf = int(fee_khoitao_mbf) / int(sl_khoitao_mbf)
        except Exception as e:
            price_khoitao_mbf = 0
        try:
            price_duytri_mbf = int(fee_duytri_mbf) / int(sl_duytri_mbf)
        except :
            price_duytri_mbf = 0
        price_khoitao_mbf = str(price_khoitao_mbf).replace(".0","")
        price_duytri_mbf = str(price_duytri_mbf).replace(".0","")
        fee_khoitao_mbf = "{:,}".format(int(fee_khoitao_mbf))
        fee_duytri_mbf = "{:,}".format(int(fee_duytri_mbf))
        price_khoitao_mbf = "{:,}".format(int(price_khoitao_mbf))
        price_duytri_mbf = "{:,}".format(int(price_duytri_mbf))
        content = ({"type": "Phí khởi tạo dịch vụ","count":str(sl_khoitao_mbf),"revenue": str(fee_khoitao_mbf),"price":str(price_khoitao_mbf)},{"type": "Phí duy trì thuê bao tháng","count":str(sl_duytri_mbf),"revenue":str(fee_duytri_mbf),"price":str(price_duytri_mbf)})
        df_numbermember_mbf = pd.DataFrame(content)
        json_records = df_numbermember_mbf.to_json(orient ='records')
        numbermember_mbf = []
        numbermember_mbf = json.loads(json_records)
        #Vinaphone
        sql_number_nvp = "SELECT  a.sl'sl_khoitao',b.sl'sl_duytri',a.telco,a.initialization_fee,b.maintaining_fee FROM (SELECT COUNT(*)'sl',b.name'telco',SUM(fee)'initialization_fee' FROM number_log a ,telco b WHERE a.telco_id = b.id AND a.action = 1 AND partner_code = (SELECT partner_code FROM inform_cdr WHERE inform_id = '"+str(inform_id)+"' LIMIT 1) AND b.name = 'Vinaphone' and a.created_time like '%"+str(month)+"%') a LEFT JOIN (SELECT COUNT(*)'sl',b.name'telco',SUM(fee)'maintaining_fee' FROM number_log a ,telco b WHERE a.telco_id = b.id AND a.action = 2 AND partner_code = (SELECT partner_code FROM inform_cdr WHERE inform_id = '"+str(inform_id)+"' LIMIT 1) AND b.name = 'Vinaphone' and a.created_time like '%"+str(month)+"%') b ON b.telco =a.telco"
        df_number_nvp = pd.read_sql(sql_number_nvp,db)
        # thay thế giá trị 'None' thành '0'
        df_number_nvp = df_number_nvp.fillna(0)
        sl_khoitao_nvp= str(df_number_nvp.iloc[0]['sl_khoitao'])
        sl_duytri_nvp = str(df_number_nvp.iloc[0]['sl_duytri'])
        fee_khoitao_nvp = str(df_number_nvp.iloc[0]['initialization_fee']).replace(".0","")
        fee_duytri_nvp = str(df_number_nvp.iloc[0]['maintaining_fee']).replace(".0","")
        # phân cách hàng nghìn fee_khoitao_nvp bằng dấu ','
        try:
            price_khoitao_nvp = int(fee_khoitao_nvp) / int(sl_khoitao_nvp)
        except :
            price_khoitao_nvp = 0
        try:
            price_duytri_nvp = int(fee_duytri_nvp) / int(sl_duytri_nvp)
        except :
            price_duytri_nvp = 0
        price_khoitao_nvp = str(price_khoitao_nvp).replace(".0","")
        price_duytri_nvp = str(price_duytri_nvp).replace(".0","")
        fee_khoitao_nvp = "{:,}".format(int(fee_khoitao_nvp))
        fee_duytri_nvp = "{:,}".format(int(fee_duytri_nvp))
        price_khoitao_nvp = "{:,}".format(int(price_khoitao_nvp))
        price_duytri_nvp = "{:,}".format(int(price_duytri_nvp))
        content = ({"type": "Phí khởi tạo dịch vụ","count":str(sl_khoitao_nvp),"revenue": str(fee_khoitao_nvp),"price":str(price_khoitao_nvp)},{"type": "Phí duy trì thuê bao tháng","count":str(sl_duytri_nvp),"revenue":str(fee_duytri_nvp),"price":str(price_duytri_nvp)})
        df_numbermember_nvp = pd.DataFrame(content)
        json_records = df_numbermember_nvp.to_json(orient ='records')
        numbermember_nvp = []
        numbermember_nvp = json.loads(json_records)
        #Vietnamobile
        sql_number_vnm = "SELECT  a.sl'sl_khoitao',b.sl'sl_duytri',a.telco,a.initialization_fee,b.maintaining_fee FROM (SELECT COUNT(*)'sl',b.name'telco',SUM(fee)'initialization_fee' FROM number_log a ,telco b WHERE a.telco_id = b.id AND a.action = 1 AND partner_code = (SELECT partner_code FROM inform_cdr WHERE inform_id = '"+str(inform_id)+"' LIMIT 1) AND b.name = 'Vietnamobile' and a.created_time like '%"+str(month)+"%') a LEFT JOIN (SELECT COUNT(*)'sl',b.name'telco',SUM(fee)'maintaining_fee' FROM number_log a ,telco b WHERE a.telco_id = b.id AND a.action = 2 AND partner_code = (SELECT partner_code FROM inform_cdr WHERE inform_id = '"+str(inform_id)+"' LIMIT 1) AND b.name = 'Vietnamobile' and a.created_time like '%"+str(month)+"%') b ON b.telco =a.telco"
        df_number_vnm = pd.read_sql(sql_number_vnm,db)
        # thay thế giá trị 'None' thành '0'
        df_number_vnm = df_number_vnm.fillna(0)
        sl_khoitao_vnm = str(df_number_vnm.iloc[0]['sl_khoitao'])
        sl_duytri_vnm = str(df_number_vnm.iloc[0]['sl_duytri'])
        fee_khoitao_vnm = str(df_number_vnm.iloc[0]['initialization_fee']).replace(".0","")
        fee_duytri_vnm = str(df_number_vnm.iloc[0]['maintaining_fee']).replace(".0","")
        try:
            price_khoitao_vnm = int(fee_khoitao_vnm) / int(sl_khoitao_vnm)
        except :
            price_khoitao_vnm = 0
        try:
            price_duytri_vnm = int(fee_duytri_vnm) / int(sl_duytri_vnm)
        except :
            price_duytri_vnm = 0
        price_khoitao_vnm = str(price_khoitao_vnm).replace(".0","")
        price_duytri_vnm = str(price_duytri_vnm).replace(".0","")
        fee_khoitao_vnm = "{:,}".format(int(fee_khoitao_vnm))
        fee_duytri_vnm = "{:,}".format(int(fee_duytri_vnm))
        price_khoitao_vnm = "{:,}".format(int(price_khoitao_vnm))
        price_duytri_vnm = "{:,}".format(int(price_duytri_vnm))
        content = ({"type": "Phí khởi tạo dịch vụ","count":str(sl_khoitao_vnm),"revenue": str(fee_khoitao_vnm),"price":str(price_khoitao_vnm)},{"type": "Phí duy trì thuê bao tháng","count":str(sl_duytri_vnm),"revenue":str(fee_duytri_vnm),"price":str(price_duytri_vnm)})
        df_numbermember_vnm = pd.DataFrame(content)
        json_records = df_numbermember_vnm.to_json(orient ='records')
        numbermember_vnm = []
        numbermember_vnm = json.loads(json_records)
        #Gmobile
        sql_number_gmb = "SELECT  a.sl'sl_khoitao',b.sl'sl_duytri',a.telco,a.initialization_fee,b.maintaining_fee FROM (SELECT COUNT(*)'sl',b.name'telco',SUM(fee)'initialization_fee' FROM number_log a ,telco b WHERE a.telco_id = b.id AND a.action = 1 AND partner_code = (SELECT partner_code FROM inform_cdr WHERE inform_id = '"+str(inform_id)+"' LIMIT 1) AND b.name = 'Gmobile' and a.created_time like '%"+str(month)+"%') a LEFT JOIN (SELECT COUNT(*)'sl',b.name'telco',SUM(fee)'maintaining_fee' FROM number_log a ,telco b WHERE a.telco_id = b.id AND a.action = 2 AND partner_code = (SELECT partner_code FROM inform_cdr WHERE inform_id = '"+str(inform_id)+"' LIMIT 1) AND b.name = 'Gmobile' and a.created_time like '%"+str(month)+"%') b ON b.telco =a.telco"
        df_number_gmb = pd.read_sql(sql_number_gmb,db)
        # thay thế giá trị 'None' thành '0'
        df_number_gmb = df_number_gmb.fillna(0)
        sl_khoitao_gmb = str(df_number_gmb.iloc[0]['sl_khoitao'])
        sl_duytri_gmb = str(df_number_gmb.iloc[0]['sl_duytri'])
        fee_khoitao_gmb = str(df_number_gmb.iloc[0]['initialization_fee']).replace(".0","")
        fee_duytri_gmb = str(df_number_gmb.iloc[0]['maintaining_fee']).replace(".0","")
        try:
            price_khoitao_gmb = int(fee_khoitao_gmb) / int(sl_khoitao_gmb)
        except :
            price_khoitao_gmb = 0
        try:
            price_duytri_gmb = int(fee_duytri_gmb) / int(sl_duytri_gmb)
        except :
            price_duytri_gmb = 0
        price_khoitao_gmb = str(price_khoitao_gmb).replace(".0","")
        price_duytri_gmb = str(price_duytri_gmb).replace(".0","")
        fee_khoitao_gmb = "{:,}".format(int(fee_khoitao_gmb))
        fee_duytri_gmb = "{:,}".format(int(fee_duytri_gmb))
        price_khoitao_gmb = "{:,}".format(int(price_khoitao_gmb))
        price_duytri_gmb = "{:,}".format(int(price_duytri_gmb))
        content = ({"type": "Phí khởi tạo dịch vụ","count":str(sl_khoitao_gmb),"revenue": str(fee_khoitao_gmb),"price":str(price_khoitao_gmb)},{"type": "Phí duy trì thuê bao tháng","count":str(sl_duytri_gmb),"revenue":str(fee_duytri_gmb),"price":str(price_duytri_gmb)})
        df_numbermember_gmb = pd.DataFrame(content)
        json_records = df_numbermember_gmb.to_json(orient ='records')
        numbermember_gmb = []
        numbermember_gmb = json.loads(json_records)
        #cô định
        sql_number_codinh = "SELECT  a.sl'sl_khoitao',b.sl'sl_duytri',a.telco,a.initialization_fee,b.maintaining_fee FROM (SELECT COUNT(*)'sl',b.name'telco',SUM(fee)'initialization_fee' FROM number_log a ,telco b WHERE a.telco_id = b.id AND a.action = 1 AND partner_code = (SELECT partner_code FROM inform_cdr WHERE inform_id = '"+str(inform_id)+"' LIMIT 1) AND b.name in ('Co','Gtel','Digitel','Other') and a.created_time like '%"+str(month)+"%') a LEFT JOIN (SELECT COUNT(*)'sl',b.name'telco',SUM(fee)'maintaining_fee' FROM number_log a ,telco b WHERE a.telco_id = b.id AND a.action = 2 AND partner_code = (SELECT partner_code FROM inform_cdr WHERE inform_id = '"+str(inform_id)+"' LIMIT 1) AND b.name in ('Co','Gtel','Digitel','Other') and a.created_time like '%"+str(month)+"%') b ON b.telco =a.telco"
        df_number_codinh = pd.read_sql(sql_number_codinh,db)
        # thay thế giá trị 'None' thành '0'
        df_number_codinh = df_number_codinh.fillna(0)
        sl_khoitao_codinh = str(df_number_codinh.iloc[0]['sl_khoitao'])
        sl_duytri_codinh = str(df_number_codinh.iloc[0]['sl_duytri'])
        fee_khoitao_codinh = str(df_number_codinh.iloc[0]['initialization_fee']).replace(".0","")
        fee_duytri_codinh = str(df_number_codinh.iloc[0]['maintaining_fee']).replace(".0","")
        try:
            price_khoitao_codinh = int(fee_khoitao_codinh) / int(sl_khoitao_codinh)
        except :
            price_khoitao_codinh = 0
        try:
            price_duytri_codinh = int(fee_duytri_codinh) / int(sl_duytri_codinh)
        except :
            price_duytri_codinh = 0
        price_khoitao_codinh = str(price_khoitao_codinh).replace(".0","")
        price_duytri_codinh = str(price_duytri_codinh).replace(".0","")
        fee_khoitao_codinh = "{:,}".format(int(fee_khoitao_codinh))
        fee_duytri_codinh = "{:,}".format(int(fee_duytri_codinh))
        price_khoitao_codinh = "{:,}".format(int(price_khoitao_codinh))
        price_duytri_codinh = "{:,}".format(int(price_duytri_codinh))
        content = ({"type": "Phí khởi tạo dịch vụ","count":str(sl_khoitao_codinh),"revenue": str(fee_khoitao_codinh),"price":str(price_khoitao_codinh)},{"type": "Phí duy trì thuê bao tháng","count":str(sl_duytri_codinh),"revenue":str(fee_duytri_codinh),"price":str(price_duytri_codinh)})
        df_numbermember_codinh = pd.DataFrame(content)
        json_records = df_numbermember_codinh.to_json(orient ='records')
        numbermember_codinh = []
        numbermember_codinh = json.loads(json_records)
        # thông tin viettel
        sql_vt ="SELECT a.id,a.created_time,a.partner_code, a.partner_name, a.call_type'callType', a.total_minute, a.price,a.revenue,a.status,a.inform_id FROM inform_cdr a WHERE  a.inform_id = '"+str(inform_id)+"' and a.call_type like '%Di động - VTL%'"
        df_vt = pd.read_sql(sql_vt,db)
        if df_vt.empty :
            content = ({"callType":"Di động - VTL Nội mạng","revenue": 0,"total_minute": 0,"price":0},{"callType":"Di động - VTL Ngoại mạng","revenue": 0,"total_minute": 0,"price":0})
            data_vt = json.dumps(content)
            df_vt = pd.DataFrame.from_dict(json.loads(data_vt), orient='columns')
            json_records = df_vt.to_json(orient ='records')
            data_vt = []
            data_vt = json.loads(json_records)
        else :
            #phân cách hàng nghìn bằng dấu ','
            df_vt['revenue'] = df_vt['revenue'].apply(lambda x: "{:,}".format(x))
            df_vt['total_minute'] = df_vt['total_minute'].apply(lambda x: "{:,}".format(x))
            df_vt['created_time'] = df_vt['created_time'].astype('str')
            json_records = df_vt.to_json(orient ='records')
            data_vt = []
            data_vt = json.loads(json_records)
        # thông tin Mobifone
        sql_mbf ="SELECT a.id,a.created_time,a.partner_code, a.partner_name, a.call_type'callType', a.total_minute, a.price,a.revenue,a.status,a.inform_id FROM inform_cdr a WHERE  a.inform_id = '"+str(inform_id)+"' and a.call_type like '%Di động - MBF%'"
        df_mbf = pd.read_sql(sql_mbf,db)
        if df_mbf.empty :
            content = ({"callType":"Di động - MBF Nội mạng","revenue": 0,"total_minute": 0,"price":0},{"callType":"Di động - MBF Ngoại mạng","revenue": 0,"total_minute": 0,"price":0})
            data_mbf = json.dumps(content)
            df_mbf = pd.DataFrame.from_dict(json.loads(data_mbf), orient='columns')
            json_records = df_mbf.to_json(orient ='records')
            data_mbf = []
            data_mbf = json.loads(json_records)
        else :
            #phân cách hàng nghìn bằng dấu ','
            df_mbf['revenue'] = df_mbf['revenue'].apply(lambda x: "{:,}".format(x))
            df_mbf['total_minute'] = df_mbf['total_minute'].apply(lambda x: "{:,}".format(x))
            df_mbf['created_time'] = df_mbf['created_time'].astype('str')
            json_records = df_mbf.to_json(orient ='records')
            data_mbf = []
            data_mbf = json.loads(json_records)
        # thông tin gmb
        sql_gmb ="SELECT a.id,a.created_time,a.partner_code, a.partner_name, a.call_type'callType', a.total_minute, a.price,a.revenue,a.status,a.inform_id FROM inform_cdr a WHERE  a.inform_id = '"+str(inform_id)+"' and a.call_type like '%Di động - GMB%'"
        df_gmb = pd.read_sql(sql_gmb,db)
        if df_gmb.empty :
            content = ({"callType":"Di động - GMB Nội mạng","revenue": 0,"total_minute": 0,"price":0},{"callType":"Di động - GMB Ngoại mạng","revenue": 0,"total_minute": 0,"price":0})
            data_gmb = json.dumps(content)
            df_gmb = pd.DataFrame.from_dict(json.loads(data_gmb), orient='columns')
            json_records = df_gmb.to_json(orient ='records')
            data_gmb = []
            data_gmb = json.loads(json_records)
        else :
            #phân cách hàng nghìn bằng dấu ','
            df_gmb['revenue'] = df_gmb['revenue'].apply(lambda x: "{:,}".format(x))
            df_gmb['total_minute'] = df_gmb['total_minute'].apply(lambda x: "{:,}".format(x))
            df_gmb['created_time'] = df_gmb['created_time'].astype('str')
            json_records = df_gmb.to_json(orient ='records')
            data_gmb = []
            data_gmb = json.loads(json_records)
        # thông tin vnm
        sql_vnm ="SELECT a.id,a.created_time,a.partner_code, a.partner_name, a.call_type'callType', a.total_minute, a.price,a.revenue,a.status,a.inform_id FROM inform_cdr a WHERE  a.inform_id = '"+str(inform_id)+"' and a.call_type like '%Di động - VNM%'"
        df_vnm = pd.read_sql(sql_vnm,db)
        if df_vnm.empty :
            content = ({"callType":"Di động - VNM Nội mạng","revenue": 0,"total_minute": 0,"price":0},{"callType":"Di động - VNM Ngoại mạng","revenue": 0,"total_minute": 0,"price":0})
            data_vnm = json.dumps(content)
            df_vnm = pd.DataFrame.from_dict(json.loads(data_vnm), orient='columns')
            json_records = df_vnm.to_json(orient ='records')
            data_vnm = []
            data_vnm = json.loads(json_records)
        else :
            #phân cách hàng nghìn bằng dấu ','
            df_vnm['revenue'] = df_vnm['revenue'].apply(lambda x: "{:,}".format(x))
            df_vnm['total_minute'] = df_vnm['total_minute'].apply(lambda x: "{:,}".format(x))
            df_vnm['created_time'] = df_vnm['created_time'].astype('str')
            json_records = df_vnm.to_json(orient ='records')
            data_vnm = []
            data_vnm = json.loads(json_records)
        # thông tin vnp
        sql_vnp ="SELECT a.id,a.created_time,a.partner_code, a.partner_name,a.call_type'callType',a.total_minute, a.price,a.revenue,a.status,a.inform_id FROM inform_cdr a WHERE  a.inform_id = '"+str(inform_id)+"' and a.call_type like '%Di động - VNP%'"
        df_vnp = pd.read_sql(sql_vnp,db)
        if df_vnp.empty :
            content = ({"callType":"Di động - VNP Nội mạng","revenue": 0,"total_minute": 0,"price":0},{"callType":"Di động - VNP Ngoại mạng","revenue": 0,"total_minute": 0,"price":0})
            data_vnp = json.dumps(content)
            df_vnp = pd.DataFrame.from_dict(json.loads(data_vnp), orient='columns')
            json_records = df_vnp.to_json(orient ='records')
            data_vnp = []
            data_vnp = json.loads(json_records)
        else :
            #phân cách hàng nghìn bằng dấu ','
            df_vnp['revenue'] = df_vnp['revenue'].apply(lambda x: "{:,}".format(x))
            df_vnp['total_minute'] = df_vnp['total_minute'].apply(lambda x: "{:,}".format(x))
            df_vnp['created_time'] = df_vnp['created_time'].astype('str')
            json_records = df_vnp.to_json(orient ='records')
            data_vnp = []
            data_vnp = json.loads(json_records)
        # thông tin co dịnh
        sql_codinh ="SELECT id,created_time,partner_code, partner_name,'Cố định'`callType`, IFNULL(sum(total_minute),0)'total_minute', price,IFNULL(sum(revenue),0)'revenue',status,inform_id FROM inform_cdr WHERE inform_id = '"+str(inform_id)+"' and call_type like '%Cố định%'"
        df_codinh = pd.read_sql(sql_codinh,db)
        if df_codinh.iloc[0]['revenue'] == "0" :
            content = ({"callType":"Cố định","revenue": 0,"total_minute": 0,"price":0})
            data_codinh = json.dumps(content)
            df_codinh = pd.DataFrame.from_dict(json.loads(data_codinh), orient='columns')
            json_records = df_codinh.to_json(orient ='records')
            data_codinh = []
            data_codinh = json.loads(json_records)
        else :
            #phân cách hàng nghìn bằng dấu ','
            df_codinh['revenue'] = df_codinh['revenue'].apply(lambda x: "{:,}".format(x))
            df_codinh['total_minute'] = df_codinh['total_minute'].apply(lambda x: "{:,}".format(x))
            df_codinh['created_time'] = df_codinh['created_time'].astype('str')
            df_codinh = df_codinh.to_json(orient ='records')
            data_codinh = []
            data_codinh = json.loads(json_records)
        #lấy thông tin partner
        sql_partner ="SELECT * FROM partner WHERE  nickname = (select partner_name from inform_cdr where inform_id = '"+str(inform_id)+"' limit 1)"
        df_partner = pd.read_sql(sql_partner,db)
        json_records = df_partner.to_json(orient ='records')
        data_partner = []
        data_partner = json.loads(json_records)
        #lấy total revenue
        sql_total ="SELECT SUM(revenue) as total_revenue FROM inform_cdr WHERE  inform_id = '"+str(inform_id)+"'"
        sql_total_number = "SELECT IFNULL(SUM(fee),0) FROM number_log WHERE action in('1','2') AND partner_code = (SELECT partner_code FROM inform_cdr WHERE inform_id = '"+str(inform_id)+"' LIMIT 1) and created_time like '%"+str(month)+"%'"
        df_total = pd.read_sql(sql_total,db)
        df_total_number = pd.read_sql(sql_total_number,db)
        total_revenue = df_total.iloc[0,0]
        total_number = df_total_number.iloc[0,0]
        total = int(total_revenue)+int(total_number)
        vat = int(int(total)*0.1)
        total_thanhtoan = int(total)+int(vat)
        total_revenue = "{:,}".format(total_revenue)
        total_number = "{:,}".format(total_number)
        total = "{:,}".format(total)
        vat = "{:,}".format(vat)
        total_thanhtoan = "{:,}".format(total_thanhtoan)
        # xóa các ký tự sau dấu . trong total
        total = str(total).replace(".0","")
        total_revenue = str(total_revenue).replace(".0","")
        total_number = str(total_number).replace(".0","")
        content = ({"type_total_revenue":"THUẾ VAT 10%","revenue": str(vat)})
        df_total = pd.DataFrame(content, index=[0])
        json_records = df_total.to_json(orient ='records')
        data_vat = []
        data_vat = json.loads(json_records)
        content = ({"type_total_revenue":"TỔNG II","revenue": str(total_revenue)})
        df_total = pd.DataFrame(content, index=[0])
        json_records = df_total.to_json(orient ='records')
        data_total_2 = []
        data_total_2 = json.loads(json_records)
        content = ({"type_total_revenue":"TỔNG I","revenue": str(total_number)})
        df_total = pd.DataFrame(content, index=[0])
        json_records = df_total.to_json(orient ='records')
        data_total_1 = []
        data_total_1 = json.loads(json_records)
        content = ({"type_total_revenue":"TỔNG CỘNG","revenue": str(total)})
        df_total = pd.DataFrame(content, index=[0])
        json_records = df_total.to_json(orient ='records')
        data_total = []
        data_total = json.loads(json_records)
        content = ({"type_total_revenue":"TỔNG SỐ TIỀN CẦN THANH TOÁN","revenue": str(total_thanhtoan)})
        df_total = pd.DataFrame(content, index=[0])
        json_records = df_total.to_json(orient ='records')
        data_thanhtoan = []
        data_thanhtoan = json.loads(json_records)
        # lấy thông tin inform_id
        sql_inform_id = "SELECT inform_id FROM inform_cdr WHERE  inform_id = '"+str(inform_id)+"' limit 1"
        df_inform_id = pd.read_sql(sql_inform_id,db)
        json_records = df_inform_id.to_json(orient ='records')
        data_inform_id = []
        data_inform_id = json.loads(json_records)
        context = {'numbermember_vt' : numbermember_vt,'numbermember_mbf' : numbermember_mbf,'numbermember_nvp' : numbermember_nvp,'numbermember_vnm' : numbermember_vnm,'numbermember_gmb' : numbermember_gmb,'numbermember_codinh' : numbermember_codinh,'data': data_inform_id,'data_vt': data_vt,'data_mbf': data_mbf,'data_vnm': data_vnm,'data_vnp': data_vnp,'data_gmb': data_gmb,'data_codinh': data_codinh,'data_partner': data_partner,'data_total': data_total,'data_total1': data_total_1,'data_total2': data_total_2,'data_date':data_date,'vat' : data_vat,'data_thanhtoan':data_thanhtoan,'code': 'OK'}
        db.close()
        return context
    except Exception as e:
        logging.error(traceback.format_exc())
        return jsonify({'NOTOK': 'NOTOK'}),400
@invoice.route('/saovang_list_invoice', methods=['GET'])
def saovang_list_invoice():
    try:
        db = pymysql.connect(host="172.17.0.1",    # your host, usually localhost
                     user="leeoncrm",         # your username
                     passwd="41XmKsO3NBgHPwv",  # your password
                     db="leeon_crm",
                     port=3306
                     )

        data = group()
        print(data)
        if str(data)== "chua truyen token" :
            return jsonify({'NOTOK': str(data)})
        else:
            timetoken = data["expireddate"]
            group_name = data["group_name"]
            if True:
                currentDate = time.strftime("%Y-%m-%d %H:%M:%S")
                logging.info(currentDate)
                if str(timetoken) >= str(currentDate) :
                    sql = "SELECT id,inform_id,partner_code,partner_name,partner_name'nickname',created_time,created_time'month',case when status = 0 then 'Chờ gửi' when status = 1 then 'Đã gửi' when status = 2 then 'Đã hủy' end as STATUS,updated_time FROM inform_cdr WHERE status != 2 and partner_name IN(SELECT nickname FROM partner WHERE agency_id =1) GROUP BY inform_id"
                    df = pd.read_sql(sql,db)
                    df['created_time'] = df['created_time'].astype(str)
                    #convert cột created_time sang dạng chuỗi
                    df['month'] = df['month'].astype(str)
                    #replace cột created_time
                    df['month'] = df['month'].str[0:7].replace('-','')
                    df['month'] = df['month'].str.replace('-','')
                    json_records = df.to_json(orient ='records')
                    data = []
                    data = json.loads(json_records)
                    context = {'data': data,'code': 'OK'}
                    db.close()
                    return context
                else :
                    return jsonify({'NOTOK': 'token hết hạn','code':401}),401
            else :
                return jsonify({'NOTOK': 'khong co quyen'})
    except Exception as e:
        logging.error(traceback.format_exc())
        return jsonify({'NOTOK': 'loi he thong'}),400

@invoice.route('/saovang_send_mail_invoice', methods=['POST'])
def saovang_send_mail_invoice():
    try:
        db = pymysql.connect(host="172.17.0.1",    # your host, usually localhost
                     user="leeoncrm",         # your username
                     passwd="41XmKsO3NBgHPwv",  # your password
                     db="leeon_crm",
                     port=3306
                     )
        cursor = db.cursor()
        data = group()
        print(data)
        if str(data)== "chua truyen token" :
            return jsonify({'NOTOK': str(data)})
        else:
            timetoken = data["expireddate"]
            group_name = data["group_name"]
            if True:
                record = json.loads(request.data)
                logging.info(record)
                inform_id = record["inform_id"]
                link = record["link"]
                email = record["email"]
                email_cc = record["email_cc"]
                information = str(inform_id).split('-')
                partner_code = information[0]
                # get nickname
                sql_nickname = "select * from partner where partner_code = '"+str(partner_code)+"'"
                df_nickname = pd.read_sql(sql_nickname,db)
                nickname = df_nickname.iloc[0]['nickname']
                address = df_nickname.iloc[0]['address']
                taxcode = df_nickname.iloc[0]['taxcode']
                month_invoice = str(information[1])[4:6]
                year_invoice = str(information[1])[0:4]
                time_invoices = str(month_invoice) + "/" + str(year_invoice)
                time_invoice = datetime.strptime(time_invoices, '%m/%Y')
                first_day = datetime(time_invoice.year, time_invoice.month, 1)
                first_day = first_day.strftime("%d/%m/%Y")
                last_day = datetime(time_invoice.year, time_invoice.month, calendar.monthrange(time_invoice.year, time_invoice.month)[1])
                last_day = last_day.strftime("%d/%m/%Y")
                #lấy ngày hiện tại
                currentDate = time.strftime("%d/%m/%Y")
                # get thông tin invoice thuê bao
                sql = "SELECT IFNULL(SUM(fee),0) FROM number_log WHERE action in('1','2') AND partner_code = (SELECT partner_code FROM inform_cdr WHERE inform_id = '"+str(inform_id)+"' LIMIT 1) and created_time like '%"+str(year_invoice)+"-"+str(month_invoice)+"%'"
                df = pd.read_sql(sql,db)
                thuebao = df.iloc[0,0]
                thuebao = int(int(thuebao)*1.1)
                # get thông tin invoice cước
                sql_total ="SELECT SUM(revenue) as total_revenue FROM inform_cdr WHERE  inform_id = '"+str(inform_id)+"'"
                df_total = pd.read_sql(sql_total,db)
                total_revenue = df_total.iloc[0,0]
                total_revenue = int(int(total_revenue)*1.1)
                total_fee = total_revenue + thuebao
                words_tien = number_to_words(total_fee)
                f = open("/home/dungnt/api_crm_v2/invoice_saovang.html", "r")
                html = f.read()
                thuebao = "{:,}".format(thuebao)
                total_revenue = "{:,}".format(total_revenue)
                total_fee = "{:,}".format(total_fee)
                html = str(html).replace("$link",str(link)).replace("$time_invoice",str(time_invoices)).replace("$nickname",str(nickname)).replace("$address",str(address)).replace("$taxcode",str(taxcode)).replace("$total_fee",str(total_fee)).replace("$last_day",str(last_day)).replace("$first_day",str(first_day)).replace("$thuebao",str(thuebao)).replace("$total_revenue",str(total_revenue)).replace("$currentDate",str(currentDate)).replace("$words_tien",str(words_tien)).replace("$month_invoice",str(month_invoice))
                f.close()
                content = MIMEText(html, 'html')
                mailuser='doisoat@svtelecom.vn'
                mailpassword='Sur57262'
                mailserver='smtp.office365.com'
                subject="[SAO VÀNG-SVFONE] - THÔNG BÁO CƯỚC DỊCH VỤ ĐIỆN THOẠI SVFONE THÁNG " + str(time_invoices)
                msg = MIMEMultipart()
                msg['From'] = 'doisoat@svtelecom.vn'
                msg['To'] = ', '.join([str(email)])
                msg['Cc'] = ', '.join([str(email_cc)])
                msg['Bcc'] = ', '.join(['dung.nt@leeon.vn', 'dung90.nt02@gmail.com'])
                msg['Subject'] = "%s" % Header(subject, 'utf-8')
                msg.attach(content)
                mailServer = smtplib.SMTP(mailserver, 25)
                mailServer.ehlo()
                mailServer.starttls()
                mailServer.ehlo()
                mailServer.login(mailuser, mailpassword)
                mailServer.sendmail(mailuser, [msg['To'], msg['Cc'], msg['Bcc']], msg.as_string())
                mailServer.quit()
                # update status
                sql_update = "UPDATE inform_cdr SET status = '1' WHERE inform_id = '"+str(inform_id)+"'"
                cursor.execute(sql_update)
                db.commit()
                db.close()
                return jsonify({'OK': 'OK'})
            else :
                return jsonify({'NOTOK': 'khong co quyen'})
    except Exception as e:
        logging.error(traceback.format_exc())
        return jsonify({'NOTOK': 'loi he thong'}),400
@invoice.route('/delete_invoice', methods=['DELETE'])
def delete_invoice():
    logging.info("----------------------invoice_detail-------------------------")
    try:
        db = pymysql.connect(host="172.17.0.1",    # your host, usually localhost
                     user="leeoncrm",         # your username
                     passwd="41XmKsO3NBgHPwv",  # your password
                     db="leeon_crm",
                     port=3306
                     )
        data = group()
        logging.info(data)
        if str(data)== "chua truyen token" :
            return jsonify({'NOTOK': str(data)})
        else:
            timetoken = data["expireddate"]
            group_name = data["group_name"]
            if True:
                currentDate = time.strftime("%Y-%m-%d %H:%M:%S")
                logging.info(currentDate)
                if str(timetoken) >= str(currentDate) :
                    # lấy thời gian hiện tại
                    record = json.loads(request.data)
                    logging.info(record)
                    inform_id = record["inform_id"]
                    sql="UPDATE inform_cdr SET status = 2 WHERE inform_id = '"+str(inform_id)+"'"
                    logging.info(sql)
                    cursor = db.cursor()
                    cursor.execute(sql)
                    db.commit()
                    return jsonify({'OK': 'OK'}),200
                else :
                    return jsonify({'NOTOK': 'token hết hạn','code':401}),401
            else :
                return jsonify({'NOTOK': 'khong co quyen'}),400
    except Exception as e:
        logging.error(traceback.format_exc())
        return jsonify({'NOTOK': 'NOTOK'}),400