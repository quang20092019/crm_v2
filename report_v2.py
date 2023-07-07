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
from datetime import datetime
import datetime
import re
import os,zipfile
import traceback
import math
def get_all_date(a,b):
    start_date = datetime.datetime.strptime(a, '%Y-%m-%d')
    end_date = datetime.datetime.strptime(b, '%Y-%m-%d')
    delta = end_date - start_date
    list_date = []
    for i in range(delta.days + 1):
        day = start_date + datetime.timedelta(days=i)
        list_date.append(day.strftime("%Y-%m-%d"))
    return list_date
report_v2 = Blueprint('report_v2', __name__)
@report_v2.route('/findreport_v2', methods=['GET'])
def findreport_v2():
    try:
        db = pymysql.connect(host="172.17.0.1",    # your host, usually localhost
                     user="leeoncrm",         # your username
                     passwd="41XmKsO3NBgHPwv",  # your password
                     db="leeon_crm",
                     port=3306
                     )

        data = group()
        nicknames=""
        print(data)
        if str(data)== "chua truyen token" :
            return jsonify({'NOTOK': str(data)})
        else:
            timetoken = data["expireddate"]
            group_name = data["group_name"]
            partner = data["partner"]
            partner_code = data["partner_code"]
            logging.info("partner : "+ str(partner))
            logging.info("partner_code : "+ str(partner_code))
            if str(partner) != "":
                partner= str(partner).replace("[","").replace("]","").replace("'","")
                partner =re.split('[,;/ ]+', partner)
                logging.info("partner : "+ str(partner))
                nicknames=[]
                for i in partner:
                    sqlpartnerid = "select partner_code from partner where nickname ='"+str(i)+"'"
                    dfpartnerid = pd.read_sql(sqlpartnerid,db)
                    if not dfpartnerid.empty:
                        nicknames.append(str(dfpartnerid.iloc[0,0]))
                logging.info("nickname"+str(nicknames))
            if True:
                currentDate = time.strftime("%Y-%m-%d %H:%M:%S")
                print(currentDate)
                if str(timetoken) >= str(currentDate) :
                    starttime = request.args.get('starttime')
                    endtime = request.args.get('endtime')
                    endtime = str(endtime) + " 23:59:59"
                    nickname1 = request.args.get('nickname1')
                    nickname2 = request.args.get('nickname2')
                    nickname3 = request.args.get('nickname3')
                    servicename = request.args.get('servicename')
                    telco = request.args.get('telco')
                    logging.info("telco : "+str(telco))
                    if len(starttime) == 0 :
                        timestartquery = ""
                    else :
                        timestartquery = " and a.createdtime >= '"+ str(starttime) +"'"
                    if len(endtime) == 0 :
                        timeendquery = ""
                    else :
                        timeendquery = " and a.createdtime <= '"+ str(endtime) +"'"
                    nickname=""
                    if len(nickname1) == 0 :
                        partner1query = ""
                    else :
                        nickname = nickname1
                        partner1query = " and a.partner01 = (SELECT partner_code FROM partner where nickname ='"+str(nickname1)+"')"
                    if len(nickname2) == 0 :
                        partner2query = ""
                    else :
                        nickname =nickname2
                        partner2query = " and a.partner02 = (SELECT partner_code FROM partner where nickname ='"+str(nickname2)+"')"
                    if len(nickname3) == 0 :
                        partner3query = ""
                    else :
                        nickname = nickname3
                        partner3query = " and a.partner03 = (SELECT partner_code FROM partner where nickname ='"+str(nickname3)+"')"
                    if len(servicename) == 0 :
                        servicenamequery = ""
                    else :
                        if str(servicename) == "SIP_TRUNK":
                            servicenamequery = " and a.isBrand = '1'"
                        else :
                            servicenamequery = " and a.isBrand = '0'"
                    sql =timestartquery + timeendquery+ partner1query + partner2query+ partner3query+ servicenamequery
                    if str(partner_code) == "None" and partner !='':
                        if len(nickname1)==0:
                            if str(partner) == "":
                                sqlfind = "SELECT a.id,LEFT(a.createdtime,10)'createdtime',(SUM(a.callsuccess)+SUM(a.callmiss))'calltotal',SUM(a.callsuccess)'callsuccess',SUM(a.callmiss) 'callmiss',ROUND(SUM(a.voicetime)/60,0)'voicetime',SUM(a.revenue)'revenue',a.vosIp,b.nickname,a.telco,CONCAT(ROUND((SUM(a.callmiss)/(SUM(a.callsuccess)+SUM(a.callmiss)))*100,2),' %')'callrate',case when a.isBrand = '0' then 'BRAND' else 'SIP_TRUNK' end as servicename FROM report_v2 a,partner b WHERE RIGHT(a.partner_code,5) = b.partner_code and telco like '%"+str(telco)+"%'"+sql+" GROUP BY a.partner_code ,LEFT(a.createdtime,10),a.isBrand"
                            else :
                                sqll=""
                                for i in nicknames:
                                    sql_join = "(SELECT a.id,LEFT(a.createdtime,10)'createdtime',(SUM(a.callsuccess)+SUM(a.callmiss))'calltotal',SUM(a.callsuccess)'callsuccess',SUM(a.callmiss) 'callmiss',ROUND(SUM(a.voicetime)/60,0)'voicetime',SUM(a.revenue)'revenue',a.vosIp,b.nickname,a.telco,CONCAT(ROUND((SUM(a.callmiss)/(SUM(a.callsuccess)+SUM(a.callmiss)))*100,2),' %')'callrate',case when a.isBrand = '0' then 'BRAND' else 'SIP_TRUNK' end as servicename FROM report_v2 a,partner b WHERE RIGHT(a.partner_code,5) = b.partner_code and telco like '%"+str(telco)+"%' and a.partner_code like '%"+str(i)+"%'"+sql+" GROUP BY a.partner_code ,LEFT(a.createdtime,10),a.isBrand)  union all"
                                    sqll = sqll + sql_join
                                sqlfind = sqll[:-9]
                        else :
                            sqlfind = "SELECT a.id,LEFT(a.createdtime,10)'createdtime',(SUM(a.callsuccess)+SUM(a.callmiss))'calltotal',SUM(a.callsuccess)'callsuccess',SUM(a.callmiss) 'callmiss',ROUND(SUM(a.voicetime)/60,0)'voicetime',SUM(a.revenue)'revenue',a.vosIp,'"+str(nickname)+"'`nickname`,a.telco,CONCAT(ROUND((SUM(a.callmiss)/(SUM(a.callsuccess)+SUM(a.callmiss)))*100,2),' %')'callrate',case when a.isBrand = '0' then 'BRAND' else 'SIP_TRUNK' end as servicename FROM report_v2 a WHERE a.telco like '%"+str(telco)+"%'"+sql+" GROUP BY LEFT(a.createdtime,10),a.isBrand"
                    else :
                        if len(nickname1)==0:
                            sqlfind = "SELECT a.id,LEFT(a.createdtime,10)'createdtime',(SUM(a.callsuccess)+SUM(a.callmiss))'calltotal',SUM(a.callsuccess)'callsuccess',SUM(a.callmiss) 'callmiss',ROUND(SUM(a.voicetime)/60,0)'voicetime',SUM(a.revenue)'revenue',a.vosIp,b.nickname,a.telco,CONCAT(ROUND((SUM(a.callmiss)/(SUM(a.callsuccess)+SUM(a.callmiss)))*100,2),' %')'callrate',case when a.isBrand = '0' then 'BRAND' else 'SIP_TRUNK' end as servicename FROM report_v2 a,partner b WHERE RIGHT(a.partner_code,5) = b.partner_code and telco like '%"+str(telco)+"%'"+sql+" GROUP BY a.partner_code ,LEFT(a.createdtime,10),a.isBrand"
                        else :
                            sqlfind = "SELECT a.id,LEFT(a.createdtime,10)'createdtime',(SUM(a.callsuccess)+SUM(a.callmiss))'calltotal',SUM(a.callsuccess)'callsuccess',SUM(a.callmiss) 'callmiss',ROUND(SUM(a.voicetime)/60,0)'voicetime',SUM(a.revenue)'revenue',a.vosIp,'"+str(nickname)+"'`nickname`,a.telco,CONCAT(ROUND((SUM(a.callmiss)/(SUM(a.callsuccess)+SUM(a.callmiss)))*100,2),' %')'callrate',case when a.isBrand = '0' then 'BRAND' else 'SIP_TRUNK' end as servicename FROM report_v2 a WHERE a.telco like '%"+str(telco)+"%'"+sql+" GROUP BY LEFT(a.createdtime,10),a.isBrand"
                    df = pd.read_sql(sqlfind,db)
                    df["createdtime"] = pd.to_datetime(df["createdtime"])
                    df = df.sort_values(by=['createdtime'], ascending=False)
                    df['createdtime'] = df['createdtime'].astype('str')
                    logging.info("sqlfind : "+str(sqlfind))
                    json_records = df.to_json(orient ='records')
                    data = []
                    # id =  df.iloc[0]['id']
                    # if str(id) !="None":
                    data = json.loads(json_records)
                    if str(partner_code) == "None" and partner !='':
                        sqll="SELECT id,createdtime,sum(calltotal)'calltotal',sum(callsuccess)'callsuccess',sum(callmiss)'callmiss',sum(voicetime)'voicetime',SUM(revenue)'revenue',ROUND((SUM(callmiss)/(SUM(callsuccess)+SUM(callmiss)))*100,2)'callrate' FROM ("
                        for i in nicknames:
                            sql_join = "(SELECT a.id,LEFT(a.createdtime,10)'createdtime',(SUM(a.callsuccess)+SUM(a.callmiss))'calltotal',SUM(a.callsuccess)'callsuccess',SUM(a.callmiss) 'callmiss',ROUND(SUM(a.voicetime)/60,0)'voicetime',SUM(a.revenue)'revenue',a.vosIp,b.nickname,a.telco,CONCAT(ROUND((SUM(a.callmiss)/(SUM(a.callsuccess)+SUM(a.callmiss)))*100,2),' %')'callrate',case when a.isBrand = '0' then 'BRAND' else 'SIP_TRUNK' end as servicename FROM report_v2 a,partner b WHERE RIGHT(a.partner_code,5) = b.partner_code and telco like '%"+str(telco)+"%' and a.partner_code like '%"+str(i)+"%'"+sql+" GROUP BY a.partner_code ,LEFT(a.createdtime,10),a.isBrand) union all"
                            sqll = sqll + sql_join
                        sqltotal = sqll[:-10] + ") as tmp"
                    else :
                        sqltotal = "SELECT a.id,LEFT(a.createdtime,10)'createdtime',(SUM(a.callsuccess)+SUM(a.callmiss))'calltotal',SUM(a.callsuccess)'callsuccess',SUM(a.callmiss) 'callmiss',ROUND(SUM(a.voicetime)/60,0)'voicetime',SUM(a.revenue)'revenue',ROUND((SUM(a.callmiss)/(SUM(a.callsuccess)+SUM(a.callmiss)))*100,2)'callrate' FROM report_v2 a WHERE telco like '%"+str(telco)+"%'"+sql
                    logging.info("sqltotal : "+str(sqltotal))
                    dftotal = pd.read_sql(sqltotal,db)
                    json_recordstotal = dftotal.to_json(orient ='records')
                    datatotal = []
                    datatotal = json.loads(json_recordstotal)
                    context = {'data': data,'total':datatotal,'code': 'OK'}
                    db.close()
                    return context
                else :
                    return jsonify({'NOTOK': 'token hết hạn','code':401}),401
            else :
                return jsonify({'NOTOK': 'khong co quyen'})
    except Exception as e:
        logging.error(traceback.format_exc())
        return jsonify({'NOTOK': str(e)})
@report_v2.route('/find_report_v2_detail', methods=['GET'])
def find_report_v2_detail():
    logging.info("----------------------find_report_v2_detail-------------------------")
    try:
        db = pymysql.connect(host="127.0.0.1",    # your host, usually localhost
                     user="leeonbilling",         # your username
                     passwd="sT1iYy7ApRLOW9G09384",  # your password
                     db="leeon_billing",
                     port=3306
                     )
        db1 = pymysql.connect(host="172.17.0.1",    # your host, usually localhost
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
            partner_code= data["partner_code"]
            if True:
                currentDate = time.strftime("%Y-%m-%d %H:%M:%S")
                logging.info(currentDate)
                if str(timetoken) >= str(currentDate) :
                    # lấy thời gian hiện tại
                    row_page = 50
                    offset = request.args.get('offset')
                    logging.info("offset : "+ str(offset))
                    starttime = request.args.get('starttime')
                    logging.info("starttime : "+ str(starttime))
                    endtime = request.args.get('endtime')
                    nickname1 = request.args.get('nickname1')
                    nickname2 = request.args.get('nickname2')
                    nickname3 = request.args.get('nickname3')

                    if len(nickname1) == 0 :
                        partner1query = ""
                    else :
                        nickname = nickname1
                        sql_partner_code= "SELECT partner_code FROM partner where nickname ='"+str(nickname1)+"'"
                        df_partner_code = pd.read_sql(sql_partner_code,db1)
                        partner_code = df_partner_code.iloc[0]['partner_code']
                        partner1query = " partner01 = '"+str(partner_code)+"'"
                    if len(nickname2) == 0 :
                        partner2query = ""
                    else :
                        nickname =nickname2
                        sql_partner_code2= "SELECT partner_code FROM partner where nickname ='"+str(nickname2)+"'"
                        df_partner_code2 = pd.read_sql(sql_partner_code2,db1)
                        partner_code2 = df_partner_code2.iloc[0]['partner_code']
                        partner2query = " and partner02 = '"+str(partner_code2)+"'"
                    if len(nickname3) == 0 :
                        partner3query = ""
                    else :
                        nickname = nickname3
                        sql_partner_code3= "SELECT partner_code FROM partner where nickname ='"+str(nickname3)+"'"
                        df_partner_code3 = pd.read_sql(sql_partner_code3,db1)
                        partner_code3 = df_partner_code3.iloc[0]['partner_code']
                        partner3query = " and partner03 = '"+str(partner_code3)+"'"
                    nicknamequery = partner1query + partner2query + partner3query
                    list_date = get_all_date(starttime,endtime)
                    sql_find_detail = ""
                    sql_total="SELECT SUM(count1) FROM ("
                    for i in range(len(list_date)):
                        logging.info("list_date : "+ str(list_date[i]))
                        #lấy 1 ngày sau khi chuyển đổi
                        now = datetime.datetime.strptime(list_date[i], '%Y-%m-%d')
                        #lấy ngày sau khi chuyển đổi +1
                        next_day = now + datetime.timedelta(days=1)
                        partition = "p"+str(now).replace("-","")[0:8]
                        logging.info("partition : "+ str(partition))
                        # lấy tên bảng
                        table = "cdr_billing_"+str(now).replace("-","")[0:6]
                        logging.info("table : "+ str(table))
                        #query total
                        sql= "SELECT COUNT(1)'count1' FROM "+str(table)+" PARTITION("+str(partition)+") WHERE"+ str(nicknamequery) + " union all "
                        sql_total =sql_total+sql
                        #query find detail
                        sql_find = "SELECT a.id,a.caller,b.telcoName'callerTelcoId',a.callee,c.telcoName'calleeTelcoid',a.callerRtpIp,a.startTime,a.stopTime,a.billTime,a.billAmount,a.callercallid'callercallid',a.brandName,a.cdrId,CASE WHEN a.isMnp = 1 THEN 'True' ELSE 'False' END AS isMnp,d.callType,CASE WHEN a.isBrand =1 THEN 'Sip' ELSE 'Brandname' END AS isBrand ,CASE WHEN a.endReason = '-8' THEN 'CalleeHangup' WHEN a.endReason = '-7' THEN 'CallerHangup' ELSE 'Other' END AS endReason,a.partnerTag FROM (SELECT * FROM "+str(table)+" PARTITION("+str(partition)+") WHERE "+ str(nicknamequery) + ") a,service_prefix b ,service_prefix c,service_type d WHERE a.callerTelcoId = b.id AND a.calleeTelcoid = c.id AND a.callType =d.id union all "
                        sql_find_detail = sql_find_detail + sql_find
                    sql_total = sql_total[:-11] + ") AS tmp"
                    logging.info("sql_total : "+ str(sql_total))
                    df_total = pd.read_sql(sql_total,db)
                    total = df_total.iloc[0,0]
                    logging.info("total : " +str(total))
                    total_page = math.ceil(total/row_page)
                    logging.info("total_page : " +str(total_page))
                    if str(offset) == "None" or str(offset) == "0":
                        sql_find_detail = sql_find_detail[:-11]+" limit "+str(row_page)
                    else :
                        offset = int(offset)-1
                        limit_offset = int(offset)*int(row_page)-int(row_page)
                        logging.info("limit_offset : " +str(limit_offset))
                        sql_find_detail = sql_find_detail[:-11]+" limit "+str(row_page)+" offset "+str(offset)
                    logging.info("sql_find_detail : "+ str(sql_find_detail))
                    df = pd.read_sql(sql_find_detail,db)
                    df['startTime'] = df['startTime'].astype('str')
                    df['stopTime'] = df['stopTime'].astype('str')
                    json_records = df.to_json(orient ='records')
                    data = []
                    data = json.loads(json_records)
                    context = {'data': data,'total_page': total_page,'current_page' : str(offset),'code': 'OK'}
                    db.close()
                    return jsonify(context)
                else :
                    return jsonify({'NOTOK': 'token hết hạn','code':401}),401
            else :
                return jsonify({'NOTOK': 'khong co quyen'}),400
    except Exception as e:
        logging.error(traceback.format_exc())
        return jsonify({'NOTOK': 'NOTOK'}),400
        
@report_v2.route('/download_report_v2_detail', methods=['POST'])
def download_report_v2_detail():
    logging.info("----------------------download_report_v2_detail-------------------------")
    try:
        db = pymysql.connect(host="127.0.0.1",    # your host, usually localhost
                     user="leeonbilling",         # your username
                     passwd="sT1iYy7ApRLOW9G09384",  # your password
                     db="leeon_billing",
                     port=3306
                     )
        db1 = pymysql.connect(host="172.17.0.1",    # your host, usually localhost
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
                    try:
                        starttime = record["starttime"]
                        endtime = record["endtime"]
                        nickname1 = record["selectedPartner"]
                        nickname2 = record["selectedPartner2"]
                        nickname3 = record["selectedPartner3"]
                    except Exception as e:
                        logging.error(traceback.format_exc())
                        return jsonify({'NOTOK': 'Vui lòng chọn ngày và tên đối tác'}),400
                    if len(nickname1) == 0 :
                        partner1query = ""
                    else :
                        nickname = nickname1
                        sql_partner_code= "SELECT partner_code FROM partner where nickname ='"+str(nickname1)+"'"
                        df_partner_code = pd.read_sql(sql_partner_code,db1)
                        partner_code = df_partner_code.iloc[0]['partner_code']
                        partner1query = " partner01 = '"+str(partner_code)+"'"
                    if len(nickname2) == 0 :
                        partner2query = ""
                    else :
                        nickname =nickname2
                        sql_partner_code2= "SELECT partner_code FROM partner where nickname ='"+str(nickname2)+"'"
                        df_partner_code2 = pd.read_sql(sql_partner_code2,db1)
                        partner_code2 = df_partner_code2.iloc[0]['partner_code']
                        partner2query = " and partner02 = '"+str(partner_code2)+"'"
                    if len(nickname3) == 0 :
                        partner3query = ""
                    else :
                        nickname = nickname3
                        sql_partner_code3= "SELECT partner_code FROM partner where nickname ='"+str(nickname3)+"'"
                        df_partner_code3 = pd.read_sql(sql_partner_code3,db1)
                        partner_code3 = df_partner_code3.iloc[0]['partner_code']
                        partner3query = " and partner03 = '"+str(partner_code3)+"'"
                    nicknamequery = partner1query + partner2query + partner3query
                    list_date = get_all_date(starttime,endtime)
                    sql_find_detail = ""
                    for i in range(len(list_date)):
                    # lấy query đúng
                        logging.info("list_date : "+ str(list_date[i]))
                        #lấy 1 ngày sau khi chuyển đổi
                        now = datetime.datetime.strptime(list_date[i], '%Y-%m-%d')
                        #lấy ngày sau khi chuyển đổi +1
                        next_day = now + datetime.timedelta(days=1)
                        # lấy tên bảng
                        table = "cdr_billing_"+str(now).replace("-","")[0:6]
                        logging.info("table : "+ str(table))
                        # get info table
                        sql_info = "SHOW CREATE TABLE "+table
                        df_info = pd.read_sql(sql_info,db)
                        info = df_info.iloc[0,1]
                        partition = "p"+str(now).replace("-","")[0:8]
                        if partition not in str(info):
                            partition = "pfuture"
                        logging.info("partition : "+ str(partition))               
                        sql_find = "SELECT a.caller,b.telcoName'callerTelcoId',a.callee,c.telcoName'calleeTelcoid',a.callerRtpIp,a.startTime,a.stopTime,a.billTime,a.billAmount,a.callercallid'callercallid',a.brandName,CASE WHEN a.isMnp = 1 THEN 'True' ELSE 'False' END AS isMnp,d.callType,CASE WHEN a.isBrand =1 THEN 'Sip' ELSE 'Brandname' END AS isBrand ,CASE WHEN a.endReason = '-8' THEN 'CalleeHangup' WHEN a.endReason = '-7' THEN 'CallerHangup' ELSE 'other' END AS endReason FROM (SELECT * FROM "+str(table)+" PARTITION("+str(partition)+") WHERE "+ str(nicknamequery) + ") a,service_prefix b ,service_prefix c,service_type d WHERE a.callerTelcoId = b.id AND a.calleeTelcoid = c.id AND a.callType =d.id union all "
                        sql_find_detail = sql_find_detail + sql_find
                    sql_find_detail = sql_find_detail[:-11]
                    logging.info(sql_find_detail)
                    df = pd.read_sql(sql_find_detail,db)
                    logging.info("lấy dữ liệu xong")
                    df['startTime'] = df['startTime'].astype('str')
                    df['stopTime'] = df['stopTime'].astype('str')
                    session=datetime.datetime.now()
                    filename = "report_v2"+str(session).replace(" ","")+".csv"
                    filepath = "filedownload/"+str(nickname)+"_"+str(session).replace(" ","").replace("-","").replace(".","")[0:8]
                    # taọ thư mục filepath
                    if not os.path.exists(filepath):
                        os.mkdir(filepath)
                    path = os.path.join(os.getcwd(),filepath,filename)
                    path_export = os.path.join(os.getcwd(),filepath,'report_v2')
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
                    logging.info("xong")
                    return send_file(zip_name, as_attachment=True)
                else :
                    return jsonify({'NOTOK': 'token hết hạn','code':401}),401
            else :
                return jsonify({'NOTOK': 'khong co quyen'}),400
    except Exception as e:
        logging.error(traceback.format_exc())
        return jsonify({'NOTOK': 'NOTOK'}),400
@report_v2.route('/saovang_find_report_v2_detail', methods=['GET'])
def saovang_find_report_v2_detail():
    logging.info("----------------------find_report_v2_detail-------------------------")
    try:
        db = pymysql.connect(host="127.0.0.1",    # your host, usually localhost
                     user="leeonbilling",         # your username
                     passwd="sT1iYy7ApRLOW9G09384",  # your password
                     db="leeon_billing",
                     port=3306
                     )
        db1 = pymysql.connect(host="172.17.0.1",    # your host, usually localhost
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
            partner_code= data["partner_code"]
            if True:
                currentDate = time.strftime("%Y-%m-%d %H:%M:%S")
                logging.info(currentDate)
                if str(timetoken) >= str(currentDate) :
                    # lấy thời gian hiện tại
                    row_page = 50
                    offset = request.args.get('offset')
                    logging.info("offset : "+ str(offset))
                    starttime = request.args.get('starttime')
                    logging.info("starttime : "+ str(starttime))
                    endtime = request.args.get('endtime')
                    nickname1 = request.args.get('nickname1')
                    nickname2 = request.args.get('nickname2')
                    nickname3 = request.args.get('nickname3')

                    if len(nickname1) == 0 :
                        partner1query = ""
                    else :
                        nickname = nickname1
                        sql_partner_code= "SELECT partner_code FROM partner where nickname ='"+str(nickname1)+"'"
                        df_partner_code = pd.read_sql(sql_partner_code,db1)
                        partner_code = df_partner_code.iloc[0]['partner_code']
                        partner1query = " partner01 = '"+str(partner_code)+"'"
                    if len(nickname2) == 0 :
                        partner2query = ""
                    else :
                        nickname =nickname2
                        sql_partner_code2= "SELECT partner_code FROM partner where nickname ='"+str(nickname2)+"'"
                        df_partner_code2 = pd.read_sql(sql_partner_code2,db1)
                        partner_code2 = df_partner_code2.iloc[0]['partner_code']
                        partner2query = " and partner02 = '"+str(partner_code2)+"'"
                    if len(nickname3) == 0 :
                        partner3query = ""
                    else :
                        nickname = nickname3
                        sql_partner_code3= "SELECT partner_code FROM partner where nickname ='"+str(nickname3)+"'"
                        df_partner_code3 = pd.read_sql(sql_partner_code3,db1)
                        partner_code3 = df_partner_code3.iloc[0]['partner_code']
                        partner3query = " and partner03 = '"+str(partner_code3)+"'"
                    nicknamequery = partner1query + partner2query + partner3query
                    list_date = get_all_date(starttime,endtime)
                    sql_find_detail = ""
                    sql_total="SELECT SUM(count1) FROM ("
                    for i in range(len(list_date)):
                        logging.info("list_date : "+ str(list_date[i]))
                        #lấy 1 ngày sau khi chuyển đổi
                        now = datetime.datetime.strptime(list_date[i], '%Y-%m-%d')
                        #lấy ngày sau khi chuyển đổi +1
                        next_day = now + datetime.timedelta(days=1)
                        partition = "p"+str(now).replace("-","")[0:8]
                        logging.info("partition : "+ str(partition))
                        # lấy tên bảng
                        table = "cdr_billing_"+str(now).replace("-","")[0:6]
                        logging.info("table : "+ str(table))
                        #query total
                        sql= "SELECT COUNT(1)'count1' FROM "+str(table)+" PARTITION("+str(partition)+") WHERE"+ str(nicknamequery) + " union all "
                        sql_total =sql_total+sql
                        #query find detail
                        sql_find = "SELECT a.id,a.caller,b.telcoName'callerTelcoId',a.callee,c.telcoName'calleeTelcoid',a.callerRtpIp,a.startTime,a.stopTime,a.billTime,a.billAmount,a.holdTime,a.callercallid'callercallid',a.brandName,a.cdrId,CASE WHEN a.isMnp = 1 THEN 'True' ELSE 'False' END AS isMnp,d.callType,CASE WHEN a.isBrand =1 THEN 'Sip' ELSE 'Brandname' END AS isBrand ,CASE WHEN a.endReason = '-8' THEN 'CalleeHangup' WHEN a.endReason = '-7' THEN 'CallerHangup' ELSE 'Other' END AS endReason,a.partnerTag FROM (SELECT * FROM "+str(table)+" PARTITION("+str(partition)+") WHERE "+ str(nicknamequery) + ") a,service_prefix b ,service_prefix c,service_type d WHERE a.callerTelcoId = b.id AND a.calleeTelcoid = c.id AND a.callType =d.id union all "
                        sql_find_detail = sql_find_detail + sql_find
                    sql_total = sql_total[:-11] + ") AS tmp"
                    logging.info("sql_total : "+ str(sql_total))
                    df_total = pd.read_sql(sql_total,db)
                    total = df_total.iloc[0,0]
                    logging.info("total : " +str(total))
                    total_page = math.ceil(total/row_page)
                    logging.info("total_page : " +str(total_page))
                    if str(offset) == "None" or str(offset) == "0":
                        sql_find_detail = sql_find_detail[:-11]+" limit "+str(row_page)
                    else :
                        offset = int(offset)-1
                        limit_offset = int(offset)*int(row_page)-int(row_page)
                        logging.info("limit_offset : " +str(limit_offset))
                        sql_find_detail = sql_find_detail[:-11]+" limit "+str(row_page)+" offset "+str(offset)
                    logging.info("sql_find_detail : "+ str(sql_find_detail))
                    df = pd.read_sql(sql_find_detail,db)
                    df['startTime'] = df['startTime'].astype('str')
                    df['stopTime'] = df['stopTime'].astype('str')
                    json_records = df.to_json(orient ='records')
                    data = []
                    data = json.loads(json_records)
                    context = {'data': data,'total_page': total_page,'current_page' : str(offset),'code': 'OK'}
                    db.close()
                    return jsonify(context)
                else :
                    return jsonify({'NOTOK': 'token hết hạn','code':401}),401
            else :
                return jsonify({'NOTOK': 'khong co quyen'}),400
    except Exception as e:
        logging.error(traceback.format_exc())
        return jsonify({'NOTOK': 'NOTOK'}),400
@report_v2.route('/saovang_download_report_v2_detail', methods=['POST'])
def saovang_download_report_v2_detail():
    logging.info("----------------------download_report_v2_detail-------------------------")
    try:
        db = pymysql.connect(host="127.0.0.1",    # your host, usually localhost
                     user="leeonbilling",         # your username
                     passwd="sT1iYy7ApRLOW9G09384",  # your password
                     db="leeon_billing",
                     port=3306
                     )
        db1 = pymysql.connect(host="172.17.0.1",    # your host, usually localhost
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
                    try:
                        starttime = record["starttime"]
                        endtime = record["endtime"]
                        nickname1 = record["selectedPartner"]
                        nickname2 = record["selectedPartner2"]
                        nickname3 = record["selectedPartner3"]
                    except Exception as e:
                        logging.error(traceback.format_exc())
                        return jsonify({'NOTOK': 'Vui lòng chọn ngày và tên đối tác'}),400
                    if len(nickname1) == 0 :
                        partner1query = ""
                    else :
                        nickname = nickname1
                        sql_partner_code= "SELECT partner_code FROM partner where nickname ='"+str(nickname1)+"'"
                        df_partner_code = pd.read_sql(sql_partner_code,db1)
                        partner_code = df_partner_code.iloc[0]['partner_code']
                        partner1query = " partner01 = '"+str(partner_code)+"'"
                    if len(nickname2) == 0 :
                        partner2query = ""
                    else :
                        nickname =nickname2
                        sql_partner_code2= "SELECT partner_code FROM partner where nickname ='"+str(nickname2)+"'"
                        df_partner_code2 = pd.read_sql(sql_partner_code2,db1)
                        partner_code2 = df_partner_code2.iloc[0]['partner_code']
                        partner2query = " and partner02 = '"+str(partner_code2)+"'"
                    if len(nickname3) == 0 :
                        partner3query = ""
                    else :
                        nickname = nickname3
                        sql_partner_code3= "SELECT partner_code FROM partner where nickname ='"+str(nickname3)+"'"
                        df_partner_code3 = pd.read_sql(sql_partner_code3,db1)
                        partner_code3 = df_partner_code3.iloc[0]['partner_code']
                        partner3query = " and partner03 = '"+str(partner_code3)+"'"
                    nicknamequery = partner1query + partner2query + partner3query
                    list_date = get_all_date(starttime,endtime)
                    sql_find_detail = ""
                    for i in range(len(list_date)):
                    # lấy query đúng
                        logging.info("list_date : "+ str(list_date[i]))
                        #lấy 1 ngày sau khi chuyển đổi
                        now = datetime.datetime.strptime(list_date[i], '%Y-%m-%d')
                        #lấy ngày sau khi chuyển đổi +1
                        next_day = now + datetime.timedelta(days=1)
                        # lấy tên bảng
                        table = "cdr_billing_"+str(now).replace("-","")[0:6]
                        logging.info("table : "+ str(table))
                        # get info table
                        sql_info = "SHOW CREATE TABLE "+table
                        df_info = pd.read_sql(sql_info,db)
                        info = df_info.iloc[0,1]
                        partition = "p"+str(now).replace("-","")[0:8]
                        if partition not in str(info):
                            partition = "pfuture"
                        logging.info("partition : "+ str(partition))               
                        sql_find = "SELECT a.caller,b.telcoName'callerTelcoId',a.callee,c.telcoName'calleeTelcoid',a.callerRtpIp,a.startTime,a.stopTime,a.billTime,a.billAmount,a.holdTime,a.callercallid'callercallid',a.brandName,CASE WHEN a.isMnp = 1 THEN 'True' ELSE 'False' END AS isMnp,d.callType,CASE WHEN a.isBrand =1 THEN 'Sip' ELSE 'Brandname' END AS isBrand ,CASE WHEN a.endReason = '-8' THEN 'CalleeHangup' WHEN a.endReason = '-7' THEN 'CallerHangup' ELSE 'other' END AS endReason FROM (SELECT * FROM "+str(table)+" PARTITION("+str(partition)+") WHERE "+ str(nicknamequery) + ") a,service_prefix b ,service_prefix c,service_type d WHERE a.callerTelcoId = b.id AND a.calleeTelcoid = c.id AND a.callType =d.id union all "
                        sql_find_detail = sql_find_detail + sql_find
                    sql_find_detail = sql_find_detail[:-11]
                    logging.info(sql_find_detail)
                    df = pd.read_sql(sql_find_detail,db)
                    logging.info("lấy dữ liệu xong")
                    df['startTime'] = df['startTime'].astype('str')
                    df['stopTime'] = df['stopTime'].astype('str')
                    session=datetime.datetime.now()
                    filename = "report_v2"+str(session).replace(" ","")+".csv"
                    filepath = "filedownload/"+str(nickname)+"_"+str(session).replace(" ","").replace("-","").replace(".","")[0:8]
                    # taọ thư mục filepath
                    if not os.path.exists(filepath):
                        os.mkdir(filepath)
                    path = os.path.join(os.getcwd(),filepath,filename)
                    path_export = os.path.join(os.getcwd(),filepath,'report_v2')
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
                    logging.info("xong")
                    return send_file(zip_name, as_attachment=True)
                else :
                    return jsonify({'NOTOK': 'token hết hạn','code':401}),401
            else :
                return jsonify({'NOTOK': 'khong co quyen'}),400
    except Exception as e:
        logging.error(traceback.format_exc())
        return jsonify({'NOTOK': 'NOTOK'}),400