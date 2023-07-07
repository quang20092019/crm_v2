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
report_exception = Blueprint('report_exception', __name__)
@report_exception.route('/saovang_report', methods=['GET'])
def saovang_report():
    logging.info("-------------------report_exception----------------------")
    try:
        db = pymysql.connect(host="172.17.0.1",    # your host, usually localhost
                     user="leeoncrm",         # your username
                     passwd="41XmKsO3NBgHPwv",  # your password
                     db="leeon_crm",
                     port=3306
                     )
        data = group()
        logging.info("data login : "+ str(data))
        if str(data)== "chua truyen token" :
            return jsonify({'NOTOK': str(data)})
        else:
            timetoken = data["expireddate"]
            group_name = data["group_name"]
            partner_code = data["partner_code"]
            if str(group_name) == "CUSTOMER":
                logging.info("partner_code : "+ str(partner_code))
                sql= "SELECT a.id,LEFT(a.createdtime,10)'createdtime',(SUM(a.callsuccess)+SUM(a.callmiss))'calltotal',SUM(a.callsuccess)'callsuccess',SUM(a.callmiss) 'callmiss',ROUND(SUM(a.voicetime)/60,0)'voicetime',SUM(a.revenue)'revenue',a.vosIp,b.nickname,a.telco,CONCAT(ROUND((SUM(a.callmiss)/(SUM(a.callsuccess)+SUM(a.callmiss)))*100,2),' %')'callrate',case when a.isBrand = '0' then 'BRAND' else 'SIP_TRUNK' end as servicename FROM report a,partner b WHERE a.createdtime >= CURDATE() AND a.partner_code REGEXP '"+str(partner_code)+"' AND RIGHT(a.partner_code,5) = b.partner_code"
            else :
                # lấy tất cả partner_code liên quan
                sql_partner_code = "SELECT partner_code from partner WHERE agency_id= 1"
                df_partnercode = pd.read_sql(sql_partner_code,db)
                # lưu tất cả partner1 , partner2 , partner3 vào 1 list
                list_partner = df_partnercode['partner_code'].tolist()
                logging.info("list_partner : "+ str(list_partner))
                list_partner = str(list_partner).replace("[","'").replace("]","'").replace(", ","|")
                sql= "SELECT a.id,LEFT(a.createdtime,10)'createdtime',(SUM(a.callsuccess)+SUM(a.callmiss))'calltotal',SUM(a.callsuccess)'callsuccess',SUM(a.callmiss) 'callmiss',ROUND(SUM(a.voicetime)/60,0)'voicetime',SUM(a.revenue)'revenue',a.vosIp,b.nickname,a.telco,CONCAT(ROUND((SUM(a.callmiss)/(SUM(a.callsuccess)+SUM(a.callmiss)))*100,2),' %')'callrate',case when a.isBrand = '0' then 'BRAND' else 'SIP_TRUNK' end as servicename FROM report a,partner b WHERE a.createdtime >= CURDATE() AND a.partner_code REGEXP "+str(list_partner)+" AND RIGHT(a.partner_code,5) = b.partner_code GROUP BY a.partner_code"
            logging.info("sql_report_exception :" + sql)
            df = pd.read_sql(sql,db)
            df['calltotal'] = df['calltotal'].astype('int')
            df['callsuccess'] = df['callsuccess'].astype('int')
            df['voicetime'] = df['voicetime'].astype('int')
            df['revenue'] = df['revenue'].astype('int')
            json_records = df.to_json(orient ='records')
            data = []
            data = json.loads(json_records)
            context = {'data': data,'code': 'OK'}
            db.close()
            return context
    except Exception as e:
        return jsonify({'NOTOK': str(e)})
        logging.error(traceback.format_exc())
@report_exception.route('/saovang_findreport', methods=['GET'])
def saovang_findreport():
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
            partner_code = data["partner_code"]
            logging.info("partner_code : "+ str(partner_code))
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
                    if str(group_name) == "CUSTOMER":
                        sqlfind = "SELECT a.id,LEFT(a.createdtime,10)'createdtime',(SUM(a.callsuccess)+SUM(a.callmiss))'calltotal',SUM(a.callsuccess)'callsuccess',SUM(a.callmiss) 'callmiss',ROUND(SUM(a.voicetime)/60,0)'voicetime',SUM(a.revenue)'revenue',a.vosIp,b.nickname,a.telco,CONCAT(ROUND((SUM(a.callmiss)/(SUM(a.callsuccess)+SUM(a.callmiss)))*100,2),' %')'callrate',case when a.isBrand = '0' then 'BRAND' else 'SIP_TRUNK' end as servicename FROM report a,partner b WHERE a.partner_code = '"+str(partner_code)+"' and RIGHT(a.partner_code,5) = b.partner_code and telco like '%"+str(telco)+"%'"+sql+" GROUP BY a.partner_code ,LEFT(a.createdtime,10),a.isBrand"
                    else:
                        sql_partner_code = "SELECT partner_code from partner WHERE agency_id= 1"
                        df_partnercode = pd.read_sql(sql_partner_code,db)
                        # lưu tất cả partner1 , partner2 , partner3 vào 1 list
                        list_partner = df_partnercode['partner_code'].tolist()
                        logging.info("list_partner : "+ str(list_partner))
                        list_partner = str(list_partner).replace("[","'").replace("]","'").replace(", ","|")
                        sqlfind = "SELECT a.id,LEFT(a.createdtime,10)'createdtime',(SUM(a.callsuccess)+SUM(a.callmiss))'calltotal',SUM(a.callsuccess)'callsuccess',SUM(a.callmiss) 'callmiss',ROUND(SUM(a.voicetime)/60,0)'voicetime',SUM(a.revenue)'revenue',a.vosIp,b.nickname,a.telco,CONCAT(ROUND((SUM(a.callmiss)/(SUM(a.callsuccess)+SUM(a.callmiss)))*100,2),' %')'callrate',case when a.isBrand = '0' then 'BRAND' else 'SIP_TRUNK' end as servicename FROM report a,partner b WHERE a.partner_code REGEXP "+str(list_partner)+" and RIGHT(a.partner_code,5) = b.partner_code and telco like '%"+str(telco)+"%'"+sql+" GROUP BY a.partner_code ,LEFT(a.createdtime,10),a.isBrand"
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
                    if str(group_name) == "CUSTOMER":
                        sqltotal = "SELECT a.id,LEFT(a.createdtime,10)'createdtime',(SUM(a.callsuccess)+SUM(a.callmiss))'calltotal',SUM(a.callsuccess)'callsuccess',SUM(a.callmiss) 'callmiss',ROUND(SUM(a.voicetime)/60,0)'voicetime',SUM(a.revenue)'revenue',ROUND((SUM(a.callmiss)/(SUM(a.callsuccess)+SUM(a.callmiss)))*100,2)'callrate' FROM report a WHERE partner_code = '"+str(partner_code)+"' and telco like '%"+str(telco)+"%'"+sql
                    else :
                        sqltotal = "SELECT a.id,LEFT(a.createdtime,10)'createdtime',(SUM(a.callsuccess)+SUM(a.callmiss))'calltotal',SUM(a.callsuccess)'callsuccess',SUM(a.callmiss) 'callmiss',ROUND(SUM(a.voicetime)/60,0)'voicetime',SUM(a.revenue)'revenue',ROUND((SUM(a.callmiss)/(SUM(a.callsuccess)+SUM(a.callmiss)))*100,2)'callrate' FROM report a WHERE a.partner_code REGEXP "+str(list_partner)+" and telco like '%"+str(telco)+"%'"+sql
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
@report_exception.route('/totalreport_exceptiondate', methods=['GET'])
def totalreport_exceptiondate():
    now = datetime.datetime.now()
    session = str(now).replace("-","").replace(":","").replace(".","").replace(" ","")
    logging.info(session +"----------------totalreport_exceptiondate-----------------------")
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
            partner = data["partner"]
            partner_code = data["partner_code"]
            logging.info(session +" | data | "+str(data))
            logging.info(session +" | partner_code | "+str(partner_code))
            logging.info(session +"| partner | "+str(partner))
            if str(partner) != "":
                partner= str(partner).replace("[","").replace("]","").replace("'","")
                partner =re.split('[,;/ ]+', partner)
                logging.info("partner : "+ str(partner))
                nickname=[]
                for i in partner:
                    sqlpartnerid = "select partner_code from partner where nickname ='"+str(i)+"'"
                    dfpartnerid = pd.read_sql(sqlpartnerid,db)
                    if not dfpartnerid.empty:
                        nickname.append(str(dfpartnerid.iloc[0,0]))
                logging.info(session+"| nickname | "+str(nickname))
            if True:
                startdate = request.args.get('startdate')
                print(startdate)
                enddate = request.args.get('enddate')
                print(enddate)
                enddate =str(enddate) + " 23:59:59"
                if str(startdate) == "None" :
                    # quyền admin
                    if str(partner) == "" and str(partner_code) == "None" :
                        sql = "SELECT a.id,LEFT(a.createdtime,10)'date',(SUM(a.callsuccess)+SUM(a.callmiss))'calltotal',SUM(a.callsuccess)'callsuccess',SUM(a.callmiss) 'callmiss',ROUND(SUM(a.voicetime)/60,0)'voicetime',SUM(a.revenue)'revenue',a.vosIp,a.telco,CONCAT(ROUND((SUM(a.callmiss)/(SUM(a.callsuccess)+SUM(a.callmiss)))*100,2),' %')'callrate','SIP_TRUNK'`servicename` FROM report a WHERE a.createdtime >= DATE_SUB(NOW(), INTERVAL 31 day) GROUP BY date"
                        logging.info(session +" | sql | "+str(sql))
                        df = pd.read_sql(sql,db)
                        df["date"] = pd.to_datetime(df["date"])
                        df = df.sort_values(by=['date'], ascending=False)
                        df['date'] = df['date'].astype('str')
                        json_records = df.to_json(orient ='records')
                        data = []
                        data = json.loads(json_records)
                        sql = "SELECT a.id,(SUM(a.callsuccess)+SUM(a.callmiss))'calltotal',SUM(a.callsuccess)'callsuccess',SUM(a.callmiss) 'callmiss',ROUND(SUM(a.voicetime)/60,0)'voicetime',SUM(a.revenue)'revenue',CONCAT(ROUND((SUM(a.callmiss)/(SUM(a.callsuccess)+SUM(a.callmiss)))*100,2),' %')'callrate' FROM report a WHERE a.createdtime >= DATE_SUB(NOW(), INTERVAL 31 day)"
                        logging.info(session +" | sql_total | "+str(sql))
                        df = pd.read_sql(sql,db)
                        json_records = df.to_json(orient ='records')
                        data1 = []
                        data1 = json.loads(json_records)
                    elif str(partner) != "" and str(partner_code) == "None" :
                        sql=""
                        for i in nickname:
                            sql_join= "(SELECT a.id,LEFT(a.createdtime,10)'date',(SUM(a.callsuccess)+SUM(a.callmiss))'calltotal',SUM(a.callsuccess)'callsuccess',SUM(a.callmiss) 'callmiss',ROUND(SUM(a.voicetime)/60,0)'voicetime',SUM(a.revenue)'revenue',a.vosIp,b.nickname,a.telco,CONCAT(ROUND((SUM(a.callmiss)/(SUM(a.callsuccess)+SUM(a.callmiss)))*100,2),' %')'callrate','SIP_TRUNK'`servicename` FROM report a,partner b WHERE a.createdtime >= DATE_SUB(NOW(), INTERVAL 31 day) AND a.partner_code like '%"+str(i)+"%' AND RIGHT(a.partner_code,5) = b.partner_code GROUP BY date) union all"
                            sql = sql + sql_join
                        sql = sql[:-9]
                        logging.info(session +" | sql | "+str(sql))
                        df = pd.read_sql(sql,db)
                        json_records = df.to_json(orient ='records')
                        data = []
                        data = json.loads(json_records)
                        sql="select id, sum(calltotal)'calltotal',sum(callsuccess)'callsuccess',sum(callmiss)'callmiss',sum(voicetime),sum(revenue)'revenue',CONCAT(ROUND((SUM(callmiss)/(SUM(callsuccess)+SUM(callmiss)))*100,2),' %')'callrate' ,sum(voicetime)'voicetime' from ("
                        for i in nickname:
                            sql_join= "(SELECT a.id,LEFT(a.createdtime,10)'createdtime',(SUM(a.callsuccess)+SUM(a.callmiss))'calltotal',SUM(a.callsuccess)'callsuccess',SUM(a.callmiss) 'callmiss',ROUND(SUM(a.voicetime)/60,0)'voicetime',SUM(a.revenue)'revenue',a.vosIp,b.nickname,a.telco,CONCAT(ROUND((SUM(a.callmiss)/(SUM(a.callsuccess)+SUM(a.callmiss)))*100,2),' %')'callrate','SIP_TRUNK'`servicename` FROM report a,partner b WHERE a.createdtime >= DATE_SUB(NOW(), INTERVAL 31 day) AND a.partner_code like '%"+str(i)+"%' AND RIGHT(a.partner_code,5) = b.partner_code GROUP BY createdtime) union all "
                            sql = sql + sql_join
                        sql = sql[:-10] + ") AS e"
                        logging.info(session +" | sql_total | "+str(sql))
                        df = pd.read_sql(sql,db)
                        json_records = df.to_json(orient ='records')
                        data1 = []
                        data1 = json.loads(json_records)
                    else :
                        sql = "SELECT a.id,LEFT(a.createdtime,10)'date',(SUM(a.callsuccess)+SUM(a.callmiss))'calltotal',SUM(a.callsuccess)'callsuccess',SUM(a.callmiss) 'callmiss',ROUND(SUM(a.voicetime)/60,0)'voicetime',SUM(a.revenue)'revenue',a.vosIp,a.telco,CONCAT(ROUND((SUM(a.callmiss)/(SUM(a.callsuccess)+SUM(a.callmiss)))*100,2),' %')'callrate','SIP_TRUNK'`servicename` FROM report a WHERE a.partner_code like '%"+str(partner_code)+"%' and a.createdtime >= DATE_SUB(NOW(), INTERVAL 31 day) GROUP BY date"
                        df = pd.read_sql(sql,db)
                        df["date"] = pd.to_datetime(df["date"])
                        df = df.sort_values(by=['date'], ascending=False)
                        df['date'] = df['date'].astype('str')
                        json_records = df.to_json(orient ='records')
                        data = []
                        data = json.loads(json_records)
                        data1 ="data1"
                    context = {'data': data,'total': data1,'code': 'OK'}
                    db.close()
                    return context
                else :
                    if str(partner) == "" and str(partner_code) == "None" :
                        sql = "SELECT a.id,LEFT(a.createdtime,10)'date',(SUM(a.callsuccess)+SUM(a.callmiss))'calltotal',SUM(a.callsuccess)'callsuccess',SUM(a.callmiss) 'callmiss',ROUND(SUM(a.voicetime)/60,0)'voicetime',SUM(a.revenue)'revenue',a.vosIp,a.telco,CONCAT(ROUND((SUM(a.callmiss)/(SUM(a.callsuccess)+SUM(a.callmiss)))*100,2),' %')'callrate','SIP_TRUNK'`servicename` FROM report a WHERE a.createdtime >= '"+str(startdate)+"' and a.createdtime <= '"+str(enddate)+"' GROUP BY date"
                        df = pd.read_sql(sql,db)
                        json_records = df.to_json(orient ='records')
                        data = []
                        data = json.loads(json_records)
                        sql = "SELECT a.id,(SUM(a.callsuccess)+SUM(a.callmiss))'calltotal',SUM(a.callsuccess)'callsuccess',SUM(a.callmiss) 'callmiss',ROUND(SUM(a.voicetime)/60,0)'voicetime',SUM(a.revenue)'revenue',ROUND((SUM(a.callmiss)/(SUM(a.callsuccess)+SUM(a.callmiss)))*100,2)'callrate' FROM report a WHERE a.createdtime >= '"+str(startdate)+"' and a.createdtime <= '"+str(enddate)+"'"
                        df = pd.read_sql(sql,db)
                        json_records = df.to_json(orient ='records')
                        data1 = []
                        data1 = json.loads(json_records)
                    elif str(partner) != "" and str(partner_code) == "None" :
                        sql=""
                        for i in nickname:
                            sql_join= "(SELECT a.id,LEFT(a.createdtime,10)'date',(SUM(a.callsuccess)+SUM(a.callmiss))'calltotal',SUM(a.callsuccess)'callsuccess',SUM(a.callmiss) 'callmiss',ROUND(SUM(a.voicetime)/60,0)'voicetime',SUM(a.revenue)'revenue',a.vosIp,b.nickname,a.telco,CONCAT(ROUND((SUM(a.callmiss)/(SUM(a.callsuccess)+SUM(a.callmiss)))*100,2),' %')'callrate','SIP_TRUNK'`servicename` FROM report a,partner b WHERE a.createdtime >= CURDATE() and a.createdtime >= '"+str(startdate)+"' and a.createdtime <= '"+str(enddate)+"' AND a.partner_code like '%"+str(i)+"%' AND RIGHT(a.partner_code,5) = b.partner_code GROUP BY a.partner_code) union all"
                            sql = sql + sql_join
                        sql = sql[:-9]
                        logging.info(sql)
                        df = pd.read_sql(sql,db)
                        json_records = df.to_json(orient ='records')
                        data = []
                        data = json.loads(json_records)
                        sql="select id, sum(calltotal)'calltotal',sum(callsuccess)'callsuccess',sum(callmiss)'callmiss',sum(voicetime),sum(revenue)'revenue',CONCAT(ROUND((SUM(callmiss)/(SUM(callsuccess)+SUM(callmiss)))*100,2),' %')'callrate' ,sum(voicetime)'voicetime' from ("
                        for i in nickname:
                            sql_join= "(SELECT a.id,LEFT(a.createdtime,10)'createdtime',(SUM(a.callsuccess)+SUM(a.callmiss))'calltotal',SUM(a.callsuccess)'callsuccess',SUM(a.callmiss) 'callmiss',ROUND(SUM(a.voicetime)/60,0)'voicetime',SUM(a.revenue)'revenue',a.vosIp,b.nickname,a.telco,CONCAT(ROUND((SUM(a.callmiss)/(SUM(a.callsuccess)+SUM(a.callmiss)))*100,2),' %')'callrate','SIP_TRUNK'`servicename` FROM report a,partner b WHERE a.createdtime >= CURDATE() and a.createdtime >= '"+str(startdate)+"' and a.createdtime <= '"+str(enddate)+"' AND a.partner_code like '%"+str(i)+"%' AND RIGHT(a.partner_code,5) = b.partner_code GROUP BY a.partner_code) union all "
                            sql = sql + sql_join
                        sql = sql[:-10] + ") AS e"
                        logging.info(sql)
                        df = pd.read_sql(sql,db)
                        json_records = df.to_json(orient ='records')
                        data1 = []
                        data1 = json.loads(json_records)
                    else :
                        sql = "SELECT a.id,LEFT(a.createdtime,10)'date',(SUM(a.callsuccess)+SUM(a.callmiss))'calltotal',SUM(a.callsuccess)'callsuccess',SUM(a.callmiss) 'callmiss',ROUND(SUM(a.voicetime)/60,0)'voicetime',SUM(a.revenue)'revenue',a.vosIp,a.telco,CONCAT(ROUND((SUM(a.callmiss)/(SUM(a.callsuccess)+SUM(a.callmiss)))*100,2),' %')'callrate','SIP_TRUNK'`servicename` FROM report a WHERE a.partner_code like '%"+str(partner_code)+"%' and a.createdtime >= DATE_SUB(NOW(), INTERVAL 31 day) GROUP BY date"
                        df = pd.read_sql(sql,db)
                        df["date"] = pd.to_datetime(df["date"])
                        df = df.sort_values(by=['date'], ascending=False)
                        df['date'] = df['date'].astype('str')
                        json_records = df.to_json(orient ='records')
                        data = []
                        data = json.loads(json_records)
                        data1 ="data1"
                    context = {'data': data,'total': data1,'code': 'OK'}
                    db.close()
                    return context
    except Exception as e:
        logging.error(traceback.format_exc())
        return jsonify({'NOTOK': str(e)})
@report_exception.route('/saovang_top_revenue', methods=['GET'])
def saovang_top_revenue():
    try :
        now = datetime.datetime.now()
        session = str(now).replace(" ","").replace(":","").replace("-","").replace(".","")
        logging.info(session + "------------------------top_revenue----------------------------")
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
            logging.info(session + " | partner | " + str(partner))
            if str(partner) != "":
                partner= str(partner).replace("[","").replace("]","").replace("'","")
                partner =re.split('[,;/ ]+', partner)
                print("---------")
                nickname=""
                for i in partner:
                    print(i)
                    sqlpartnerid = "select id from partner where nickname ='"+str(i)+"'"
                    dfpartnerid = pd.read_sql(sqlpartnerid,db)
                    nickname=nickname+",'"+str(dfpartnerid.iloc[0,0])+"'"
                nickname=nickname[1:len(nickname)]
                logging.info(session + " | nickname | "+ str(nickname))
            if True :
                currentDate = time.strftime("%Y-%m-%d %H:%M:%S")
                print(currentDate)
                if str(timetoken) >= str(currentDate) :
                    sql_partner_code = "SELECT partner_code from partner WHERE agency_id= 1"
                    df_partnercode = pd.read_sql(sql_partner_code,db)
                    # lưu tất cả partner1 , partner2 , partner3 vào 1 list
                    list_partner = df_partnercode['partner_code'].tolist()
                    logging.info("list_partner : "+ str(list_partner))
                    list_partner = str(list_partner).replace("[","'").replace("]","'").replace(", ","|")
                    sql = "SELECT b.nickname,SUM(a.callsuccess)'calltotal',ROUND(SUM(a.voicetime)/60,0)'voicetime',SUM(a.revenue)'revenue' FROM report a,partner b WHERE a.partner_code REGEXP "+list_partner+" and a.partner01 = b.partner_code and a.createdtime >= DATE_SUB(NOW(), INTERVAL 7 day) group by a.partner01 order by revenue desc limit 10"
                    logging.info(session + " | sql | "+ str(sql))
                    df = pd.read_sql(sql,db)
                    #cộng 1 vào giá trị index trong dataframe df ở mỗi dòng
                    df.index = df.index + 1
                    #đổi tên cột index thành id
                    df = df.rename_axis('id').reset_index()
                    json_records = df.to_json(orient ='records')
                    data = []
                    data = json.loads(json_records)
                    context = {'data': data,'code': 'OK'}
                    db.close()
                    return context
                else :
                    return jsonify({'NOTOK': 'token hết hạn','code':401}),401
            else :
                context = {'data': '','code': 'OK'}
                db.close()
                return context
    except Exception as e:
        logging.error(traceback.format_exc())
        return jsonify({'NOTOK': str(e)})
@report_exception.route('/download_report_exception', methods=['POST'])
def download_report_exception():
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
            partner = data["partner"]
            partner_code1 = data["partner_code"]
            if True:
                currentDate = time.strftime("%Y-%m-%d %H:%M:%S")
                print(currentDate)
                if str(timetoken) >= str(currentDate) :
                    record = json.loads(request.data)
                    starttime = record["starttime"]
                    endtime = record["endtime"]
                    endtime = str(endtime) + " 23:59:59"
                    nickname = record["selectedPartner"]
                    telco = record["selectedAccount"]
                    if len(starttime) == 0 :
                        starttime = str(currentDate)[0:10]
                    timestartquery = " and a.createdtime >= '"+ str(starttime) +"'"
                    if len(endtime) == 0 :
                        endtime = str(currentDate)[0:10]+" 23:59:59"
                    timeendquery = " and a.createdtime <= '"+ str(endtime) +"'"
                    if len(nickname) == 0 :
                        partnerquery = ""
                    else :
                        sql_get_partner_code = "select partner_code from partner where nickname ='"+str(nickname)+"'"
                        df_get_partner_code = pd.read_sql(sql_get_partner_code,db)
                        partner_code = df_get_partner_code.iloc[0,0]
                        partnerquery = " and a.partner_code like '%"+str(partner_code)+"%'"
                    sql = timestartquery + timeendquery + partnerquery
                    if len(str(nickname)) == 0:
                        if str(group_name) == "CUSTOMER" :
                            sqlfind = "SELECT a.id,LEFT(a.createdtime,10)'createdtime',(SUM(a.callsuccess)+SUM(a.callmiss))'calltotal',SUM(a.callsuccess)'callsuccess',SUM(a.callmiss) 'callmiss',ROUND(SUM(a.voicetime)/60,0)'voicetime',SUM(a.revenue)'revenue',a.vosIp,b.nickname,a.telco,CONCAT(ROUND((SUM(a.callmiss)/(SUM(a.callsuccess)+SUM(a.callmiss)))*100,2),' %')'callrate','SIP_TRUNK'`servicename` FROM report a,partner b WHERE a.partner_code like '%"+str(partner_code1)+"%' and RIGHT(a.partner_code,5) = b.partner_code and telco like '%"+str(telco)+"%'"+sql+" GROUP BY a.partner_code ,LEFT(a.createdtime,10)"
                        else :
                            sqlfind = "SELECT a.id,LEFT(a.createdtime,10)'createdtime',(SUM(a.callsuccess)+SUM(a.callmiss))'calltotal',SUM(a.callsuccess)'callsuccess',SUM(a.callmiss) 'callmiss',ROUND(SUM(a.voicetime)/60,0)'voicetime',SUM(a.revenue)'revenue',a.vosIp,b.nickname,a.telco,CONCAT(ROUND((SUM(a.callmiss)/(SUM(a.callsuccess)+SUM(a.callmiss)))*100,2),' %')'callrate','SIP_TRUNK'`servicename` FROM report a,partner b WHERE RIGHT(a.partner_code,5) = b.partner_code and telco like '%"+str(telco)+"%'"+sql+" GROUP BY a.partner_code ,LEFT(a.createdtime,10)"
                    else :
                        sqlfind = "SELECT a.id,LEFT(a.createdtime,10)'createdtime',(SUM(a.callsuccess)+SUM(a.callmiss))'calltotal',SUM(a.callsuccess)'callsuccess',SUM(a.callmiss) 'callmiss',ROUND(SUM(a.voicetime)/60,0)'voicetime',SUM(a.revenue)'revenue',a.vosIp,'"+str(nickname)+"'`nickname`,a.telco,CONCAT(ROUND((SUM(a.callmiss)/(SUM(a.callsuccess)+SUM(a.callmiss)))*100,2),' %')'callrate','SIP_TRUNK'`servicename` FROM report a WHERE a.telco like '%"+str(telco)+"%'"+sql+" GROUP BY LEFT(a.createdtime,10)"
                    df = pd.read_sql(sqlfind,db)
                    df["createdtime"] = pd.to_datetime(df["createdtime"])
                    df = df.sort_values(by=['createdtime'], ascending=False)
                    df['createdtime'] = df['createdtime'].astype('str')
                    session=datetime.datetime.now()
                    filename = "report_exception"+str(session)+".csv"
                    path = os.path.join(os.getcwd(),'filedownload',filename)
                    df.to_csv(path,encoding='utf-8-sig')
                    db.close()
                    return send_file(path, as_attachment=True)
                else :
                    return jsonify({'NOTOK': 'token hết hạn','code':401}),401
            else :
                return jsonify({'NOTOK': 'khong co quyen'})
    except Exception as e:
        logging.error(traceback.format_exc())
        return jsonify({'NOTOK': 'khong co quyen'})

@report_exception.route('/findreport_exception_customer', methods=['GET'])
def findreport_exception_customer():
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
            partner = data["partner"]
            partner_code1 = data["partner_code"]
            if True:
                currentDate = time.strftime("%Y-%m-%d %H:%M:%S")
                print(currentDate)
                if str(timetoken) >= str(currentDate) :
                    starttime = request.args.get('starttime')
                    endtime = request.args.get('endtime')
                    if str(endtime) != "":
                        endtime = str(endtime) + " 23:59:59"
                    nickname = request.args.get('nickname')
                    telco = request.args.get('telco')
                    if len(starttime) == 0 :
                        starttime = str(currentDate)[0:10]
                    timestartquery = " and a.createdtime >= '"+ str(starttime) +"'"
                    if len(endtime) == 0 :
                        endtime = str(currentDate)[0:10]+" 23:59:59"
                    timeendquery = " and a.createdtime <= '"+ str(endtime) +"'"
                    if len(nickname) == 0 :
                        partnerquery = ""
                    else :
                        sql_get_partner_code = "select partner_code from partner where nickname ='"+str(nickname)+"'"
                        df_get_partner_code = pd.read_sql(sql_get_partner_code,db)
                        partner_code = df_get_partner_code.iloc[0,0]
                        partnerquery = " and a.partner_code like '%"+str(partner_code)+"%'"
                    sql = timestartquery + timeendquery + partnerquery
                    if len(str(nickname)) == 0:
                        if str(group_name) == "CUSTOMER" :
                            sqlfind = "SELECT a.id,LEFT(a.createdtime,10)'createdtime',(SUM(a.callsuccess)+SUM(a.callmiss))'calltotal',SUM(a.callsuccess)'callsuccess',SUM(a.callmiss) 'callmiss',ROUND(SUM(a.voicetime)/60,0)'voicetime',SUM(a.revenue)'revenue',a.vosIp,b.nickname,a.telco,CONCAT(ROUND((SUM(a.callmiss)/(SUM(a.callsuccess)+SUM(a.callmiss)))*100,2),' %')'callrate','SIP_TRUNK'`servicename` FROM report a,partner b WHERE a.partner_code like '%"+str(partner_code1)+"%' and RIGHT(a.partner_code,5) = b.partner_code and telco like '%"+str(telco)+"%'"+sql+" GROUP BY a.partner_code ,LEFT(a.createdtime,10)"
                        else :
                            sqlfind = "SELECT a.id,LEFT(a.createdtime,10)'createdtime',(SUM(a.callsuccess)+SUM(a.callmiss))'calltotal',SUM(a.callsuccess)'callsuccess',SUM(a.callmiss) 'callmiss',ROUND(SUM(a.voicetime)/60,0)'voicetime',SUM(a.revenue)'revenue',a.vosIp,b.nickname,a.telco,CONCAT(ROUND((SUM(a.callmiss)/(SUM(a.callsuccess)+SUM(a.callmiss)))*100,2),' %')'callrate','SIP_TRUNK'`servicename` FROM report a,partner b WHERE RIGHT(a.partner_code,5) = b.partner_code and telco like '%"+str(telco)+"%'"+sql+" GROUP BY a.partner_code ,LEFT(a.createdtime,10)"
                    else :
                        sqlfind = "SELECT a.id,LEFT(a.createdtime,10)'createdtime',(SUM(a.callsuccess)+SUM(a.callmiss))'calltotal',SUM(a.callsuccess)'callsuccess',SUM(a.callmiss) 'callmiss',ROUND(SUM(a.voicetime)/60,0)'voicetime',SUM(a.revenue)'revenue',a.vosIp,'"+str(nickname)+"'`nickname`,a.telco,CONCAT(ROUND((SUM(a.callmiss)/(SUM(a.callsuccess)+SUM(a.callmiss)))*100,2),' %')'callrate','SIP_TRUNK'`servicename` FROM report a WHERE a.telco like '%"+str(telco)+"%'"+sql+" GROUP BY LEFT(a.createdtime,10)"
                    df = pd.read_sql(sqlfind,db)
                    df["createdtime"] = pd.to_datetime(df["createdtime"])
                    df = df.sort_values(by=['createdtime'], ascending=False)
                    df['createdtime'] = df['createdtime'].astype('str')
                    logging.info(sqlfind)
                    json_records = df.to_json(orient ='records')
                    data = []
                    data = json.loads(json_records)
                    if str(group_name) == "CUSTOMER" :
                        sqltotal = "SELECT a.id,LEFT(a.createdtime,10)'createdtime',(SUM(a.callsuccess)+SUM(a.callmiss))'calltotal',SUM(a.callsuccess)'callsuccess',SUM(a.callmiss) 'callmiss',ROUND(SUM(a.voicetime)/60,0)'voicetime',SUM(a.revenue)'revenue',ROUND((SUM(a.callmiss)/(SUM(a.callsuccess)+SUM(a.callmiss)))*100,2)'callrate' FROM report a WHERE a.partner_code like '%"+str(partner_code1)+"%' and a.telco like '%"+str(telco)+"%'"+sql
                    else :
                        sqltotal = "SELECT a.id,LEFT(a.createdtime,10)'createdtime',(SUM(a.callsuccess)+SUM(a.callmiss))'calltotal',SUM(a.callsuccess)'callsuccess',SUM(a.callmiss) 'callmiss',ROUND(SUM(a.voicetime)/60,0)'voicetime',SUM(a.revenue)'revenue',ROUND((SUM(a.callmiss)/(SUM(a.callsuccess)+SUM(a.callmiss)))*100,2)'callrate' FROM report a WHERE telco like '%"+str(telco)+"%'"+sql
                    print(sqltotal)
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
        return jsonify({'NOTOK': 'khong co quyen'})
@report_exception.route('/saovang_get_partner2', methods=['GET'])
def saovang_get_partner2():
    try:
        db = pymysql.connect(host="172.17.0.1",    # your host, usually localhost
                        user="leeoncrm",         # your username
                        passwd="41XmKsO3NBgHPwv",  # your password
                        db="leeon_crm",
                        port=3306
                        )
        nickname = request.args.get('nickname')
        logging.info(nickname)
        #get partner1
        sql = "select partner_code from  partner where nickname ='"+str(nickname)+"'"
        logging.info(sql)
        df = pd.read_sql(sql,db)
        if df.empty:
            return jsonify({'OK': 'Không có nickname này'})
        partner01 = df.iloc[0]['partner_code']
        logging.info("partner01 :" + str(partner01))
        sql = "SELECT nickname'nickname2' FROM partner WHERE partner_code IN (select partner2 from  partner_group where partner1 ='"+str(partner01)+"')"
        df = pd.read_sql(sql,db)
        json_records = df.to_json(orient ='records')
        data = []
        data = json.loads(json_records)
        db.close()
        return jsonify({'data': data,'code':200})
    except Exception as e:
        logging.error(traceback.format_exc())
        return jsonify({'NOTOK': 'NOTOK'}),400
@report_exception.route('/saovang_get_partner3', methods=['GET'])
def saovang_get_partner3():
    try:
        db = pymysql.connect(host="172.17.0.1",    # your host, usually localhost
                        user="leeoncrm",         # your username
                        passwd="41XmKsO3NBgHPwv",  # your password
                        db="leeon_crm",
                        port=3306
                        )
        nickname = request.args.get('nickname')
        logging.info(nickname)
        #get partner1
        sql = "select partner_code from  partner where nickname ='"+str(nickname)+"'"
        df = pd.read_sql(sql,db)
        if df.empty:
            return jsonify({'OK': 'Không có nickname này'})
        partner01 = df.iloc[0]['partner_code']
        sql = "SELECT nickname'nickname3' FROM partner WHERE partner_code IN (select partner3 from partner_group where partner2 ='"+str(partner01)+"')"
        df = pd.read_sql(sql,db)
        json_records = df.to_json(orient ='records')
        data = []
        data = json.loads(json_records)
        db.close()
        return jsonify({'data': data,'code':200})
    except Exception as e:
        return jsonify({'NOTOK': 'NOTOK'}),401
@report_exception.route('/report_exception_detail', methods=['GET'])
def report_exception_detail():
    logging.info("----------------------report_exception_detail-------------------------")
    try:
        db = pymysql.connect(host="127.0.0.1",    # your host, usually localhost
                     user="leeonbilling",         # your username
                     passwd="sT1iYy7ApRLOW9G09384",  # your password
                     db="leeon_billing",
                     port=3306
                     )
        # lấy thời gian hiện tại
        data = group()
        partner_code= data["partner_code"]
        row_page = 50
        offset = request.args.get('offset')
        logging.info("offset : "+ str(offset))
        now = datetime.datetime.now()
        # lấy 1 ngày sau
        # tomorrow = now + datetime.timedelta(days=1)
        partition = "p"+str(now)[0:10].replace("-","")
        table = "cdr_billing_"+str(now)[0:7].replace("-","")
        logging.info("table : "+ str(table))
        if str(partner_code) == 'None':
            sql_total = "select count(1) from "+table+" PARTITION("+str(partition)+") where startTime >= CURDATE()"
        else :
            sql_total = "select count(1) from "+table+" PARTITION("+str(partition)+") where startTime >= CURDATE() and partner01 = '"+str(partner_code)+"'"
        df_total = pd.read_sql(sql_total,db)
        total = df_total.iloc[0,0]
        logging.info("total : " +str(total))
        total_page = math.ceil(total/row_page)
        logging.info("total_page : " +str(total_page))
        if str(offset) == "None" or str(offset) == "0":
            if str(partner_code) == 'None':
                sql = "SELECT a.id,a.caller,b.telcoName'callerTelcoId',a.callee,c.telcoName'calleeTelcoid',a.callerRtpIp,a.startTime,a.stopTime,a.billTime,a.billAmount,a.callercallid,a.brandName,a.cdrId,CASE WHEN a.isMnp = 1 THEN 'True' ELSE 'False' END AS isMnp,d.callType,CASE WHEN a.isBrand =1 THEN 'Sip' ELSE 'Brandname' END AS isBrand ,Case when a.endReason = '-8' then 'CalleeHangup' when a.endReason = '-7' then 'CallerHangup' else 'other' end as endReason ,a.partnerTag FROM "+table+" PARTITION("+str(partition)+") a ,service_prefix b ,service_prefix c,service_type d WHERE a.startTime >= CURDATE() and a.callerTelcoId = b.id AND a.calleeTelcoid = c.id AND a.callType =d.id ORDER BY a.id DESC LIMIT "+str(row_page)
            else :
                sql = "SELECT a.id,a.caller,b.telcoName'callerTelcoId',a.callee,c.telcoName'calleeTelcoid',a.callerRtpIp,a.startTime,a.stopTime,a.billTime,a.billAmount,a.callercallid,a.brandName,a.cdrId,CASE WHEN a.isMnp = 1 THEN 'True' ELSE 'False' END AS isMnp,d.callType,CASE WHEN a.isBrand =1 THEN 'Sip' ELSE 'Brandname' END AS isBrand ,Case when a.endReason = '-8' then 'CalleeHangup' when a.endReason = '-7' then 'CallerHangup' else 'other' end as endReason ,a.partnerTag FROM "+table+" PARTITION("+str(partition)+") a ,service_prefix b ,service_prefix c,service_type d WHERE a.startTime >= CURDATE() and a.partner01 = '"+str(partner_code)+"' and a.callerTelcoId = b.id AND a.calleeTelcoid = c.id AND a.callType =d.id ORDER BY a.id DESC LIMIT "+str(row_page)
            df = pd.read_sql(sql,db)
            df['startTime'] = df['startTime'].astype('str')
            df['stopTime'] = df['stopTime'].astype('str')
            json_records = df.to_json(orient ='records')
            data = []
            data = json.loads(json_records)
            context = {'data': data,'total_page': total_page,'current_page' : '1','code': 'OK','limit': row_page}
            db.close()
            return context
        limit_offset = int(offset)*int(row_page)-int(row_page)
        logging.info("limit_offset : " +str(limit_offset))
        if str(partner_code) == 'None':
            sql ="SELECT a.id,a.caller,b.telcoName'callerTelcoId',a.callee,c.telcoName'calleeTelcoid',a.callerRtpIp,a.startTime,a.stopTime,a.billTime,a.billAmount,a.callercallid,a.brandName,a.cdrId,CASE WHEN a.isMnp = 1 THEN 'True' ELSE 'False' END AS isMnp,d.callType,CASE WHEN a.isBrand =1 THEN 'Sip' ELSE 'Brandname' END AS isBrand ,Case when a.endReason = '-8' then 'CalleeHangup' when a.endReason = '-7' then 'CallerHangup' else 'other' end as endReason,a.partnerTag FROM "+table+" PARTITION("+str(partition)+") a,service_prefix b ,service_prefix c,service_type d WHERE a.startTime >= CURDATE() and a.callerTelcoId = b.id AND a.calleeTelcoid = c.id AND a.callType =d.id ORDER BY a.id DESC limit "+str(row_page) +" offset "+str(limit_offset)
        else :
            sql ="SELECT a.id,a.caller,b.telcoName'callerTelcoId',a.callee,c.telcoName'calleeTelcoid',a.callerRtpIp,a.startTime,a.stopTime,a.billTime,a.billAmount,a.callercallid,a.brandName,a.cdrId,CASE WHEN a.isMnp = 1 THEN 'True' ELSE 'False' END AS isMnp,d.callType,CASE WHEN a.isBrand =1 THEN 'Sip' ELSE 'Brandname' END AS isBrand ,Case when a.endReason = '-8' then 'CalleeHangup' when a.endReason = '-7' then 'CallerHangup' else 'other' end as endReason,a.partnerTag FROM "+table+" PARTITION("+str(partition)+") a,service_prefix b ,service_prefix c,service_type d WHERE a.startTime >= CURDATE() and a.partner01 = '"+str(partner_code)+"' and a.callerTelcoId = b.id AND a.calleeTelcoid = c.id AND a.callType =d.id ORDER BY a.id DESC limit "+str(row_page) +" offset "+str(limit_offset)
        logging.info(sql)
        df = pd.read_sql(sql,db)
        df['startTime'] = df['startTime'].astype('str')
        df['stopTime'] = df['stopTime'].astype('str')
        json_records = df.to_json(orient ='records')
        data = []
        data = json.loads(json_records)
        context = {'data': data,'total_page': total_page,'current_page' : str(offset),'code': 'OK','limit': row_page}
        db.close()
        return context
    except Exception as e:
        logging.error(traceback.format_exc())
        return jsonify({'NOTOK': 'NOTOK'}),400

@report_exception.route('/find_report_exception_detail', methods=['GET'])
def find_report_exception_detail():
    logging.info("----------------------find_report_exception_detail-------------------------")
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
                    nickname = request.args.get('nickname')
                    if str(nickname) == "" :
                        nicknamequery = ""
                    else :
                        sql_partnercode = "SELECT partner_code FROM partner where nickname ='"+str(nickname)+"'"
                        df_partnercode = pd.read_sql(sql_partnercode,db1)
                        if df_partnercode.empty :
                            db1.close()
                            return jsonify({'NOTOK': 'Không có tên đối tác này'}),400
                        partner_code =df_partnercode.iloc[0,0]
                        logging.info ("partner_code : "+ str(partner_code))
                        nicknamequery = "(partner01 = "+ str(partner_code) +" or partner02 = "+ str(partner_code)+" or partner03 = "+ str(partner_code) +")"
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
                        if str(group_name) =="CUSTOMER":
                            # sql= "SELECT COUNT(1)'count1' FROM "+str(table)+" PARTITION("+str(partition)+") WHERE (partner01 = '"+str(partner_code)+"' or partner02 = '"+str(partner_code)+"' or partner03 ='"+str(partner_code)+"') and id is not null"+ str(nicknamequery) + " union all "
                            sql= "SELECT COUNT(1)'count1' FROM "+str(table)+" PARTITION("+str(partition)+") WHERE "+ str(nicknamequery) + " union all "
                            sql_total =sql_total+sql
                        else :
                            sql= "SELECT COUNT(1)'count1' FROM "+str(table)+" PARTITION("+str(partition)+") WHERE "+ str(nicknamequery) + " union all "
                            sql_total =sql_total+sql
                        #query find detail
                        if str(group_name) =="CUSTOMER":
                            # sql_find = "SELECT a.id,a.caller,b.telcoName'callerTelcoId',a.callee,c.telcoName'calleeTelcoid',a.callerRtpIp,a.startTime,a.stopTime,a.billTime,a.billAmount,a.callercallid'callercallid',a.brandName,a.cdrId,CASE WHEN a.isMnp = 1 THEN 'True' ELSE 'False' END AS isMnp,d.callType,CASE WHEN a.isBrand =1 THEN 'Sip' ELSE 'Brandname' END AS isBrand ,CASE WHEN a.endReason = '-8' THEN 'CalleeHangup' WHEN a.endReason = '-7' THEN 'CallerHangup' ELSE 'other' END AS endReason,a.partnerTag FROM (SELECT * FROM "+str(table)+" PARTITION("+str(partition)+") WHERE (partner01 = '"+str(partner_code)+"' or partner02 = '"+str(partner_code)+"' or partner03 ='"+str(partner_code)+"') and id IS NOT NULL"+ str(nicknamequery) + ") a,service_prefix b ,service_prefix c,service_type d WHERE a.callerTelcoId = b.id AND a.calleeTelcoid = c.id AND a.callType =d.id union all "
                            sql_find = "SELECT a.id,a.caller,b.telcoName'callerTelcoId',a.callee,c.telcoName'calleeTelcoid',a.callerRtpIp,a.startTime,a.stopTime,a.billTime,a.billAmount,a.callercallid'callercallid',a.brandName,a.cdrId,CASE WHEN a.isMnp = 1 THEN 'True' ELSE 'False' END AS isMnp,d.callType,CASE WHEN a.isBrand =1 THEN 'Sip' ELSE 'Brandname' END AS isBrand ,CASE WHEN a.endReason = '-8' THEN 'CalleeHangup' WHEN a.endReason = '-7' THEN 'CallerHangup' ELSE 'other' END AS endReason,a.partnerTag FROM (SELECT * FROM "+str(table)+" PARTITION("+str(partition)+") WHERE "+ str(nicknamequery) + ") a,service_prefix b ,service_prefix c,service_type d WHERE a.callerTelcoId = b.id AND a.calleeTelcoid = c.id AND a.callType =d.id union all "
                        else :
                            sql_find = "SELECT a.id,a.caller,b.telcoName'callerTelcoId',a.callee,c.telcoName'calleeTelcoid',a.callerRtpIp,a.startTime,a.stopTime,a.billTime,a.billAmount,a.callercallid'callercallid',a.brandName,a.cdrId,CASE WHEN a.isMnp = 1 THEN 'True' ELSE 'False' END AS isMnp,d.callType,CASE WHEN a.isBrand =1 THEN 'Sip' ELSE 'Brandname' END AS isBrand ,CASE WHEN a.endReason = '-8' THEN 'CalleeHangup' WHEN a.endReason = '-7' THEN 'CallerHangup' ELSE 'other' END AS endReason,a.partnerTag FROM (SELECT * FROM "+str(table)+" PARTITION("+str(partition)+") WHERE "+ str(nicknamequery) + ") a,service_prefix b ,service_prefix c,service_type d WHERE a.callerTelcoId = b.id AND a.calleeTelcoid = c.id AND a.callType =d.id union all "
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
        

@report_exception.route('/report_exception_calltype', methods=['GET'])
def report_exception_calltype():
    logging.info("----------------------report_exception_calltype---------------------")
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
            logging.info("partner_code : "+ str(partner_code))
            logging.info("partner : "+ str(partner))
            if str(partner) != "":
                partner= str(partner).replace("[","").replace("]","").replace("'","")
                partner =re.split('[,;/ ]+', partner)
                logging.info("partner : "+ str(partner))
                nickname=[]
                for i in partner:
                    sqlpartnerid = "select partner_code from partner where nickname ='"+str(i)+"'"
                    dfpartnerid = pd.read_sql(sqlpartnerid,db)
                    if not dfpartnerid.empty:
                        nickname.append(str(dfpartnerid.iloc[0,0]))
                logging.info("nickname"+str(nickname))
                nickname = str(nickname).replace("[","").replace("]","")
            currentDate = time.strftime("%Y-%m-%d %H:%M:%S")
            print(currentDate)
            if str(timetoken) >= str(currentDate):
                if str(partner_code) == "None" and str(partner) == "" :
                    sql= "SELECT a.id,a.dateTime, c.nickname, b.callType, round(a.totalDuration/60,0)'totalDuration',a.totalAmount,a.vosIp, CASE WHEN a.isBrand =0 THEN 'BRAND' ELSE 'SIP' END AS isBrand FROM service_partner a ,service_type b ,partner c WHERE a.partnerCode = c.partner_code AND a.callTypeId =b.id AND a.dateTime >= CURDATE()"
                elif str(partner_code) == "None" and str(partner) != "" :
                    sql= "SELECT a.id,a.dateTime, c.nickname, b.callType, round(a.totalDuration/60,0)'totalDuration',a.totalAmount,a.vosIp, CASE WHEN a.isBrand =0 THEN 'BRAND' ELSE 'SIP' END AS isBrand FROM service_partner a ,service_type b ,partner c WHERE a.partnerCode = c.partner_code AND a.callTypeId =b.id AND a.partnerCode in ("+str(nickname)+") AND a.dateTime >= CURDATE()"
                else :
                    sql= "SELECT a.id,a.dateTime, c.nickname, b.callType, round(a.totalDuration/60,0)'totalDuration',a.totalAmount,a.vosIp, CASE WHEN a.isBrand =0 THEN 'BRAND' ELSE 'SIP' END AS isBrand FROM service_partner a ,service_type b ,partner c WHERE a.partnerCode = c.partner_code AND a.callTypeId =b.id AND a.partnerCode ='"+str(partner_code)+"' and a.dateTime >= CURDATE()"
                logging.info(sql)
                df = pd.read_sql(sql,db)
                df['dateTime'] = df['dateTime'].astype('str')
                json_records = df.to_json(orient ='records')
                data = []
                data = json.loads(json_records)
                if str(partner_code) == "None" and str(partner) == "" :
                    sqltotal = "SELECT SUM(totalDuration)'totalDuration',SUM(totalAmount)'totalAmount' FROM service_partner WHERE  DATETIME >= CURDATE()"
                elif str(partner_code) == "None" and str(partner) != "" :
                    sqltotal = "SELECT SUM(totalDuration)'totalDuration',SUM(totalAmount)'totalAmount' FROM service_partner WHERE partnerCode in ("+str(nickname)+") and DATETIME >= CURDATE()"
                else :
                    sqltotal = "SELECT SUM(totalDuration)'totalDuration',SUM(totalAmount)'totalAmount' FROM service_partner WHERE partnerCode ='"+str(partner_code)+"' and DATETIME >= CURDATE()"
                logging.info(sqltotal)
                dftotal = pd.read_sql(sqltotal,db)
                json_recordstotal = dftotal.to_json(orient ='records')
                datatotal = []
                datatotal = json.loads(json_recordstotal)
                context = {'data': data,'total':datatotal,'code': 'OK'}
                db.close()
                return context
            else :
                return jsonify({'NOTOK': 'token hết hạn','code':401}),401
    except Exception as e:
        logging.error(traceback.format_exc())
        return jsonify({'NOTOK': str(traceback.format_exc())})

@report_exception.route('/find_report_exception_calltype', methods=['GET'])
def find_report_exception_calltype():
    logging.info("----------------------find_report_exception_calltype-------------------------")
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
            partner = data["partner"]
            partner_codes = data["partner_code"]
            logging.info("partner_code : "+ str(partner_codes))
            logging.info("partner : "+ str(partner))
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
                logging.info("nicknames"+str(nicknames))
                nicknames = str(nicknames).replace("[","").replace("]","")
            if True:
                currentDate = time.strftime("%Y-%m-%d %H:%M:%S")
                logging.info(currentDate)
                if str(timetoken) >= str(currentDate) :
                    logging.info(request.args)
                    starttime = request.args.get('starttime')
                    endtime = request.args.get('endtime')
                    if endtime != "" :
                        endtime = str(endtime) + " 23:59:59"
                    callType = request.args.get('callType')
                    nickname = request.args.get('nickname')
                    #check partner code
                    if str(nickname) != "" :
                        sql_partnercode = "SELECT partner_code FROM partner where nickname ='"+str(nickname)+"'"
                        df_partnercode = pd.read_sql(sql_partnercode,db)
                        if df_partnercode.empty :
                            db.close()
                            return jsonify({'NOTOK': 'Không có nickname này'}),400
                        partner_code =df_partnercode.iloc[0,0]
                    if len(starttime) == 0 :
                        timestartquery = ""
                    else :
                        timestartquery = " a.dateTime >= '"+ str(starttime) +"'"
                    if len(endtime) == 0 :
                        timeendquery = ""
                    else :
                        timeendquery = " and a.dateTime <= '"+ str(endtime) +"'"
                    if len(nickname) == 0 :
                        nicknamequery = ""
                    else :
                        nicknamequery = " and a.partnerCode = '"+str(partner_code)+"'"
                    if len(callType) == 0 :
                        callTypequery = ""
                    else :
                        callTypequery = " and a.callTypeId = (select id from service_type where callType = '"+str(callType)+"')"
                    sql = timestartquery + timeendquery + nicknamequery +callTypequery
                    if str(partner_codes) == "None" and str(partner) == "" :
                        logging.info("vai trò admin")
                        sqlfind = "SELECT a.id,a.dateTime, c.nickname, b.callType, round(a.totalDuration/60,0)'totalDuration',a.totalAmount,a.vosIp, CASE WHEN a.isBrand =0 THEN 'BRAND' ELSE 'SIP' END AS isBrand FROM service_partner a ,service_type b ,partner c WHERE a.partnerCode = c.partner_code AND a.callTypeId =b.id and"+sql
                    elif str(partner_codes) == "None" and str(partner) != "" :
                        sqlfind = "SELECT a.id,a.dateTime, c.nickname, b.callType, round(a.totalDuration/60,0)'totalDuration',a.totalAmount,a.vosIp, CASE WHEN a.isBrand =0 THEN 'BRAND' ELSE 'SIP' END AS isBrand FROM service_partner a ,service_type b ,partner c WHERE a.partnerCode = c.partner_code AND a.callTypeId =b.id and a.partnerCode in ("+str(nicknames)+") and"+sql
                    else :
                        sqlfind = "SELECT a.id,a.dateTime, c.nickname, b.callType, round(a.totalDuration/60,0)'totalDuration',a.totalAmount,a.vosIp, CASE WHEN a.isBrand =0 THEN 'BRAND' ELSE 'SIP' END AS isBrand FROM service_partner a ,service_type b ,partner c WHERE a.partnerCode = c.partner_code AND a.callTypeId =b.id and a.partnerCode ='"+str(partner_codes)+"' and"+sql
                    logging.info("sqlfind : " +sqlfind)
                    df = pd.read_sql(sqlfind,db)
                    df["dateTime"] = pd.to_datetime(df["dateTime"])
                    df = df.sort_values(by=['dateTime'], ascending=False)
                    df['dateTime'] = df['dateTime'].astype('str')
                    json_records = df.to_json(orient ='records')
                    data = []
                    data = json.loads(json_records)
                    if str(partner_codes) == "None" :
                        sqltotal = "SELECT sum(a.totalDuration)'totalDuration',sum(a.totalAmount),'totalAmount' FROM service_partner a  WHERE"+sql
                    else :
                        sqltotal = "SELECT sum(a.totalDuration)'totalDuration',sum(a.totalAmount),'totalAmount' FROM service_partner a  WHERE a.partnerCode ='"+str(partner_code)+"'"+sql
                    logging.info(sqltotal)
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
                return jsonify({'NOTOK': 'khong co quyen'}),400
    except Exception as e:
        logging.error(traceback.format_exc())
        return jsonify({'NOTOK': str(traceback.format_exc())}),400

@report_exception.route('/saovang_list_calltype', methods=['GET'])
def saovang_list_calltype():
    logging.info("----------------------list_calltype-------------------------")
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
                    logging.info(request.args)
                    nickname = request.args.get('nickname')
                    #check partner code
                    if str(nickname) != "" :
                        sql_partnercode = "SELECT partner_code FROM partner where nickname ='"+str(nickname)+"'"
                        df_partnercode = pd.read_sql(sql_partnercode,db)
                        if df_partnercode.empty :
                            db.close()
                            return jsonify({'NOTOK': 'Không có nickname này'}),400
                        partner_code =df_partnercode.iloc[0,0]
                    sqlfind = "SELECT id,callType from service_type where id in (select callTypeId from service_partner where partnerCode = '"+str(partner_code)+"')"
                    logging.info(sqlfind)
                    df = pd.read_sql(sqlfind,db)
                    json_records = df.to_json(orient ='records')
                    data = []
                    data = json.loads(json_records)
                    context = {'data': data,'code': 'OK'}
                    db.close()
                    return context
                else :
                    return jsonify({'NOTOK': 'token hết hạn','code':401}),401
            else :
                return jsonify({'NOTOK': 'khong co quyen'}),400
    except Exception as e:
        logging.error(traceback.format_exc())
        return jsonify({'NOTOK': str(traceback.format_exc())})

@report_exception.route('/report_exception_hinh_tron', methods=['GET'])
def report_exception_hinh_tron():
    logging.info("------------------------report_exception_hinh_tron----------------------------")
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
            partner = data["partner"]
            partner_code = data["partner_code"]
            if True:
                currentDate = time.strftime("%Y-%m-%d %H:%M:%S")
                print(currentDate)
                if str(timetoken) >= str(currentDate) :
                    if str(partner_code) == "None":
                        sql ="(SELECT '1'`id`,SUM(revenue)'value' ,'SIP Quốc Tế'`name`FROM report_exception WHERE createdtime >= DATE_SUB(NOW(), INTERVAL 31 DAY) AND isBrand =1 AND (partner01 IN (SELECT partner_code FROM partner WHERE partner_type =2) OR partner02 IN (SELECT partner_code FROM partner WHERE partner_type =2) OR partner03 IN (SELECT partner_code FROM partner WHERE partner_type =2))) UNION ALL (SELECT '3'`id`,SUM(revenue)'value' ,'SIP Trong Nước'`name`FROM report_exception WHERE createdtime >= DATE_SUB(NOW(), INTERVAL 31 DAY) AND isBrand =1 AND (partner01 IN (SELECT partner_code FROM partner WHERE partner_type =1) OR partner02 IN (SELECT partner_code FROM partner WHERE partner_type =1) OR partner03 IN (SELECT partner_code FROM partner WHERE partner_type =1))) UNION ALL (SELECT '2'`id`,SUM(revenue)'value','BRAND'`name` FROM report_exception WHERE createdtime >= DATE_SUB(NOW(), INTERVAL 31 DAY) AND isBrand =0)"
                    else :
                        sql = "(SELECT '1'`id`,SUM(revenue)'value' ,'SIP'`name`FROM report_exception WHERE createdtime >= DATE_SUB(NOW(), INTERVAL 31 day) AND isBrand =1 AND partner_code like '%"+str(partner_code)+"%') union all (SELECT '2'`id`,SUM(revenue)'value','BRAND'`name` FROM report_exception WHERE createdtime >= DATE_SUB(NOW(), INTERVAL 31 day) AND isBrand =0 AND partner_code like '%"+str(partner_code)+"%')"
                    logging.info(sql)
                    df = pd.read_sql(sql,db)
                    print(str(df.iloc[0,0]))
                    if str(df.iloc[0,0]) == 'None' :
                        return jsonify({'OK': 'Không có dữ liệu'})
                    else :
                        json_records = df.to_json(orient ='records')
                        data = []
                        data = json.loads(json_records)
                        context = {'data': data,'code': 'OK'}
                        return context
                    return jsonify({'OK': 'Không có dữ liệu'})
                else :
                    return jsonify({'NOTOK': 'token hết hạn'})
            else :
                return jsonify({'NOTOK': 'khong co quyen'})
    except Exception as e:
        logging.error(traceback.format_exc())
        return jsonify({'NOTOK': str(e)})
@report_exception.route('/download_report_exception_detail', methods=['POST'])
def download_report_exception_detail():
    logging.info("----------------------download_report_exception_detail-------------------------")
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
                        nickname = record["selectedPartner"]
                    except Exception as e:
                        logging.error(traceback.format_exc())
                        return jsonify({'NOTOK': 'Vui lòng chọn ngày và tên đối tác'}),400
                    if str(nickname) == "" :
                        nicknamequery = ""
                    else :
                        sql_partnercode = "SELECT partner_code FROM partner where nickname ='"+str(nickname)+"'"
                        df_partnercode = pd.read_sql(sql_partnercode,db1)
                        if df_partnercode.empty :
                            db1.close()
                            return jsonify({'NOTOK': 'Không có nickname này'}),400
                        partner_code =df_partnercode.iloc[0,0]
                        nicknamequery = "(partner01 = "+ str(partner_code) +" or partner02 = "+ str(partner_code)+" or partner03 = "+ str(partner_code) +")"
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
                        # sql_find = "SELECT a.caller,b.telcoName'callerTelcoId',a.callee,c.telcoName'calleeTelcoid',a.callerRtpIp,a.startTime,a.stopTime,a.billTime,a.billAmount,a.callercallid'callercallid',a.brandName,CASE WHEN a.isMnp = 1 THEN 'True' ELSE 'False' END AS isMnp,d.callType,CASE WHEN a.isBrand =1 THEN 'Sip' ELSE 'Brandname' END AS isBrand ,CASE WHEN a.endReason = '-8' THEN 'CalleeHangup' WHEN a.endReason = '-7' THEN 'CallerHangup' ELSE 'other' END AS endReason FROM (SELECT * FROM "+str(table)+" PARTITION("+str(partition)+") WHERE id IS NOT NULL"+ str(nicknamequery) + ") a,service_prefix b ,service_prefix c,service_type d WHERE a.callerTelcoId = b.id AND a.calleeTelcoid = c.id AND a.callType =d.id union all "
                        sql_find = "SELECT a.caller,b.telcoName'callerTelcoId',a.callee,c.telcoName'calleeTelcoid',a.callerRtpIp,a.startTime,a.stopTime,a.billTime,a.billAmount,a.callercallid'callercallid',a.brandName,CASE WHEN a.isMnp = 1 THEN 'True' ELSE 'False' END AS isMnp,d.callType,CASE WHEN a.isBrand =1 THEN 'Sip' ELSE 'Brandname' END AS isBrand ,CASE WHEN a.endReason = '-8' THEN 'CalleeHangup' WHEN a.endReason = '-7' THEN 'CallerHangup' ELSE 'other' END AS endReason FROM (SELECT * FROM "+str(table)+" PARTITION("+str(partition)+") WHERE "+ str(nicknamequery) + ") a,service_prefix b ,service_prefix c,service_type d WHERE a.callerTelcoId = b.id AND a.calleeTelcoid = c.id AND a.callType =d.id union all "
                        sql_find_detail = sql_find_detail + sql_find
                    sql_find_detail = sql_find_detail[:-11]
                    logging.info(sql_find_detail)
                    df = pd.read_sql(sql_find_detail,db)
                    logging.info("lấy dữ liệu xong")
                    df['startTime'] = df['startTime'].astype('str')
                    df['stopTime'] = df['stopTime'].astype('str')
                    session=datetime.datetime.now()
                    filename = "report_exception"+str(session).replace(" ","")+".csv"
                    filepath = "filedownload/report_exception"+str(session).replace(" ","")
                    # taọ thư mục filepath
                    if not os.path.exists(filepath):
                        os.mkdir(filepath)
                    path = os.path.join(os.getcwd(),filepath,filename)
                    path_export = os.path.join(os.getcwd(),filepath,'report_exception')
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

@report_exception.route('/saovang_report_supplier_time', methods=['GET'])
def saovang_report_supplier_time():
    now = datetime.datetime.now()
    session = str(now).replace("-","").replace(":","").replace(" ","").replace(".","")
    logging.info(session + "------------------------report_exception_supplier_time----------------------------")
    try:
        db = pymysql.connect(host="172.17.0.1",    # your host, usually localhost
                        user="leeoncrm",         # your username
                        passwd="41XmKsO3NBgHPwv",  # your password
                        db="leeon_crm",
                        port=3306
                        )
        data = group()
        logging.info(session +"| data | : "+ str(data))
        if str(data)== "chua truyen token" :
            return jsonify({'NOTOK': str(data)})
        else:
            timetoken = data["expireddate"]
            group_name = data["group_name"]
            partner = data["partner"]
            partner_code = data["partner_code"]
            if True:
                currentDate = time.strftime("%Y-%m-%d %H:%M:%S")
                print(currentDate)
                if str(timetoken) >= str(currentDate) :
                    if str(group_name) != "CUSTOMER" :
                        sql_partner_code = "SELECT partner_code from partner WHERE agency_id= 1"
                        df_partnercode = pd.read_sql(sql_partner_code,db)
                        # lưu tất cả partner1 , partner2 , partner3 vào 1 list
                        list_partner = df_partnercode['partner_code'].tolist()
                        logging.info("list_partner : "+ str(list_partner))
                        list_partner = str(list_partner).replace("[","'").replace("]","'").replace(", ","|")
                        sql ="SELECT b.name'name',round(SUM(a.voicetime)/60,0)'value' FROM report a,supplier b  WHERE a.createdtime >= DATE_SUB(NOW(), INTERVAL 31 DAY) and a.supplierId is not null and a.supplierId =b.id and a.partner_code REGEXP "+list_partner+" GROUP BY b.name ORDER BY value desc"
                    else :
                        sql ="SELECT b.name'name',round(SUM(a.voicetime)/60,0)'value' FROM report a,supplier b  WHERE a.createdtime >= DATE_SUB(NOW(), INTERVAL 31 DAY) and a.supplierId is not null and a.supplierId =b.id and a.partner01 = "+str(partner_code)+" GROUP BY b.name ORDER BY value desc"
                    logging.info(session + "| sql |" + sql)
                    df = pd.read_sql(sql,db)
                    json_records = df.to_json(orient ='records')
                    data = []
                    data = json.loads(json_records)
                    context = {'data': data,'code': 'OK'}
                    return context
                    return jsonify({'OK': 'Không có dữ liệu'})
                else :
                    return jsonify({'NOTOK': 'token hết hạn'})
            else :
                return jsonify({'NOTOK': 'khong co quyen'})
    except Exception as e:
        logging.error(traceback.format_exc())
        return jsonify({'NOTOK': str(e)})

@report_exception.route('/report_exception_theo_huong', methods=['GET'])
def report_exception_theo_huong():
    logging.info("------------------------totalreport_exception----------------------------")
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
            partner = data["partner"]
            partner_code = data["partner_code"]
            if True:
                currentDate = time.strftime("%Y-%m-%d %H:%M:%S")
                print(currentDate)
                if str(timetoken) >= str(currentDate) :
                    sql ="SELECT a.partnerCode,a.dateTime,SUM(a.totalDuration)'totalDuration',ROUND(SUM(a.totalAmount),0)'totalAmount',b.callType,b.groupId FROM service_partner a, service_type b WHERE a.callTypeId = b.id  AND a.dateTime >= CURDATE() GROUP BY b.groupId,b.callType"
                    logging.info(sql)
                    df = pd.read_sql(sql,db)
                    print(str(df.iloc[0,0]))
                    if str(df.iloc[0,0]) == 'None' :
                        return jsonify({'OK': 'Không có dữ liệu'})
                    else :
                        json_records = df.to_json(orient ='records')
                        data = []
                        data = json.loads(json_records)
                        context = {'data': data,'code': 'OK'}
                        return context
                    return jsonify({'OK': 'Không có dữ liệu'})
                else :
                    return jsonify({'NOTOK': 'token hết hạn'})
            else :
                return jsonify({'NOTOK': 'khong co quyen'})
    except Exception as e:
        logging.error(traceback.format_exc())
        return jsonify({'NOTOK': str(e)})

@report_exception.route('/report_exception_supplier_time_v2', methods=['GET'])
def report_exception_supplier_time_v2():
    now = datetime.datetime.now()
    session = str(now).replace("-","").replace(":","").replace(" ","").replace(".","")
    logging.info(session + "------------------------report_exception_supplier_time----------------------------")
    try:
        db = pymysql.connect(host="172.17.0.1",    # your host, usually localhost
                        user="leeoncrm",         # your username
                        passwd="41XmKsO3NBgHPwv",  # your password
                        db="leeon_crm",
                        port=3306
                        )
        data = group()
        logging.info(session +"| data | : "+ str(data))
        if str(data)== "chua truyen token" :
            return jsonify({'NOTOK': str(data)})
        else:
            timetoken = data["expireddate"]
            group_name = data["group_name"]
            partner = data["partner"]
            partner_code = data["partner_code"]
            if True:
                currentDate = time.strftime("%Y-%m-%d %H:%M:%S")
                print(currentDate)
                if str(timetoken) >= str(currentDate) :
                    sql ="SELECT b.nickname'name',round(SUM(a.voicetime)/60,0)'value' FROM report a,supplier b  WHERE a.createdtime >= DATE_SUB(NOW(), INTERVAL 31 DAY) and a.supplierId is not null and a.supplierId =b.id GROUP BY b.nickname ORDER BY value desc"
                    logging.info(session + "| sql |" + sql)
                    df = pd.read_sql(sql,db)
                    if str(df.iloc[0,0]) == 'None' :
                        return jsonify({'OK': 'Không có dữ liệu'})
                    else :
                        json_records = df.to_json(orient ='records')
                        data = []
                        data = json.loads(json_records)
                        context = {'data': data,'code': 'OK'}
                        return context
                    return jsonify({'OK': 'Không có dữ liệu'})
                else :
                    return jsonify({'NOTOK': 'token hết hạn'})
            else :
                return jsonify({'NOTOK': 'khong co quyen'})
    except Exception as e:
        logging.error(traceback.format_exc())
        return jsonify({'NOTOK': str(e)})

@report_exception.route('/download_report_exception_detail_customer', methods=['POST'])
def download_report_exception_detail_customer():
    logging.info("----------------------download_report_exception_detail-------------------------")
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
            partner_codes = data["partner_code"]
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
                        nickname = record["selectedPartner"]
                    except Exception as e:
                        logging.error(traceback.format_exc())
                        return jsonify({'NOTOK': 'Vui lòng chọn ngày và tên đối tác'}),400
                    if str(nickname) == "" :
                        nicknamequery = ""
                    else :
                        sql_partnercode = "SELECT partner_code FROM partner where nickname ='"+str(nickname)+"'"
                        df_partnercode = pd.read_sql(sql_partnercode,db1)
                        if df_partnercode.empty :
                            db1.close()
                            return jsonify({'NOTOK': 'Không có nickname này'}),400
                        partner_code =df_partnercode.iloc[0,0]
                        if str(partner_code) == str(partner_codes) :
                            nicknamequery = "partner01 = "+ str(partner_code)
                        else :
                            sql_partnercode2 = "SELECT partner2 FROM partner_group where partner2 ='"+str(partner_code)+"'"
                            df_partnercode2 = pd.read_sql(sql_partnercode2,db1)
                            if df_partnercode2.empty :
                                nicknamequery = "partner03 = "+ str(partner_code)
                            else : 
                                nicknamequery = "partner02 = "+ str(partner_code)
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
                        sql_find = "SELECT a.caller,b.telcoName'callerTelcoId',a.callee,c.telcoName'calleeTelcoid',a.callerRtpIp,a.startTime,a.stopTime,a.billTime,a.billAmount,a.callercallid'callercallid',a.brandName,CASE WHEN a.isMnp = 1 THEN 'True' ELSE 'False' END AS isMnp,d.callType,CASE WHEN a.isBrand =1 THEN 'Sip' ELSE 'Brandname' END AS isBrand ,CASE WHEN a.endReason = '-8' THEN 'CalleeHangup' WHEN a.endReason = '-7' THEN 'CallerHangup' ELSE 'other' END AS endReason FROM (SELECT * FROM "+str(table)+" PARTITION("+str(partition)+")  WHERE "+ str(nicknamequery) + ") a ,service_prefix b ,service_prefix c,service_type d WHERE a.callerTelcoId = b.id AND a.calleeTelcoid = c.id AND a.callType =d.id union all "
                        sql_find_detail = sql_find_detail + sql_find
                    sql_find_detail = sql_find_detail[:-11]
                    logging.info(sql_find_detail)
                    df = pd.read_sql(sql_find_detail,db)
                    logging.info("lấy dữ liệu xong")
                    df['startTime'] = df['startTime'].astype('str')
                    df['stopTime'] = df['stopTime'].astype('str')
                    session=datetime.datetime.now()
                    filename = "report_exception"+str(session).replace(" ","")+".csv"
                    filepath = "filedownload/report_exception"+str(session).replace(" ","")
                    # taọ thư mục filepath
                    if not os.path.exists(filepath):
                        os.mkdir(filepath)
                    path = os.path.join(os.getcwd(),filepath,filename)
                    path_export = os.path.join(os.getcwd(),filepath,'report_exception')
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

@report_exception.route('/find_report_exception_detail_customer', methods=['GET'])
def find_report_exception_detail_customer():
    logging.info("----------------------find_report_exception_detail-------------------------")
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
                    nickname = request.args.get('nickname')
                    if str(nickname) == "" :
                        nicknamequery = ""
                    else :
                        sql_partnercode = "SELECT partner_code FROM partner where nickname ='"+str(nickname)+"'"
                        df_partnercode = pd.read_sql(sql_partnercode,db1)
                        if df_partnercode.empty :
                            db1.close()
                            return jsonify({'NOTOK': 'Không có tên đối tác này'}),400
                        partner_code =df_partnercode.iloc[0,0]
                        if str(partner_code) == str(partner_codes) :
                            nicknamequery = "partner01 = "+ str(partner_code)
                        else :
                            sql_partnercode2 = "SELECT partner2 FROM partner_group where nickname ='"+str(nickname)+"'"
                            df_partnercode2 = pd.read_sql(sql_partnercode2,db1)
                            if df_partnercode2.empty :
                                nicknamequery = "partner03 = "+ str(partner_code)
                            else : 
                                nicknamequery = "partner02 = "+ str(partner_code)
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
                        if str(group_name) =="CUSTOMER":
                            # sql= "SELECT COUNT(1)'count1' FROM "+str(table)+" PARTITION("+str(partition)+") WHERE (partner01 = '"+str(partner_code)+"' or partner02 = '"+str(partner_code)+"' or partner03 ='"+str(partner_code)+"') and id is not null"+ str(nicknamequery) + " union all "
                            sql= "SELECT COUNT(1)'count1' FROM "+str(table)+" PARTITION("+str(partition)+") WHERE "+ str(nicknamequery) + " union all "
                            sql_total =sql_total+sql
                        else :
                            sql= "SELECT COUNT(1)'count1' FROM "+str(table)+" PARTITION("+str(partition)+") WHERE "+ str(nicknamequery) + " union all "
                            sql_total =sql_total+sql
                        #query find detail
                        if str(group_name) =="CUSTOMER":
                            sql_find = "SELECT a.id,a.caller,b.telcoName'callerTelcoId',a.callee,c.telcoName'calleeTelcoid',a.callerRtpIp,a.startTime,a.stopTime,a.billTime,a.billAmount,a.callercallid'callercallid',a.brandName,a.cdrId,CASE WHEN a.isMnp = 1 THEN 'True' ELSE 'False' END AS isMnp,d.callType,CASE WHEN a.isBrand =1 THEN 'Sip' ELSE 'Brandname' END AS isBrand ,CASE WHEN a.endReason = '-8' THEN 'CalleeHangup' WHEN a.endReason = '-7' THEN 'CallerHangup' ELSE 'other' END AS endReason,a.partnerTag FROM (SELECT * FROM "+str(table)+" PARTITION("+str(partition)+") WHERE "+ str(nicknamequery) + ") a,service_prefix b ,service_prefix c,service_type d WHERE a.callerTelcoId = b.id AND a.calleeTelcoid = c.id AND a.callType =d.id union all "
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

@report_exception.route('/saovang_report_calltype', methods=['GET'])
def saovang_report_calltype():
    logging.info("----------------------saovang_report_calltype---------------------")
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
            partner_code = data["partner_code"]
            logging.info("partner_code : "+ str(partner_code))
            currentDate = time.strftime("%Y-%m-%d %H:%M:%S")
            if str(timetoken) >= str(currentDate):
                if str(group_name) =="CUSTOMER":
                    # sql= "SELECT a.id,a.dateTime, c.nickname, b.callType, round(a.totalDuration/60,0)'totalDuration',a.totalAmount,a.vosIp, CASE WHEN a.isBrand =0 THEN 'BRAND' ELSE 'SIP' END AS isBrand FROM service_partner a ,service_type b ,partner c WHERE a.partnerCode = c.partner_code AND a.callTypeId =b.id AND a.partnerCode = "+str(partner_code)+" AND a.dateTime >= CURDATE()"
                    sql = "SELECT a.id,a.createdtime'dateTime',a.callType,b.nickname,SUM(a.voicetime)/60'totalDuration',SUM(a.revenue)'totalAmount',a.vosIp,CASE WHEN a.isBrand =0 THEN 'BRAND' ELSE 'SIP' END AS isBrand FROM report a,partner b  WHERE RIGHT(a.partner_code,5) = b.partner_code AND a.partner_code like '%"+str(partner_code)+"%' AND a.createdtime >= CURDATE() and a.revenue >0 GROUP BY a.callType,a.partner_code"
                else :
                    sql_partner_code = "SELECT partner_code from partner WHERE agency_id= 1"
                    df_partnercode = pd.read_sql(sql_partner_code,db)
                    # lưu tất cả partner1 , partner2 , partner3 vào 1 list
                    list_partner = df_partnercode['partner_code'].tolist()
                    logging.info("list_partner : "+ str(list_partner))
                    list_partner = str(list_partner).replace("[","'").replace("]","'").replace(", ","|")
                    # sql= "SELECT a.id,a.dateTime, c.nickname, b.callType, round(a.totalDuration/60,0)'totalDuration',a.totalAmount,a.vosIp, CASE WHEN a.isBrand =0 THEN 'BRAND' ELSE 'SIP' END AS isBrand FROM service_partner a ,service_type b ,partner c WHERE a.partnerCode = c.partner_code AND a.callTypeId =b.id AND a.partnerCode in "+str(list_partner)+" AND a.dateTime >= CURDATE()"
                    sql = "SELECT a.id,a.createdtime'dateTime',a.callType,b.nickname,SUM(a.voicetime)/60'totalDuration',SUM(a.revenue)'totalAmount',a.vosIp,CASE WHEN a.isBrand =0 THEN 'BRAND' ELSE 'SIP' END AS isBrand FROM report a,partner b  WHERE RIGHT(a.partner_code,5) = b.partner_code AND a.partner_code REGEXP "+str(list_partner)+" AND a.createdtime >= CURDATE() and a.revenue >0 GROUP BY a.callType,a.partner_code"
                logging.info(sql)
                df = pd.read_sql(sql,db)
                df['dateTime'] = df['dateTime'].astype('str')
                json_records = df.to_json(orient ='records')
                data = []
                data = json.loads(json_records)
                if str(group_name) =="CUSTOMER":
                    # sqltotal = "SELECT SUM(totalDuration)'totalDuration',SUM(totalAmount)'totalAmount' FROM service_partner WHERE  DATETIME >= CURDATE() and partnerCode in "+str(list_partner)
                    sqltotal = "SELECT SUM(voicetime)/60'totalDuration',SUM(revenue)'totalAmount' FROM report WHERE partner_code like '%"+str(partner_code)+"%' and createdtime >= CURDATE()"
                else :
                    sqltotal = "SELECT SUM(voicetime)/60'totalDuration',SUM(revenue)'totalAmount' FROM report WHERE partner_code REGEXP "+str(list_partner)+" and createdtime >= CURDATE()"
                logging.info(sqltotal)
                dftotal = pd.read_sql(sqltotal,db)
                json_recordstotal = dftotal.to_json(orient ='records')
                datatotal = []
                datatotal = json.loads(json_recordstotal)
                context = {'data': data,'total':datatotal,'code': 'OK'}
                db.close()
                return context
            else :
                return jsonify({'NOTOK': 'token hết hạn','code':401}),401
    except Exception as e:
        logging.error(traceback.format_exc())
        return jsonify({'NOTOK': str(traceback.format_exc())})

@report_exception.route('/saovang_report_supplier_time_theo_huong', methods=['GET'])
def saovang_report_supplier_time_theo_huong():
    now = datetime.datetime.now()
    session = str(now).replace("-","").replace(":","").replace(" ","").replace(".","")
    logging.info(session + "------------------------report_supplier_time_theo_huong----------------------------")
    try:
        db = pymysql.connect(host="172.17.0.1",    # your host, usually localhost
                        user="leeoncrm",         # your username
                        passwd="41XmKsO3NBgHPwv",  # your password
                        db="leeon_crm",
                        port=3306
                        )
        data = group()
        logging.info(session +"| data | : "+ str(data))
        if str(data)== "chua truyen token" :
            return jsonify({'NOTOK': str(data)})
        else:
            timetoken = data["expireddate"]
            group_name = data["group_name"]
            partner_code = data["partner_code"]
            if True:
                currentDate = time.strftime("%Y-%m-%d %H:%M:%S")
                if str(timetoken) >= str(currentDate) :
                    sql_partner_code = "SELECT partner_code from partner WHERE agency_id= 1"
                    df_partnercode = pd.read_sql(sql_partner_code,db)
                    # lưu tất cả partner1 , partner2 , partner3 vào 1 list
                    list_partner = df_partnercode['partner_code'].tolist()
                    logging.info("list_partner : "+ str(list_partner))
                    list_partner = str(list_partner).replace("[","(").replace("]",")")
                    sql ="SELECT a.id,a.createdtime,b.nickname,a.callType,ROUND(SUM(a.voicetime)/60,0)'voicetime' FROM report a,supplier b  WHERE a.supplierId IS NOT NULL AND a.supplierId =b.id and a.partner01 in "+list_partner+" AND a.createdtime >= CURDATE() AND voicetime > 0 GROUP BY a.supplierId,a.callType"
                    logging.info(session + "| sql |" + sql)
                    df = pd.read_sql(sql,db)
                    df['createdtime'] = df['createdtime'].astype('str')
                    sqltotal="SELECT a.id,a.voicetime'total',b.voicetime'noimang',c.voicetime'ngoaimang',d.voicetime'codinh',e.voicetime'DIGITEL - 1900' FROM(SELECT id,ROUND(SUM(voicetime)/60,0)'voicetime' FROM report WHERE createdtime >= CURDATE() and partner01 in "+list_partner+") a,(SELECT id,ROUND(SUM(voicetime)/60,0)'voicetime' FROM report WHERE createdtime >= CURDATE() AND callType LIKE'%Nội mạng%' and partner01 in "+list_partner+") b,(SELECT id,ROUND(SUM(voicetime)/60,0)'voicetime' FROM report WHERE createdtime >= CURDATE() and partner01 in "+list_partner+" AND callType LIKE'%Ngoại mạng%')c,(SELECT id,ROUND(SUM(voicetime)/60,0)'voicetime' FROM report WHERE createdtime >= CURDATE() and partner01 in "+list_partner+" AND callType LIKE'%Cố định%')d,(SELECT id,ROUND(SUM(voicetime)/60,0)'voicetime' FROM report WHERE createdtime >= CURDATE() and partner01 in "+list_partner+" AND callType LIKE'%DIGITEL - 1900%') e"
                    dftotal = pd.read_sql(sqltotal,db)
                    json_records = df.to_json(orient ='records')
                    data = []
                    data = json.loads(json_records)
                    json_records = dftotal.to_json(orient ='records')
                    data1 = []
                    data1 = json.loads(json_records)
                    context = {'data': data,'total' : data1,'code': 'OK'}
                    return context
                else :
                    return jsonify({'NOTOK': 'token hết hạn'})
            else :
                return jsonify({'NOTOK': 'khong co quyen'})
    except Exception as e:
        logging.error(traceback.format_exc())
        return jsonify({'NOTOK': str(e)})

@report_exception.route('/saovang_find_report_supplier_time_theo_huong', methods=['GET'])
def saovang_find_report_supplier_time_theo_huong():
    now = datetime.datetime.now()
    session = str(now).replace("-","").replace(":","").replace(" ","").replace(".","")
    logging.info(session + "------------------------report_supplier_time_theo_huong----------------------------")
    try:
        db = pymysql.connect(host="172.17.0.1",    # your host, usually localhost
                        user="leeoncrm",         # your username
                        passwd="41XmKsO3NBgHPwv",  # your password
                        db="leeon_crm",
                        port=3306
                        )
        data = group()
        logging.info(session +"| data | : "+ str(data))
        if str(data)== "chua truyen token" :
            return jsonify({'NOTOK': str(data)})
        else:
            timetoken = data["expireddate"]
            group_name = data["group_name"]
            partner = data["partner"]
            partner_code = data["partner_code"]
            if True:
                currentDate = time.strftime("%Y-%m-%d %H:%M:%S")
                print(currentDate)
                if str(timetoken) >= str(currentDate) :
                    starttime = request.args.get('starttime')
                    logging.info("starttime : "+ str(starttime))
                    endtime = request.args.get('endtime')
                    endtime = endtime + " 23:59:59"
                    name = request.args.get('name')
                    call_type = request.args.get('call_type')
                    if str(name) =="":
                        sqlname = ""
                    else :
                        sqlname = " and b.nickname ='" +str(name)+"'"
                    if str(call_type) =="":
                        sqlcall_type = ""
                    else :
                        sqlcall_type = " and a.callType ='" +str(call_type)+"'"
                    sql_partner_code = "SELECT partner_code from partner WHERE agency_id= 1"
                    df_partnercode = pd.read_sql(sql_partner_code,db)
                    # lưu tất cả partner1 , partner2 , partner3 vào 1 list
                    list_partner = df_partnercode['partner_code'].tolist()
                    logging.info("list_partner : "+ str(list_partner))
                    list_partner = str(list_partner).replace("[","(").replace("]",")")
                    sql ="SELECT a.id,a.createdtime,b.nickname,a.callType,ROUND(SUM(a.voicetime)/60,0)'voicetime' FROM report a,supplier b  WHERE a.supplierId IS NOT NULL AND a.supplierId =b.id AND a.createdtime >= '"+str(starttime)+"' AND a.createdtime < '"+str(endtime)+"'"+sqlname+sqlcall_type+" AND voicetime > 0 and a.partner01 in "+list_partner+" GROUP BY a.supplierId,a.callType,a.createdtime order by a.createdtime desc"
                    logging.info(session + "| sql |" + sql)
                    df = pd.read_sql(sql,db)
                    if not df.empty :
                        df['createdtime'] = df['createdtime'].astype('str')
                    json_records = df.to_json(orient ='records')
                    data = []
                    data = json.loads(json_records)
                    sqltotal="SELECT a.voicetime'total',b.voicetime'noimang',c.voicetime'ngoaimang',d.voicetime'codinh',e.voicetime'DIGITEL - 1900' FROM(SELECT ROUND(SUM(a.voicetime)/60,0)'voicetime' FROM report a,supplier b  WHERE a.partner01 in "+list_partner+" and a.supplierId =b.id and a.createdtime >= '"+str(starttime)+"' AND a.createdtime < '"+str(endtime)+"'"+sqlname+sqlcall_type+") a,(SELECT ROUND(SUM(a.voicetime)/60,0)'voicetime' FROM report a ,supplier b  WHERE a.partner01 in "+list_partner+" and a.supplierId =b.id and a.createdtime >= '"+str(starttime)+"' AND a.createdtime < '"+str(endtime)+"'"+sqlname+sqlcall_type+" AND callType LIKE'%Nội mạng%') b,(SELECT ROUND(SUM(a.voicetime)/60,0)'voicetime' FROM report a ,supplier b  WHERE a.partner01 in "+list_partner+" and a.supplierId =b.id and a.createdtime >= '"+str(starttime)+"' AND a.createdtime < '"+str(endtime)+"'"+sqlname+sqlcall_type+" AND callType LIKE'%Ngoại mạng%')c,(SELECT ROUND(SUM(a.voicetime)/60,0)'voicetime' FROM report a ,supplier b  WHERE a.partner01 in "+list_partner+" and a.supplierId =b.id and a.createdtime >= '"+str(starttime)+"' AND a.createdtime < '"+str(endtime)+"'"+sqlname+sqlcall_type+" and callType LIKE'%Cố định%')d,(SELECT ROUND(SUM(a.voicetime)/60,0)'voicetime' FROM report a ,supplier b  WHERE a.partner01 in "+list_partner+" and a.supplierId =b.id and a.createdtime >= '"+str(starttime)+"' AND a.createdtime < '"+str(endtime)+"'"+sqlname+sqlcall_type+" AND callType LIKE'%DIGITEL - 1900%') e"
                    dftotal = pd.read_sql(sqltotal,db)
                    json_records = dftotal.to_json(orient ='records')
                    data1 = []
                    data1 = json.loads(json_records)
                    context = {'data': data,'total' : data1,'code': 'OK'}
                    return context
                else :
                    return jsonify({'NOTOK': 'token hết hạn'})
            else :
                return jsonify({'NOTOK': 'khong co quyen'})
    except Exception as e:
        logging.error(traceback.format_exc())
        return jsonify({'NOTOK': str(e)})

@report_exception.route('/saovang_report_supplier_time_v2', methods=['GET'])
def saovang_report_supplier_time_v2():
    now = datetime.datetime.now()
    session = str(now).replace("-","").replace(":","").replace(" ","").replace(".","")
    logging.info(session + "------------------------report_supplier_time----------------------------")
    try:
        db = pymysql.connect(host="172.17.0.1",    # your host, usually localhost
                        user="leeoncrm",         # your username
                        passwd="41XmKsO3NBgHPwv",  # your password
                        db="leeon_crm",
                        port=3306
                        )
        data = group()
        logging.info(session +"| data | : "+ str(data))
        if str(data)== "chua truyen token" :
            return jsonify({'NOTOK': str(data)})
        else:
            timetoken = data["expireddate"]
            group_name = data["group_name"]
            partner = data["partner"]
            partner_code = data["partner_code"]
            if True:
                currentDate = time.strftime("%Y-%m-%d %H:%M:%S")
                print(currentDate)
                if str(timetoken) >= str(currentDate) :
                    sql_partner_code = "SELECT partner_code from partner WHERE agency_id= 1"
                    df_partnercode = pd.read_sql(sql_partner_code,db)
                    # lưu tất cả partner1 , partner2 , partner3 vào 1 list
                    list_partner = df_partnercode['partner_code'].tolist()
                    logging.info("list_partner : "+ str(list_partner))
                    list_partner = str(list_partner).replace("[","'").replace("]","'").replace(", ","|")
                    sql ="SELECT b.nickname'name',round(SUM(a.voicetime)/60,0)'value' FROM report a,supplier b  WHERE a.createdtime >= DATE_SUB(NOW(), INTERVAL 31 DAY) and a.partner_code REGEXP "+list_partner+" and a.supplierId is not null and a.supplierId =b.id GROUP BY b.nickname ORDER BY value desc"
                    logging.info(session + "| sql |" + sql)
                    df = pd.read_sql(sql,db)
                    json_records = df.to_json(orient ='records')
                    data = []
                    data = json.loads(json_records)
                    context = {'data': data,'code': 'OK'}
                    return context
                else :
                    return jsonify({'NOTOK': 'token hết hạn'})
            else :
                return jsonify({'NOTOK': 'khong co quyen'})
    except Exception as e:
        logging.error(traceback.format_exc())
        return jsonify({'NOTOK': str(e)})

@report_exception.route('/saovang_totalreportdate', methods=['GET'])
def saovang_totalreportdate():
    now = datetime.datetime.now()
    session = str(now).replace("-","").replace(":","").replace(".","").replace(" ","")
    logging.info(session +"----------------saovang_totalreportdate-----------------------")
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
            partner_code = data["partner_code"]
            logging.info(session +" | data | "+str(data))
            logging.info(session +" | partner_code | "+str(partner_code))
            if True:
                startdate = request.args.get('startdate')
                logging.info(startdate)
                enddate = request.args.get('enddate')
                logging.info(enddate)
                enddate =str(enddate) + " 23:59:59"
                if str(startdate) == "None" :
                    # quyền admin
                    if str(group_name) != "CUSTOMER" :
                        sql_partner_code = "SELECT partner_code from partner WHERE agency_id= 1"
                        df_partnercode = pd.read_sql(sql_partner_code,db)
                        # lưu tất cả partner1 , partner2 , partner3 vào 1 list
                        list_partner = df_partnercode['partner_code'].tolist()
                        logging.info("list_partner : "+ str(list_partner))
                        list_partner = str(list_partner).replace("[","'").replace("]","'").replace(", ","|")
                        sql = "SELECT a.id,LEFT(a.createdtime,10)'date',(SUM(a.callsuccess)+SUM(a.callmiss))'calltotal',SUM(a.callsuccess)'callsuccess',SUM(a.callmiss) 'callmiss',ROUND(SUM(a.voicetime)/60,0)'voicetime',SUM(a.revenue)'revenue',a.vosIp,a.telco,CONCAT(ROUND((SUM(a.callmiss)/(SUM(a.callsuccess)+SUM(a.callmiss)))*100,2),' %')'callrate','SIP_TRUNK'`servicename` FROM report a WHERE a.createdtime >= DATE_SUB(NOW(), INTERVAL 31 day) and a.partner_code REGEXP "+list_partner+" GROUP BY date"
                        logging.info(session +" | sql | "+str(sql))
                        df = pd.read_sql(sql,db)
                        df["date"] = pd.to_datetime(df["date"])
                        df = df.sort_values(by=['date'], ascending=False)
                        df['date'] = df['date'].astype('str')
                        json_records = df.to_json(orient ='records')
                        data = []
                        data = json.loads(json_records)
                        sql = "SELECT a.id,(SUM(a.callsuccess)+SUM(a.callmiss))'calltotal',SUM(a.callsuccess)'callsuccess',SUM(a.callmiss) 'callmiss',ROUND(SUM(a.voicetime)/60,0)'voicetime',SUM(a.revenue)'revenue',ROUND((SUM(a.callmiss)/(SUM(a.callsuccess)+SUM(a.callmiss)))*100,2)'callrate' FROM report a WHERE a.createdtime >= DATE_SUB(NOW(), INTERVAL 31 day ) and a.partner_code REGEXP "+list_partner
                        logging.info(session +" | sql_total | "+str(sql))
                        df = pd.read_sql(sql,db)
                        json_records = df.to_json(orient ='records')
                        data1 = []
                        data1 = json.loads(json_records)
                    else :
                        sql = "SELECT a.id,LEFT(a.createdtime,10)'date',(SUM(a.callsuccess)+SUM(a.callmiss))'calltotal',SUM(a.callsuccess)'callsuccess',SUM(a.callmiss) 'callmiss',ROUND(SUM(a.voicetime)/60,0)'voicetime',SUM(a.revenue)'revenue',a.vosIp,a.telco,CONCAT(ROUND((SUM(a.callmiss)/(SUM(a.callsuccess)+SUM(a.callmiss)))*100,2),' %')'callrate','SIP_TRUNK'`servicename` FROM report a WHERE a.createdtime >= DATE_SUB(NOW(), INTERVAL 31 day) and a.partner_code like '%"+partner_code+"%' GROUP BY date"
                        logging.info(session +" | sql | "+str(sql))
                        df = pd.read_sql(sql,db)
                        df["date"] = pd.to_datetime(df["date"])
                        df = df.sort_values(by=['date'], ascending=False)
                        df['date'] = df['date'].astype('str')
                        json_records = df.to_json(orient ='records')
                        data = []
                        data = json.loads(json_records)
                        sql = "SELECT '1'`id`,IFNULL((SUM(a.callsuccess)+SUM(a.callmiss)),0)'calltotal',IFNULL(SUM(a.callsuccess),0)'callsuccess',IFNULL(SUM(a.callmiss),0) 'callmiss',IFNULL(ROUND(SUM(a.voicetime)/60,0),0)'voicetime',IFNULL(SUM(a.revenue),0)'revenue',IFNULL(ROUND((SUM(a.callmiss)/(SUM(a.callsuccess)+SUM(a.callmiss)))*100,2),0)'callrate' FROM report a WHERE a.createdtime >= DATE_SUB(NOW(), INTERVAL 31 day ) and a.partner_code like '%"+partner_code+"%'"
                        logging.info(session +" | sql_total | "+str(sql))
                        df = pd.read_sql(sql,db)
                        json_records = df.to_json(orient ='records')
                        data1 = []
                        data1 = json.loads(json_records)
                    context = {'data': data,'total': data1,'code': 'OK'}
                    db.close()
                    return context
                else :
                    if str(group_name) != "CUSTOMER" :
                        sql_partner_code = "SELECT partner_code from partner WHERE agency_id= 1"
                        df_partnercode = pd.read_sql(sql_partner_code,db)
                        # lưu tất cả partner1 , partner2 , partner3 vào 1 list
                        list_partner = df_partnercode['partner_code'].tolist()
                        logging.info("list_partner : "+ str(list_partner))
                        list_partner = str(list_partner).replace("[","'").replace("]","'").replace(", ","|")
                        sql = "SELECT a.id,LEFT(a.createdtime,10)'date',(SUM(a.callsuccess)+SUM(a.callmiss))'calltotal',SUM(a.callsuccess)'callsuccess',SUM(a.callmiss) 'callmiss',ROUND(SUM(a.voicetime)/60,0)'voicetime',SUM(a.revenue)'revenue',a.vosIp,a.telco,CONCAT(ROUND((SUM(a.callmiss)/(SUM(a.callsuccess)+SUM(a.callmiss)))*100,2),' %')'callrate','SIP_TRUNK'`servicename` FROM report a WHERE a.createdtime >= '"+str(startdate)+"' and a.createdtime <= '"+str(enddate)+"' and a.partner_code REGEXP "+list_partner+" GROUP BY date"
                        df = pd.read_sql(sql,db)
                        json_records = df.to_json(orient ='records')
                        data = []
                        data = json.loads(json_records)
                        sql = "SELECT a.id,(SUM(a.callsuccess)+SUM(a.callmiss))'calltotal',SUM(a.callsuccess)'callsuccess',SUM(a.callmiss) 'callmiss',ROUND(SUM(a.voicetime)/60,0)'voicetime',SUM(a.revenue)'revenue',ROUND((SUM(a.callmiss)/(SUM(a.callsuccess)+SUM(a.callmiss)))*100,2)'callrate' FROM report a WHERE a.createdtime >= '"+str(startdate)+"' and a.createdtime <= '"+str(enddate)+"' and a.partner_code REGEXP "+list_partner
                        df = pd.read_sql(sql,db)
                        json_records = df.to_json(orient ='records')
                        data1 = []
                        data1 = json.loads(json_records)
                    else :
                        sql_partner_code = "SELECT partner1,partner2,partner3 FROM agency where nickname ='"+str(nickname)+"'"
                        df_partnercode = pd.read_sql(sql_partner_code,db)
                        # lưu tất cả partner1 , partner2 , partner3 vào 1 list
                        list_partner1 = df_partnercode['partner1'].tolist()
                        list_partner2 = df_partnercode['partner2'].tolist()
                        list_partner3 = df_partnercode['partner3'].tolist()
                        list_partner = list_partner1 + list_partner2 + list_partner3
                        logging.info("list_partner : "+ str(list_partner))
                        list_partner = str(list_partner).replace("[","(").replace("]",")")
                        sql = "SELECT a.id,LEFT(a.createdtime,10)'date',(SUM(a.callsuccess)+SUM(a.callmiss))'calltotal',SUM(a.callsuccess)'callsuccess',SUM(a.callmiss) 'callmiss',ROUND(SUM(a.voicetime)/60,0)'voicetime',SUM(a.revenue)'revenue',a.vosIp,a.telco,CONCAT(ROUND((SUM(a.callmiss)/(SUM(a.callsuccess)+SUM(a.callmiss)))*100,2),' %')'callrate','SIP_TRUNK'`servicename` FROM report a WHERE a.createdtime >= '"+str(startdate)+"' and a.createdtime <= '"+str(enddate)+"' and a.partner01 = "+partner_code+" GROUP BY date"
                        df = pd.read_sql(sql,db)
                        json_records = df.to_json(orient ='records')
                        data = []
                        data = json.loads(json_records)
                        sql = "SELECT a.id,(SUM(a.callsuccess)+SUM(a.callmiss))'calltotal',SUM(a.callsuccess)'callsuccess',SUM(a.callmiss) 'callmiss',ROUND(SUM(a.voicetime)/60,0)'voicetime',SUM(a.revenue)'revenue',ROUND((SUM(a.callmiss)/(SUM(a.callsuccess)+SUM(a.callmiss)))*100,2)'callrate' FROM report a WHERE a.createdtime >= '"+str(startdate)+"' and a.createdtime <= '"+str(enddate)+"' and a.partner01 = "+partner_code
                        df = pd.read_sql(sql,db)
                        json_records = df.to_json(orient ='records')
                        data1 = []
                        data1 = json.loads(json_records)
                    context = {'data': data,'total': data1,'code': 'OK'}
                    db.close()
                    return context
    except Exception as e:
        logging.error(traceback.format_exc())
        return jsonify({'NOTOK': str(e)})

@report_exception.route('/saovang_findreport_customer', methods=['GET'])
def saovang_findreport_customer():
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
            partner = data["partner"]
            partner_code1 = data["partner_code"]
            starttime = request.args.get('starttime')
            endtime = request.args.get('endtime')
            if str(endtime) != "":
                endtime = str(endtime) + " 23:59:59"
            nickname = request.args.get('nickname')
            telco = request.args.get('telco')
            if len(starttime) == 0 :
                starttime = str(currentDate)[0:10]
            timestartquery = " and a.createdtime >= '"+ str(starttime) +"'"
            if len(endtime) == 0 :
                endtime = str(currentDate)[0:10]+" 23:59:59"
            timeendquery = " and a.createdtime <= '"+ str(endtime) +"'"
            if len(nickname) == 0 :
                partnerquery = ""
            else :
                sql_get_partner_code = "select partner_code from partner where nickname ='"+str(nickname)+"'"
                df_get_partner_code = pd.read_sql(sql_get_partner_code,db)
                partner_code = df_get_partner_code.iloc[0,0]
                partnerquery = " and a.partner_code like '%"+str(partner_code)+"%'"
            sql = timestartquery + timeendquery + partnerquery
            sql_partner_code = "SELECT partner_code from partner WHERE agency_id= 1"
            df_partnercode = pd.read_sql(sql_partner_code,db)
            # lưu tất cả partner1 , partner2 , partner3 vào 1 list
            list_partner = df_partnercode['partner_code'].tolist()
            logging.info("list_partner : "+ str(list_partner))
            list_partner = str(list_partner).replace("[","(").replace("]",")").replace(" ","")
            if len(str(nickname)) == 0:
                if str(group_name) == "CUSTOMER" :
                    sqlfind = "SELECT a.id,LEFT(a.createdtime,10)'createdtime',(SUM(a.callsuccess)+SUM(a.callmiss))'calltotal',SUM(a.callsuccess)'callsuccess',SUM(a.callmiss) 'callmiss',ROUND(SUM(a.voicetime)/60,0)'voicetime',SUM(a.revenue)'revenue',a.vosIp,b.nickname,a.telco,CONCAT(ROUND((SUM(a.callmiss)/(SUM(a.callsuccess)+SUM(a.callmiss)))*100,2),' %')'callrate',case when a.isBrand = '0' then 'BRAND' else 'SIP_TRUNK' end as servicename FROM report a,partner b WHERE a.partner_code like '%"+str(partner_code1)+"%' and RIGHT(a.partner_code,5) = b.partner_code and telco like '%"+str(telco)+"%'"+sql+" GROUP BY a.partner_code ,LEFT(a.createdtime,10)"
                else :
                    sqlfind = "SELECT a.id,LEFT(a.createdtime,10)'createdtime',(SUM(a.callsuccess)+SUM(a.callmiss))'calltotal',SUM(a.callsuccess)'callsuccess',SUM(a.callmiss) 'callmiss',ROUND(SUM(a.voicetime)/60,0)'voicetime',SUM(a.revenue)'revenue',a.vosIp,b.nickname,a.telco,CONCAT(ROUND((SUM(a.callmiss)/(SUM(a.callsuccess)+SUM(a.callmiss)))*100,2),' %')'callrate',case when a.isBrand = '0' then 'BRAND' else 'SIP_TRUNK' end as servicename FROM report a,partner b WHERE a.partner01 in "+list_partner+" and RIGHT(a.partner_code,5) = b.partner_code and telco like '%"+str(telco)+"%'"+sql+" GROUP BY a.partner_code ,LEFT(a.createdtime,10)"
            else :
                sqlfind = "SELECT a.id,LEFT(a.createdtime,10)'createdtime',(SUM(a.callsuccess)+SUM(a.callmiss))'calltotal',SUM(a.callsuccess)'callsuccess',SUM(a.callmiss) 'callmiss',ROUND(SUM(a.voicetime)/60,0)'voicetime',SUM(a.revenue)'revenue',a.vosIp,'"+str(nickname)+"'`nickname`,a.telco,CONCAT(ROUND((SUM(a.callmiss)/(SUM(a.callsuccess)+SUM(a.callmiss)))*100,2),' %')'callrate',case when a.isBrand = '0' then 'BRAND' else 'SIP_TRUNK' end as servicename FROM report a WHERE a.partner01 in "+list_partner+" and a.telco like '%"+str(telco)+"%'"+sql+" GROUP BY LEFT(a.createdtime,10)"
            df = pd.read_sql(sqlfind,db)
            df["createdtime"] = pd.to_datetime(df["createdtime"])
            df = df.sort_values(by=['createdtime'], ascending=False)
            df['createdtime'] = df['createdtime'].astype('str')
            logging.info(sqlfind)
            json_records = df.to_json(orient ='records')
            data = []
            data = json.loads(json_records)
            if str(group_name) == "CUSTOMER" :
                sqltotal = "SELECT a.id,LEFT(a.createdtime,10)'createdtime',(SUM(a.callsuccess)+SUM(a.callmiss))'calltotal',SUM(a.callsuccess)'callsuccess',SUM(a.callmiss) 'callmiss',ROUND(SUM(a.voicetime)/60,0)'voicetime',SUM(a.revenue)'revenue',ROUND((SUM(a.callmiss)/(SUM(a.callsuccess)+SUM(a.callmiss)))*100,2)'callrate' FROM report a WHERE a.partner_code like '%"+str(partner_code1)+"%' and a.telco like '%"+str(telco)+"%'"+sql
            else :
                sqltotal = "SELECT a.id,LEFT(a.createdtime,10)'createdtime',(SUM(a.callsuccess)+SUM(a.callmiss))'calltotal',SUM(a.callsuccess)'callsuccess',SUM(a.callmiss) 'callmiss',ROUND(SUM(a.voicetime)/60,0)'voicetime',SUM(a.revenue)'revenue',ROUND((SUM(a.callmiss)/(SUM(a.callsuccess)+SUM(a.callmiss)))*100,2)'callrate' FROM report a WHERE a.partner01 in "+list_partner+" and a.telco like '%"+str(telco)+"%'"+sql
            print(sqltotal)
            dftotal = pd.read_sql(sqltotal,db)
            json_recordstotal = dftotal.to_json(orient ='records')
            datatotal = []
            datatotal = json.loads(json_recordstotal)
            context = {'data': data,'total':datatotal,'code': 'OK'}
            db.close()
            return context
    except Exception as e:
        logging.error(traceback.format_exc())
        return jsonify({'NOTOK': 'khong co quyen'})

@report_exception.route('/saovang_find_report_calltype', methods=['GET'])
def saovang_find_report_calltype():
    logging.info("----------------------find_report_calltype-------------------------")
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
            partner_codes = data["partner_code"]
            logging.info("partner_code : "+ str(partner_codes))
            if True:
                currentDate = time.strftime("%Y-%m-%d %H:%M:%S")
                logging.info(currentDate)
                if str(timetoken) >= str(currentDate) :
                    logging.info(request.args)
                    starttime = request.args.get('starttime')
                    endtime = request.args.get('endtime')
                    if endtime != "" :
                        endtime = str(endtime) + " 23:59:59"
                    callType = request.args.get('callType')
                    nickname = request.args.get('nickname')
                    #check partner code
                    if str(nickname) != "" :
                        sql_partnercode = "SELECT partner_code FROM partner where nickname ='"+str(nickname)+"'"
                        df_partnercode = pd.read_sql(sql_partnercode,db)
                        partner_code =df_partnercode.iloc[0,0]
                    timestartquery = " a.dateTime >= '"+ str(starttime) +"'"
                    timeendquery = " and a.dateTime <= '"+ str(endtime) +"'"
                    if len(nickname) == 0 :
                        nicknamequery = ""
                    else :
                        nicknamequery = " and a.partnerCode = '"+str(partner_code)+"'"
                    if len(callType) == 0 :
                        callTypequery = ""
                    else :
                        callTypequery = " and a.callTypeId = (select id from service_type where callType = '"+str(callType)+"')"
                    sql = timestartquery + timeendquery + nicknamequery +callTypequery
                    if str(group_name) != "CUSTOMER":
                        sql_partner_code = "SELECT partner_code from partner WHERE agency_id= 1"
                        df_partnercode = pd.read_sql(sql_partner_code,db)
                        # lưu tất cả partner1 , partner2 , partner3 vào 1 list
                        list_partner = df_partnercode['partner_code'].tolist()
                        logging.info("list_partner : "+ str(list_partner))
                        list_partner = str(list_partner).replace("[","'").replace("]","'").replace(", ","|")
                        sqlfind = "SELECT a.id,a.dateTime, c.nickname, b.callType, round(a.totalDuration/60,0)'totalDuration',a.totalAmount,a.vosIp, CASE WHEN a.isBrand =0 THEN 'BRAND' ELSE 'SIP' END AS isBrand FROM service_partner a ,service_type b ,partner c WHERE a.partnerCode = c.partner_code and c.partner_code REGEXP "+str(list_partner)+" AND a.callTypeId =b.id and"+sql
                    else :
                        sqlfind = "SELECT a.id,a.dateTime, c.nickname, b.callType, round(a.totalDuration/60,0)'totalDuration',a.totalAmount,a.vosIp, CASE WHEN a.isBrand =0 THEN 'BRAND' ELSE 'SIP' END AS isBrand FROM service_partner a ,service_type b ,partner c WHERE a.partnerCode = c.partner_code AND a.callTypeId =b.id and a.partnerCode ='"+str(partner_codes)+"' and"+sql
                    logging.info("sqlfind : " +sqlfind)
                    df = pd.read_sql(sqlfind,db)
                    df["dateTime"] = pd.to_datetime(df["dateTime"])
                    df = df.sort_values(by=['dateTime'], ascending=False)
                    df['dateTime'] = df['dateTime'].astype('str')
                    json_records = df.to_json(orient ='records')
                    data = []
                    data = json.loads(json_records)
                    if str(group_name) != "CUSTOMER":
                        sqltotal = "SELECT sum(a.totalDuration)'totalDuration',sum(a.totalAmount),'totalAmount' FROM service_partner a  WHERE a.partnerCode REGEXP "+str(list_partner)+"and " +sql
                    else :
                        sqltotal = "SELECT sum(a.totalDuration)'totalDuration',sum(a.totalAmount),'totalAmount' FROM service_partner a  WHERE a.partnerCode ='"+str(partner_codes)+"' and "+sql
                    logging.info(sqltotal)
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
                return jsonify({'NOTOK': 'khong co quyen'}),400
    except Exception as e:
        logging.error(traceback.format_exc())
        return jsonify({'NOTOK': str(traceback.format_exc())}),400