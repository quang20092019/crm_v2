from flask import Blueprint
from flask import request, jsonify
import json
import logging
import requests
from requests.auth import HTTPDigestAuth
import pandas as pd
from pandas import json_normalize
import pymysql
from flask_jsonpify import jsonpify
from datetime import date, timedelta
import datetime
from function import group
import time
import re
import traceback
other = Blueprint('other', __name__)
@other.route('/listallcustomer', methods=['GET'])
def listallcustomer():
    db = pymysql.connect(host="172.17.0.1",    # your host, usually localhost
                     user="leeoncrm",         # your username
                     passwd="41XmKsO3NBgHPwv",  # your password
                     db="leeon_crm",
                     port=3306
                     )
    sql ="SELECT nickname FROM partner"
    df = pd.read_sql(sql,db)
    json_records = df.to_json(orient ='records')
    data = []
    data = json.loads(json_records)
    context = {'data': data,'code': 'OK'}
    return context
@other.route('/totalreport', methods=['GET'])
def totalreport():
    now = datetime.datetime.now()
    session= str(now).replace("-","").replace(":","").replace(".","").replace(" ","")
    logging.info(str(session)+"------------------------totalreport----------------------------")
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
            logging.info(str(session)+" | partner | "+ str(partner))
            logging.info(str(session)+" | partner_code | "+ str(partner_code))
            if str(partner) != "":
                partner= str(partner).replace("[","").replace("]","").replace("'","")
                partner =re.split('[,;/ ]+', partner)
                logging.info(str(session)+ "| partner | "+ str(partner))
                nickname=[]
                for i in partner:
                    sqlpartnerid = "select partner_code from partner where nickname ='"+str(i)+"'"
                    dfpartnerid = pd.read_sql(sqlpartnerid,db)
                    if not dfpartnerid.empty:
                        nickname.append(str(dfpartnerid.iloc[0,0]))
                logging.info(str(session)+ "| nickname | "+str(nickname))
            if True:
                currentDate = time.strftime("%Y-%m-%d %H:%M:%S")
                if str(timetoken) >= str(currentDate) :
                    if str(partner_code) == "None":
                        if str(partner) != "":
                            sql="select id, sum(calltotal)'calltotal',sum(callsuccess)'callsuccess',sum(callmiss)'callmiss',sum(voicetime),sum(revenue)'revenue',CONCAT(ROUND((SUM(callmiss)/(SUM(callsuccess)+SUM(callmiss)))*100,2),' %')'callrate' ,sum(voicetime)'voicetime' from ("
                            for i in nickname:
                                sql_join= "(SELECT a.id,LEFT(a.createdtime,10)'createdtime',(SUM(a.callsuccess)+SUM(a.callmiss))'calltotal',SUM(a.callsuccess)'callsuccess',SUM(a.callmiss) 'callmiss',ROUND(SUM(a.voicetime)/60,0)'voicetime',SUM(a.revenue)'revenue',a.vosIp,b.nickname,a.telco,CONCAT(ROUND((SUM(a.callmiss)/(SUM(a.callsuccess)+SUM(a.callmiss)))*100,2),' %')'callrate','SIP_TRUNK'`servicename` FROM report a,partner b WHERE a.createdtime >= CURDATE() AND a.partner_code like '%"+str(i)+"%' AND RIGHT(a.partner_code,5) = b.partner_code GROUP BY a.partner_code) union "
                                sql = sql + sql_join
                            sql = sql[:-6] + ") AS e"
                        else :
                            sql ="SELECT COALESCE(SUM(callmiss)+SUM(callsuccess),0)'calltotal',COALESCE(SUM(callmiss),0)'callmiss',COALESCE(SUM(callsuccess),0)'callsuccess',COALESCE(ROUND((SUM(callmiss)/(SUM(callmiss)+SUM(callsuccess)))*100,1),0)'callrate',COALESCE(SUM(voicetime)/60,0)'voicetime',COALESCE(SUM(revenue),0)'revenue' FROM report WHERE createdtime >= CURDATE()"
                    else :
                        sql ="SELECT COALESCE(SUM(callmiss)+SUM(callsuccess),0)'calltotal',COALESCE(SUM(callmiss),0)'callmiss',COALESCE(SUM(callsuccess),0)'callsuccess',COALESCE(ROUND((SUM(callmiss)/(SUM(callmiss)+SUM(callsuccess)))*100,1),0)'callrate',COALESCE(SUM(voicetime)/60,0)'voicetime',COALESCE(SUM(revenue),0)'revenue' FROM report WHERE createdtime >= CURDATE() AND partner_code like '%"+str(partner_code)+"%'"
                    logging.info(str(session)+ " |sql_total_report | "+str(sql))
                    df = pd.read_sql(sql,db)
                    # nếu ko có bản ghi
                    if str(df.iloc[0,0]) == 'None' :
                        json_records = df.to_json(orient ='records')
                        data = []
                        data = json.loads(json_records)
                        context = {'data': data,'code': 'OK'}
                        return context
                    else :
                        df['calltotal'] = df['calltotal'].astype('int')
                        df['callmiss'] = df['callmiss'].astype('int')
                        df['callsuccess'] = df['callsuccess'].astype('int')
                        df['voicetime'] = df['voicetime'].astype('int')
                        df['revenue'] = df['revenue'].astype('int')
                        json_records = df.to_json(orient ='records')
                        data = []
                        data = json.loads(json_records)
                        context = {'data': data,'code': 'OK'}
                        return context
                    return jsonify({'OK': 'Không có dữ liệu'}),400
                else :
                    return jsonify({'NOTOK': 'token hết hạn'})
            else :
                return jsonify({'NOTOK': 'khong co quyen'})
    except Exception as e:
        logging.error(str(session)+" | "+str(traceback.format_exc()))
        return jsonify({'NOTOK': str(traceback.format_exc())})
@other.route('/listallvos', methods=['GET'])
def listallvos():
    db = pymysql.connect(host="172.17.0.1",    # your host, usually localhost
                     user="leeoncrm",         # your username
                     passwd="41XmKsO3NBgHPwv",  # your password
                     db="leeon_crm",
                     port=3306
                     )
    sql ="SELECT id,ip,name from vos where status = '1'"
    df = pd.read_sql(sql,db)
    json_records = df.to_json(orient ='records')
    data = []
    data = json.loads(json_records)
    context = {'data': data,'code': 'OK'}
    return context
@other.route('/report', methods=['GET'])
def report():
    try :
        db = pymysql.connect(host="172.17.0.1",    # your host, usually localhost
                     user="leeoncrm",         # your username
                     passwd="41XmKsO3NBgHPwv",  # your password
                     db="leeon_crm",
                     port=3306
                     )
        record = json.loads(request.data)
        nickname = record["nickname"]
        if nickname == 'all' :
            sql= "SELECT a.createdtime,d.nickname ,b.name,c.ip,a.calltotal,a.callsuccess,a.callerror480,a.callerror503,a.voicetime,a.revenue FROM (SELECT * FROM report WHERE TYPE =0) a LEFT JOIN (SELECT * FROM mapping) b ON a.partnerroutemappingid = b.id LEFT JOIN (SELECT * FROM vos) c ON a.routevosid = c.id LEFT JOIN  (SELECT * FROM partner) d ON a.partnerid = d.id UNION ALL SELECT a.createdtime,d.nickname, b.name,c.ip,a.calltotal,a.callsuccess,a.callerror480,a.callerror503,a.voicetime,a.revenue FROM (SELECT * FROM report WHERE TYPE !=0) a LEFT JOIN (SELECT * FROM routing) b ON a.partnerroutemappingid = b.id LEFT JOIN  (SELECT * FROM vos) c ON a.routevosid = c.id LEFT JOIN  (SELECT * FROM partner) d ON a.partnerid = d.id ORDER BY createdtime"
            df = pd.read_sql(sql,db)
            df['createdtime'] = df['createdtime'].astype('str')
            json_records = df.to_json(orient ='records')
            data = []
            data = json.loads(json_records)
            context = {'data': data,'code': 'OK'}
            db.close()
            return context
        else  :
            sqlcheck ="select count(*) from partner where nickname = '"+str(nickname)+"'"
            df = pd.read_sql(sqlcheck,db)
            sl = df.iloc[0,0]
            print(sl)
            if int(sl) > 0 :
                sqlid ="select id from partner where nickname = '"+str(nickname)+"'"
                dfid = pd.read_sql(sqlid,db)
                id = dfid.iloc[0,0]
                sql= "SELECT a.createdtime,d.nickname ,b.name,c.ip,a.calltotal,a.callsuccess,a.callerror480,a.callerror503,a.voicetime,a.revenue FROM (SELECT * FROM report WHERE partnerid = '"+str(id)+"' AND TYPE =0) a LEFT JOIN (SELECT * FROM mapping) b ON a.partnerroutemappingid = b.id LEFT JOIN (SELECT * FROM vos) c ON a.routevosid = c.id LEFT JOIN  (SELECT * FROM partner) d ON a.partnerid = d.id UNION ALL SELECT a.createdtime,d.nickname, b.name,c.ip,a.calltotal,a.callsuccess,a.callerror480,a.callerror503,a.voicetime,a.revenue FROM (SELECT * FROM report WHERE partnerid = '"+str(id)+"' AND TYPE !=0) a LEFT JOIN (SELECT * FROM routing) b ON a.partnerroutemappingid = b.id LEFT JOIN  (SELECT * FROM vos) c ON a.routevosid = c.id LEFT JOIN  (SELECT * FROM partner) d ON a.partnerid = d.id ORDER BY createdtime"
                df = pd.read_sql(sql,db)
                df['createdtime'] = df['createdtime'].astype('str')
                json_records = df.to_json(orient ='records')
                data = []
                data = json.loads(json_records)
                context = {'data': data,'code': 'OK'}
                db.close()
                return context
            else :
                return jsonify({'Lỗi': 'Không có nickname này'})
    except :
        db.close()  
        return jsonify({'Lỗi': 'Quá nhiều lỗi'})

# select mapping
@other.route('/selectmapping', methods=['GET'])
def selectmapping():
        db = pymysql.connect(host="172.17.0.1",    # your host, usually localhost
                     user="leeoncrm",         # your username
                     passwd="41XmKsO3NBgHPwv",  # your password
                     db="leeon_crm",
                     port=3306
                     )
        
        ip = request.args.get('ip')
        telco = request.args.get('telco')
        sqltelco= "SELECT id from telco where name = '"+str(telco)+"'"
        df = pd.read_sql(sqltelco,db)
        id = df.iloc[0,0]
        sqlvos= "SELECT id from vos where ip = '"+str(ip)+"'"
        dfvos = pd.read_sql(sqlvos,db)
        idvos = dfvos.iloc[0,0]
        sql ="select name from mapping where vosid = '"+str(idvos)+"' and telcoid = '"+str(id)+"'"
        df = pd.read_sql(sql,db)
        json_records = df.to_json(orient ='records')
        data = []
        data = json.loads(json_records)
        context = {'data': data,'code': 'OK'}
        db.close()
        return context
@other.route('/selectrouting', methods=['GET'])
def selectrouting():
    try :
        db = pymysql.connect(host="172.17.0.1",    # your host, usually localhost
                     user="leeoncrm",         # your username
                     passwd="41XmKsO3NBgHPwv",  # your password
                     db="leeon_crm",
                     port=3306
                     )
    
        ip = request.args.get('ip')
        telco = request.args.get('telco')
        sqltelco= "SELECT id from telco where name = '"+str(telco)+"'"
        df = pd.read_sql(sqltelco,db)
        id = df.iloc[0,0]
        sqlvos= "SELECT id from vos where ip = '"+str(ip)+"'"
        dfvos = pd.read_sql(sqlvos,db)
        idvos = dfvos.iloc[0,0]
        sql ="select name from routing where vosid = '"+str(idvos)+"' and telcoid = '"+str(id)+"'"
        df = pd.read_sql(sql,db)
        json_records = df.to_json(orient ='records')
        data = []
        data = json.loads(json_records)
        context = {'data': data,'code': 'OK'}
        db.close()
        return context
    except :
        db.close()  
        return jsonify({'Lỗi': 'Quá nhiều lỗi'})
@other.route('/selectaccount', methods=['GET'])
def selectaccount():
        db = pymysql.connect(host="172.17.0.1",    # your host, usually localhost
                     user="leeoncrm",         # your username
                     passwd="41XmKsO3NBgHPwv",  # your password
                     db="leeon_crm",
                     port=3306
                     )
        
        ip = request.args.get('ip')
        print(ip)
        sqlvos= "SELECT id from vos where ip = '"+str(ip)+"'"
        dfvos = pd.read_sql(sqlvos,db)
        idvos = dfvos.iloc[0,0]
        print (idvos)
        sql ="select name from account where vosid = '"+str(idvos)+"'"
        print (sql)
        df = pd.read_sql(sql,db)
        json_records = df.to_json(orient ='records')
        data = []
        data = json.loads(json_records)
        context = {'data': data,'code': 'OK'}
        db.close()
        return context
@other.route('/search', methods=['GET'])
def search():
        db = pymysql.connect(host="172.17.0.1",    # your host, usually localhost
                     user="leeoncrm",         # your username
                     passwd="41XmKsO3NBgHPwv",  # your password
                     db="leeon_crm",
                     port=3306
                     )
        
        starttime = request.args.get('starttime')
        endtime = request.args.get('endtime')
        nickname = request.args.get('nickname')
        account = request.args.get('account')
        print(account)
        # if len(starttime) == 0 :
        #     timestartquery = ""
        # else :
        #     timestartquery = "createdtime >= '"+ str(starttime) +"' and "
        # if len(endtime) == 0 :
        #     timeendquery = ""
        # else :
        #     timeendquery = "createdtime <= '"+ str(endtime) +"' and "
        # if len(nickname) == 0 :
        #     nicknamequery = ""
        # else :
        #     nicknamequery = "partnerid = "+ str(nickname)
        # if len(account) == 0 :
        #     accountquery = ""
        # else :
        #     accountquery = " and partneraccount = "+ str(account)
        return jsonify({'Lỗi': 'Quá nhiều lỗi'})
@other.route('/baocaocuocgoi', methods=['GET'])
def baocaocuocgoi():
        db = pymysql.connect(host="127.0.0.1",    # your host, usually localhost
                     user="syncdbvos",         # your username
                     passwd="KJksjooii8998899",  # your password
                     db="vos_cdr",
                     port=3306
                     )
        
        sql="select * from report_cdr order by duration, tongcall desc"
        df = pd.read_sql(sql,db)
        json_records = df.to_json(orient ='records')
        data = []
        data = json.loads(json_records)
        sqlerr="select * from report_cdr where duration = 0 order by tongcall desc"
        dferr = pd.read_sql(sqlerr,db)
        json_recordserr = dferr.to_json(orient ='records')
        dataerr = []
        dataerr = json.loads(json_recordserr)
        context = {'data': data,'dataerr': dataerr}
        return context
@other.route('/baocaonoimang', methods=['GET'])
def baocaonoimang():
        db = pymysql.connect(host="127.0.0.1",    # your host, usually localhost
                     user="syncdbvos",         # your username
                     passwd="KJksjooii8998899",  # your password
                     db="vos_cdr",
                     port=3306
                     )
        
        date_object = datetime.date.today()- timedelta(days=1)
        print(date_object)
        ngay = ''.join(char for char in str(date_object) if char.isalnum())
        print(ngay)
        thang = str(ngay)[0:6]
        print(thang)
        sql="select a.id,a.ten `ten`, round((b.totaltime/60),2)`time`,round((a.totaltimemonth/60),2) `timemonth` from (select id,name `ten` , sum(tongthoigiangoi)`totaltimemonth`,sum(socuocgoi) `totalcallmonth` from calleeduyen_"+thang+" where time <= '"+str(date_object)+"' and name is not null group by name) a left join (select name `ten` , sum(tongthoigiangoi)`totaltime`,sum(socuocgoi) `totalcall` from calleeduyen_"+thang+" where time ='"+str(date_object)+"' and name is not null group by name) b on a.ten =b.ten"
        print(sql)
        df = pd.read_sql(sql,db)
        json_records = df.to_json(orient ='records')
        data = []
        data = json.loads(json_records)
        context = {'data': data}
        return context
@other.route('/baocaonoimangngay', methods=['GET'])
def baocaonoimangngay():
        db = pymysql.connect(host="127.0.0.1",    # your host, usually localhost
                     user="syncdbvos",         # your username
                     passwd="KJksjooii8998899",  # your password
                     db="vos_cdr",
                     port=3306
                     )
        
        datefrom = request.args.get('starttime')
        dateto = request.args.get('endtime')
        thang = str(datefrom)[0:7].replace("-", "")
        print(thang)
        sql="select id,name `ten` , sum(tongthoigiangoi)/60`totaltime`,sum(socuocgoi) `totalcall` from calleeduyen_"+thang+" where time >='"+datefrom+"' and time <='"+dateto+"' and name is not null group by name"
        df = pd.read_sql(sql,db)
        json_records = df.to_json(orient ='records')
        data = []
        data = json.loads(json_records)
        context = {'data': data}
        return context
@other.route('/saovang_totalreport', methods=['GET'])
def saovang_totalreport():
    now = datetime.datetime.now()
    session= str(now).replace("-","").replace(":","").replace(".","").replace(" ","")
    logging.info(str(session)+"------------------------totalreport----------------------------")
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
            if str(group_name) == "CUSTOMER":
                logging.info(str(session)+" | partner_code | "+ str(partner_code))
                sql ="SELECT COALESCE(SUM(callmiss)+SUM(callsuccess),0)'calltotal',COALESCE(SUM(callmiss),0)'callmiss',COALESCE(SUM(callsuccess),0)'callsuccess',COALESCE(ROUND((SUM(callmiss)/(SUM(callmiss)+SUM(callsuccess)))*100,1),0)'callrate',COALESCE(SUM(voicetime)/60,0)'voicetime',COALESCE(SUM(revenue),0)'revenue' FROM report WHERE createdtime >= CURDATE() AND partner_code like '%"+str(partner_code)+"%'"
            else :
                sql_partner_code = "SELECT partner_code from partner WHERE agency_id= 1"
                df_partnercode = pd.read_sql(sql_partner_code,db)
                # lưu tất cả partner1 , partner2 , partner3 vào 1 list
                list_partner = df_partnercode['partner_code'].tolist()
                logging.info("list_partner : "+ str(list_partner))
                list_partner = str(list_partner).replace("[","'").replace("]","'").replace(", ","|")
                sql ="SELECT COALESCE(SUM(callmiss)+SUM(callsuccess),0)'calltotal',COALESCE(SUM(callmiss),0)'callmiss',COALESCE(SUM(callsuccess),0)'callsuccess',COALESCE(ROUND((SUM(callmiss)/(SUM(callmiss)+SUM(callsuccess)))*100,1),0)'callrate',COALESCE(SUM(voicetime)/60,0)'voicetime',COALESCE(SUM(revenue),0)'revenue' FROM report WHERE createdtime >= CURDATE() AND partner_code REGEXP "+str(list_partner)
            logging.info(str(session)+ " |sql_total_report | "+str(sql))
            df = pd.read_sql(sql,db)
            # nếu ko có bản ghi
            if str(df.iloc[0,0]) == 'None' :
                json_records = df.to_json(orient ='records')
                data = []
                data = json.loads(json_records)
                context = {'data': data,'code': 'OK'}
                return context
            else :
                df['calltotal'] = df['calltotal'].astype('int')
                df['callmiss'] = df['callmiss'].astype('int')
                df['callsuccess'] = df['callsuccess'].astype('int')
                df['voicetime'] = df['voicetime'].astype('int')
                df['revenue'] = df['revenue'].astype('int')
                json_records = df.to_json(orient ='records')
                data = []
                data = json.loads(json_records)
                context = {'data': data,'code': 'OK'}
                return context
    except Exception as e:
        logging.error(str(session)+" | "+str(traceback.format_exc()))
        return jsonify({'NOTOK': str(traceback.format_exc())})