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
import datetime
import os
from function import group
import time
import numpy as np
import traceback
number_member = Blueprint('number_member', __name__)
@number_member.route('/listnumber_member', methods=['GET'])
def listnumber_member():
    try :
        logging.info("-----------------------listnumber_member-----------------------")
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
            nameuser = data["user"]
            if True:
                currentDate = time.strftime("%Y-%m-%d %H:%M:%S")
                print(currentDate)
                if str(timetoken) >= str(currentDate) :
                    sql ="SELECT a.company_owner,a.cancel_of_date,f.name'brandname',a.initialization_fee,a.maintaining_fee,a.level_number,a.nice_number_maintain_fee,a.blocking_date_1st,a.blocking_date_2nd,a.end_of_date,a.id,a.isdn,case when a.isdntype = '1' then 'Sip Trunk' when a.isdntype ='2' then 'Proconnect' when a.isdntype = '3' then '3C' when a.isdntype = '4' then 'Digisip' else '' end as isdntype,a.note,a.dateassignedtoleeon,a.dateassignedtopartner,a.cfu,b.name'vosip',c.nickname,d.name'telco',e.isdn'numberowner',a.status FROM number_member a left join (select * from vos) b on a.vosip=b.id left join (select * from partner) c on a.partnerid=c.id left join (select * from telco) d on a.telcoid=d.id left join (select * from number_owner) e on a.numberownerid=e.id left join (select * from brandname) f on a.brandnameid=f.id limit 1234"
                    logging.info(sql)
                    df = pd.read_sql(sql,db)
                    df = df.astype(object).mask(df.isna(), np.nan)
                    df = df.fillna("")
                    df = df.replace("0000-00-00 00:00:00","")
                    df['cancel_of_date'] = df['cancel_of_date'].astype('str')
                    df['dateassignedtoleeon'] = df['dateassignedtoleeon'].astype('str')
                    df['dateassignedtopartner'] = df['dateassignedtopartner'].astype('str')
                    # lấy 10 ký tự đầu của cột dateassignedtopartner
                    df['dateassignedtopartner'] = df['dateassignedtopartner'].str[:10]
                    df['end_of_date'] = df['end_of_date'].astype('str')
                    json_records = df.to_json(orient ='records')
                    data = []
                    data = json.loads(json_records)
                    context = {'data': data,'code': 'OK'}
                    sqllog = "insert into log (name,api,content,type) value ('"+str(nameuser)+"','listnumber_member','view','SELECT')"
                    cursor = db.cursor()
                    cursor.execute(sqllog)
                    db.commit()
                    db.close()
                    logging.info("success")
                    return context
                else :
                    return jsonify({'NOTOK': 'token hết hạn'})
            else :
                return jsonify({'NOTOK': 'khong co quyen'})
    except Exception as e:
        db.close()
        print(str(e))
        return jsonify({'Lỗi': 'Quá nhiều lỗi'})
@number_member.route('/deletenumber_member', methods=['DELETE'])
def deletenumber_member():
    logging.info("------------------delete_number_member------------------------")
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
            print(timetoken)
            print(group_name)
            if True:
                currentDate = time.strftime("%Y-%m-%d %H:%M:%S")
                print(currentDate)
                if str(timetoken) >= str(currentDate) :
                    record = json.loads(request.data)
                    logging.info(record)
                    id = record["id"]
                    ids = str(id).replace("[","").replace("]","").replace(" ","").split(",")
                    for x in range(len(ids)):
                        print(ids[x])
                        sqlcheck= "select count(*) from number_member where id ='"+ids[x]+"'"
                        df = pd.read_sql(sqlcheck,db)
                        sl = df.iloc[0,0]
                        if int(sl) > 0 :
                            sql="delete from number_member where id = '"+str(ids[x])+"'"
                            logging.info(sql)
                            cursor = db.cursor()
                            cursor.execute(sql)
                            sqllog = "insert into log (name,api,content,type) value ('"+str(nameuser)+"','deletenumber_member','id = "+str(ids[x])+"','DELETE')"
                            cursor = db.cursor()
                            cursor.execute(sqllog)
                            db.commit()
                        else :
                            db.close() 
                            return jsonify({'OK': 'id không tồn tại'})
                    return jsonify({'OK': 'OK'})    
                else :
                    return jsonify({'NOTOK': 'token hết hạn'})
            else :
                return jsonify({'NOTOK': 'khong co quyen'})
    except Exception as e:
        db.close()
        print(str(e))
        return jsonify({'Lỗi': 'Quá nhiều lỗi'})
@number_member.route('/insertnumber_member', methods=['POST'])
def insertnumber_member():
    logging.info("---------------------insertnumber_member--------------------")
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
            if True:
                currentDate = time.strftime("%Y-%m-%d %H:%M:%S")
                print(currentDate)
                if str(timetoken) >= str(currentDate) :
                    record = json.loads(request.data)
                    logging.info("input" +str(record))
                    isdn = record["isdn"]
                    # lấy thông tin vos
                    try:
                        vosip = record["vosip"]
                        sqlvos = "select id from vos where name = '"+str(vosip)+"'"
                        df = pd.read_sql(sqlvos,db)
                        print(df)
                        if df.empty:
                            vosip= ""
                        else :
                            vosip = df.iloc[0,0]
                    except Exception as e:
                        vosip = ""
                    # lấy thông tin partner
                    try :
                        partner = record["nickname"]
                        sqlpartner = "select id,partner_code from partner where nickname = '"+str(partner)+"'"
                        df = pd.read_sql(sqlpartner,db)
                        partnerid = df.iloc[0,0]
                        partner_code= df.iloc[0,1]
                    except Exception as e:
                        partnerid = ""
                        partner_code= ""
                    # lấy thông tin telco
                    try :
                        telco = record["telco"]
                    except Exception as e:
                        return jsonify({'NOTOK': 'Trường nhà mạng chưa điền'}),400
                    sqltelco = "select id from telco where name = '"+str(telco)+"'"
                    df = pd.read_sql(sqltelco,db)
                    telcoid= df.iloc[0,0]
                    brandname = record["brandname"]
                    sqlbrandname = "select id from brandname where name = '"+str(brandname)+"'"
                    df = pd.read_sql(sqlbrandname,db)
                    if df.empty:
                        brandnameid= ""
                    else :
                        brandnameid= df.iloc[0,0]
                    # lấy thông tin numberowner
                    initialization_fee= record["initialization_fee"]
                    maintaining_fee = record["maintaining_fee"]
                    level_number = record["level_number"]
                    end_of_date = record["end_of_date"]
                    numberowner = record["numberowner"]
                    company_owner= record["company_owner"]
                    nice_number_maintain_fee = record["nice_number_maintain_fee"]
                    # blocking_date_1st = record["blocking_date_1st"]
                    # blocking_date_2nd = record["blocking_date_2nd"]
                    sqlnumberowner = "select id from number_owner where isdn = '"+str(numberowner)+"'"
                    df = pd.read_sql(sqlnumberowner,db)
                    if df.empty :
                        numberownerid= ""
                    else :
                        numberownerid= df.iloc[0,0]
                    isdntype= record["isdntype"]
                    if str(isdntype)== "Siptrunk":
                        isdntype = 1
                    elif str(isdntype)== "Proconnect":
                        isdntype = 2
                    elif str(isdntype)== "3C":
                        isdntype = 3
                    else :
                        isdntype = 0
                    note= record["note"]
                    status = record["status"]
                    if str(status) == "0":
                        action = "0"
                        fee = 0
                    else :
                        action = "1"
                        fee = initialization_fee
                    fee = initialization_fee
                    dateassignedtoleeon= record["dateassignedtoleeon"]
                    dateassignedtopartner= record["dateassignedtopartner"]
                    now = datetime.datetime.now()
                    current_time = now.strftime("%Y-%m-%d %H:%M:%S")
                    isdn = isdn.split(",")
                    for i in isdn :
                        #check idns đã tồn tại chưa
                        sqlcheck ="select * from number_member where isdn = '"+str(i)+"'"
                        df = pd.read_sql(sqlcheck,db)
                        if df.empty:
                            sql="insert into number_member(brandnameid,isdn,vosip,partnerid,telcoid,numberownerid,status,createdtime,updatedtime,isdntype,note,dateassignedtoleeon,dateassignedtopartner,partner_code,initialization_fee,maintaining_fee,level_number,end_of_date,nice_number_maintain_fee,company_owner) value ('"+str(brandnameid)+"','"+str(i)+"','"+str(vosip)+"','"+str(partnerid)+"','"+str(telcoid)+"','"+str(numberownerid)+"','"+str(status)+"','"+str(current_time)+"','"+str(current_time)+"','"+str(isdntype)+"','"+str(note)+"','"+str(dateassignedtoleeon)+"','"+str(dateassignedtopartner)+"','"+str(partner_code)+"','"+str(initialization_fee)+"','"+str(maintaining_fee)+"','"+str(level_number)+"','"+str(end_of_date)+"','"+str(nice_number_maintain_fee)+"','"+str(company_owner)+"')"
                            sql_log="insert into number_log(fee,action,brandname_id,isdn,vos_id,telco_id,number_owner_id,isdnt_ype,note,date_assigned_to_partner,partner_code,level_number,end_of_date,user_action) value ('"+str(fee)+"','"+str(action)+"','"+str(brandnameid)+"','"+str(i)+"','"+str(vosip)+"','"+str(telcoid)+"','"+str(numberownerid)+"','"+str(isdntype)+"','"+str(note)+"','"+str(dateassignedtopartner)+"','"+str(partner_code)+"','"+str(level_number)+"','"+str(end_of_date)+"','"+str(nameuser)+"')"
                            logging.info(sql)
                            cursor = db.cursor()
                            cursor.execute(sql)
                            cursor.execute(sql_log)
                            db.commit()
                        else :
                            logging.info("isdn đã tồn tại")
                            return jsonify({'NOTOK': 'isdn đã tồn tại'}),400
                    return jsonify({'OK': 'OK'})
                    # sqllog = "insert into log (name,api,content,type) value ('"+str(nameuser)+"','insertnumber_member','"+str(record).replace("'","")+"','INSERT')"
                    # cursor = db.cursor()
                    # cursor.execute(sqllog)
                    # db.commit()
                else :
                    return jsonify({'NOTOK': 'token hết hạn','code':401}),401
            else :
                return jsonify({'NOTOK': 'khong co quyen'})  
    except Exception as e:
        logging.error(str(traceback.format_exc()))
        return jsonify({'Lỗi': str(traceback.format_exc())}),400
@number_member.route('/updatenumber_member', methods=['POST'])
def updatenumber_member():
    logging.info("----------------------updatenumber_member--------------------")
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
            print(timetoken)
            print(group_name)
            if True:
                currentDate = time.strftime("%Y-%m-%d %H:%M:%S")
                print(currentDate)
                if str(timetoken) >= str(currentDate) :
                    record = json.loads(request.data)
                    id = record["id"]
                    isdn = record["isdn"]
                    # lấy thông tin vos
                    vosip = record["vosip"]
                    sqlvos = "select id from vos where name = '"+str(vosip)+"'"
                    df = pd.read_sql(sqlvos,db)
                    print(df)
                    if df.empty:
                        vosip= ""
                    else :
                        vosip = df.iloc[0,0]
                    # lấy thông tin partner
                    # lấy thông tin partner
                    try :
                        partner = record["nickname"]
                        sqlpartner = "select id,partner_code from partner where nickname = '"+str(partner)+"'"
                        df = pd.read_sql(sqlpartner,db)
                        partnerid = df.iloc[0,0]
                        partner_code= df.iloc[0,1]
                    except Exception as e:
                        partnerid = ""
                        partner_code= ""
                    # lấy thông tin telco
                    telco = record["telco"]
                    sqltelco = "select id from telco where name = '"+str(telco)+"'"
                    df = pd.read_sql(sqltelco,db)
                    telcoid= df.iloc[0,0]
                    # lấy thông tin numberowner
                    numberowner = record["numberowner"]
                    sqlnumberowner = "select id from number_owner where isdn = '"+str(numberowner)+"'"
                    df = pd.read_sql(sqlnumberowner,db)
                    if df.empty:
                        numberownerid= ""
                    else :
                        numberownerid= df.iloc[0,0]
                    initialization_fee= record["initialization_fee"]
                    maintaining_fee = record["maintaining_fee"]
                    level_number = record["level_number"]
                    end_of_date = record["end_of_date"]
                    brandname = record["brandname"]
                    company_owner = record["company_owner"]
                    sqlbrandname = "select id from brandname where name = '"+str(brandname)+"'"
                    df = pd.read_sql(sqlbrandname,db)
                    if df.empty:
                        brandnameid= ""
                    else :
                        brandnameid= df.iloc[0,0]
                    status = record["status"]
                    if str(status) == "2":
                        action = "4"
                        fee = initialization_fee
                    elif str(status) == "3":
                        action = "5"
                        fee = initialization_fee
                    elif str(status) == "0":
                        action = "0"
                        fee = 0
                    elif str(status) == "4":
                        action = "3"
                        fee = maintaining_fee
                    elif str(status) == "5":
                        action = "3"
                        fee = maintaining_fee
                    else :
                        action = "1"
                        fee = initialization_fee
                    isdntype= record["isdntype"]
                    if str(isdntype)== "Siptrunk":
                        isdntype = 1
                    elif str(isdntype)== "Proconnect":
                        isdntype = 2
                    elif str(isdntype)== "3C":
                        isdntype = 3
                    else :
                        isdntype = 0
                    note= record["note"]
                    dateassignedtoleeon= record["dateassignedtoleeon"]
                    dateassignedtopartner= record["dateassignedtopartner"]
                    nice_number_maintain_fee = record["nice_number_maintain_fee"]
                    blocking_date_1st = record["blocking_date_1st"]
                    blocking_date_2nd = record["blocking_date_2nd"]
                    cancel_of_date = record["cancel_of_date"]
                    now = datetime.datetime.now()
                    current_time = now.strftime("%Y-%m-%d %H:%M:%S")
                    sqlcheck= "select status from number_member where id ='"+str(id)+"'"
                    df = pd.read_sql(sqlcheck,db)
                    if not df.empty:
                        status_old = df.iloc[0,0]
                        cursor = db.cursor()
                        if str(status) == "4" or str(status) == "5":
                            sql="update number_member set isdn = '"+str(isdn)+"',vosip = '"+str(vosip)+"',partnerid = '"+str(partnerid)+"',telcoid = '"+str(telcoid)+"',numberownerid = '"+str(numberownerid)+"',status = '"+str(status)+"',updatedtime='"+str(current_time)+"',brandnameid='"+str(brandnameid)+"',isdntype='"+str(isdntype)+"',note='"+str(note)+"',dateassignedtoleeon='"+str(dateassignedtoleeon)+"',dateassignedtopartner='"+str(dateassignedtopartner)+"',initialization_fee = '"+str(initialization_fee)+"',maintaining_fee = '"+str(maintaining_fee)+"',level_number = '"+str(level_number)+"',end_of_date = '"+str(end_of_date)+"',nice_number_maintain_fee = '"+str(nice_number_maintain_fee)+"',blocking_date_1st ='"+str(blocking_date_1st)+"',blocking_date_2nd = '"+str(blocking_date_2nd)+"',cancel_of_date = '"+cancel_of_date+"',company_owner = '"+str(company_owner)+"' where id ='"+str(id)+"'"
                            cursor.execute(sql)
                            sql_log="insert into number_log(fee,action,brandname_id,isdn,vos_id,telco_id,number_owner_id,isdnt_ype,note,date_assigned_to_partner,partner_code,level_number,end_of_date,user_action,cancel_of_date) value ('0','"+str(action)+"','"+str(brandnameid)+"','"+str(isdn)+"','"+str(vosip)+"','"+str(telcoid)+"','"+str(numberownerid)+"','"+str(isdntype)+"','"+str(note)+"','"+str(dateassignedtopartner)+"','"+str(partner_code)+"','"+str(level_number)+"','"+str(end_of_date)+"','"+str(nameuser)+"','"+str(cancel_of_date)+"')" 
                            logging.info(sql_log)
                            cursor.execute(sql_log)
                            db.commit()
                            sql_log="insert into number_log(fee,action,brandname_id,isdn,vos_id,telco_id,number_owner_id,isdnt_ype,note,date_assigned_to_partner,partner_code,level_number,end_of_date,user_action,cancel_of_date) value ('"+str(fee)+"','2','"+str(brandnameid)+"','"+str(isdn)+"','"+str(vosip)+"','"+str(telcoid)+"','"+str(numberownerid)+"','"+str(isdntype)+"','"+str(note)+"','"+str(dateassignedtopartner)+"','"+str(partner_code)+"','"+str(level_number)+"','"+str(end_of_date)+"','"+str(nameuser)+"','"+str(cancel_of_date)+"')" 
                            logging.info(sql_log)
                            cursor.execute(sql_log)
                            db.commit()
                        else :
                            sql="update number_member set isdn = '"+str(isdn)+"',vosip = '"+str(vosip)+"',partnerid = '"+str(partnerid)+"',telcoid = '"+str(telcoid)+"',numberownerid = '"+str(numberownerid)+"',status = '"+str(status)+"',updatedtime='"+str(current_time)+"',brandnameid='"+str(brandnameid)+"',isdntype='"+str(isdntype)+"',note='"+str(note)+"',dateassignedtoleeon='"+str(dateassignedtoleeon)+"',dateassignedtopartner='"+str(dateassignedtopartner)+"',initialization_fee = '"+str(initialization_fee)+"',maintaining_fee = '"+str(maintaining_fee)+"',level_number = '"+str(level_number)+"',end_of_date = '"+str(end_of_date)+"',nice_number_maintain_fee = '"+str(nice_number_maintain_fee)+"',blocking_date_1st ='"+str(blocking_date_1st)+"',blocking_date_2nd = '"+str(blocking_date_2nd)+"' where id ='"+str(id)+"'"
                            cursor.execute(sql)
                            sql_log="insert into number_log(fee,action,brandname_id,isdn,vos_id,telco_id,number_owner_id,isdnt_ype,note,date_assigned_to_partner,partner_code,level_number,end_of_date,user_action) value ('"+str(fee)+"','"+str(action)+"','"+str(brandnameid)+"','"+str(isdn)+"','"+str(vosip)+"','"+str(telcoid)+"','"+str(numberownerid)+"','"+str(isdntype)+"','"+str(note)+"','"+str(dateassignedtopartner)+"','"+str(partner_code)+"','"+str(level_number)+"','"+str(end_of_date)+"','"+str(nameuser)+"')" 
                            logging.info(sql_log)
                            cursor.execute(sql_log)
                            db.commit()
                        sqllog = "insert into log (name,api,content,type) value ('"+str(nameuser)+"','updatenumber_member','"+str(record).replace("'","")+"','UPDATE')"
                        cursor = db.cursor()
                        cursor.execute(sqllog)
                        db.commit()
                        return jsonify({'OK': 'OK'})
                    else :
                        return jsonify({'OK': 'id không tồn tại'})
                    db.close()
                else :
                    return jsonify({'NOTOK': 'token hết hạn','code':401}),401
            else :
                return jsonify({'NOTOK': 'khong co quyen'})  
    except Exception as e:
        db.close()
        logging.error(str(traceback.format_exc()))
        return jsonify({'Lỗi': str(e)}),400
@number_member.route('/downloadnumber_member', methods=['POST'])
def downloadnumber_member():
        db = pymysql.connect(host="172.17.0.1",    # your host, usually localhost
                     user="leeoncrm",         # your username
                     passwd="41XmKsO3NBgHPwv",  # your password
                     db="leeon_crm",
                     port=3306
                     )
        print(request.data)
        record = json.loads(request.data)
        print(record)
        # isdn = request.args.get('isdn')
        # telcoid = request.args.get('telcoid')
        # partnerid = request.args.get('partnerid')
        isdn = record["isdn"]
        telcoid= record["telcoid"]
        partnerid= record["partnerid"]
        status = record["status"]
        try:
            if str(isdn)=="" :
                isdn_query=""
            else:
                isdn_query="isdn = '"+str(isdn) +"' and "
            if str(telcoid)=="" :
                telcoid_query=""
            else:
                telcoid_query="telcoid = '"+str(telcoid) +"' and "
            if str(status)=="" :
                status_query=""
            else:
                status_query="status = '"+str(status) +"' and "
            if str(partnerid)=="" :
                partnerid_query=""
            else:
                partnerid_query="partnerid = '"+str(partnerid)+"' and "
            if str(isdn)=="" and str(telcoid)=="" and str(partnerid)=="" and str(status)=="":
                query= ""
            else :
                query= "where "
            query = query+isdn_query+telcoid_query+status_query+partnerid_query+"id is not null"
            sql ="SELECT f.name'brandname',a.isdn,case when a.isdntype = '1' then 'Sip Trunk' when a.isdntype ='2' then 'Proconnect' when a.isdntype = '3' then '3C' else '' end as isdntype,a.note,a.dateassignedtoleeon,a.dateassignedtopartner,a.cfu,b.name'vosip',c.nickname,d.name'telco',e.isdn'numberowner',case when a.status = '1'  then 'Hoạt động' when a.status = '2'  then 'Khóa lần 1' when a.status = '3'  then 'Khóa lần 2' end as status FROM (select * from number_member "+str(query)+") a left join (select * from vos) b on a.vosip=b.id left join (select * from partner) c on a.partnerid=c.id left join (select * from telco) d on a.telcoid=d.id left join (select * from number_owner) e on a.numberownerid=e.id left join (select * from brandname) f on a.brandnameid=f.id"
            df = pd.read_sql(sql,db)
        except Exception as e:
            return jsonify({'NOTOK': str(e)})
        df = pd.read_sql(sql,db)
        session=datetime.datetime.now()
        filename = "number_member_"+str(session)+".csv"
        path = os.path.join(os.getcwd(),'filedownload',filename)
        df.to_csv(path,encoding='utf-8-sig')
        json_records = df.to_json(orient ='records')
        data = []
        data = json.loads(json_records)
        context = {'data': data,'code': 'OK'}
        db.close()
        return send_file(path, as_attachment=True)
@number_member.route('/find_number_member', methods=['GET'])
def find_number_member():
    logging.info("----------------find_number_member----------------")
    try:
        db = pymysql.connect(host="172.17.0.1",    # your host, usually localhost
                        user="leeoncrm",         # your username
                        passwd="41XmKsO3NBgHPwv",  # your password
                        db="leeon_crm",
                        port=3306
                        )
        isdn = request.args.get('isdn')
        telcoid = request.args.get('telcoid')
        partnerid = request.args.get('partnerid')
        status = request.args.get("status")
        if str(status)=="" :
            status_query=""
        else:
            status_query="status = '"+str(status) +"' and "
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
        # if str(isdn)=="" and str(telcoid)=="" and str(partnerid)=="" and str(status):
        #     query= ""
        # else :
        #     query= "where "
        query = status_query+isdn_query+telcoid_query+partnerid_query+"id is not null"
        sql ="SELECT a.company_owner,a.cancel_of_date,f.name'brandname',a.id,a.initialization_fee,a.maintaining_fee,a.level_number,a.end_of_date,a.nice_number_maintain_fee,a.blocking_date_1st,a.blocking_date_2nd,a.isdn,case when a.isdntype = '1' then 'Sip Trunk' when a.isdntype ='2' then 'Proconnect' when a.isdntype = '3' then '3C' else '' end as isdntype,a.note,a.dateassignedtoleeon,a.dateassignedtopartner,a.cfu,b.name'vosip',c.nickname,d.name'telco',e.isdn'numberowner',a.status FROM (select * from number_member where "+str(query)+") a left join (select * from vos) b on a.vosip=b.id left join (select * from partner) c on a.partnerid=c.id left join (select * from telco) d on a.telcoid=d.id left join (select * from number_owner) e on a.numberownerid=e.id left join (select * from brandname) f on a.brandnameid=f.id"
        logging.info(sql)
        df = pd.read_sql(sql,db)
        df = df.fillna("")
        df = df.replace("0000-00-00 00:00:00","")
        df['dateassignedtoleeon'] = df['dateassignedtoleeon'].astype('str')
        df['cancel_of_date'] = df['cancel_of_date'].astype('str')
        df['dateassignedtopartner'] = df['dateassignedtopartner'].astype('str')
        df['dateassignedtopartner'] = df['dateassignedtopartner'].str[:10]
        df['end_of_date'] = df['end_of_date'].astype('str')
        json_records = df.to_json(orient ='records')
        data = []
        data = json.loads(json_records)
        context = {'data': data,'code': 'OK'}
        db.close() 
        return context
    except Exception as e:
        logging.error(traceback.format_exc())
        return jsonify({'NOTOK': str(e)})
@number_member.route('/update_many_number_member', methods=['POST'])
def update_many_number_member():
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
            print(timetoken)
            print(group_name)
            if True:
                currentDate = time.strftime("%Y-%m-%d %H:%M:%S")
                print(currentDate)
                if str(timetoken) >= str(currentDate) :
                    record = json.loads(request.data)
                    isdn = record["isdn"]  
                    # get thông tin
                    sql_info = "select * from number_member where isdn = '"+str(isdn)+"'"
                    df_info = pd.read_sql(sql_info,db)
                    if str(isdn) == "" :
                        return jsonify({'NOTOK': "isdn khong duoc de trong"}),400
                    blocking_date_1st = record["blocking_date_1st"]
                    if str(blocking_date_1st) == "" :
                        sql_blocking_date_1st = ""
                    else :
                        sql_blocking_date_1st = ",blocking_date_1st = '"+str(blocking_date_1st)+"'"
                    blocking_date_2nd = record["blocking_date_2nd"]
                    if str(blocking_date_2nd) == "" :
                        sql_blocking_date_2nd = ""
                    else :
                        sql_blocking_date_2nd = ",blocking_date_2nd = '"+str(blocking_date_2nd)+"'"
                    brandname = record["brandname"]
                    if str(brandname) == "" :
                        sql_brandname = ""
                        sql_log_brandname_id = ""
                    else :
                        sqlbrandname = "select id from brandname where name = '"+str(brandname)+"'"
                        dfbrandname = pd.read_sql(sqlbrandname,db)
                        if dfbrandname.empty:
                            sql_brandname= ""
                        else :
                            sql_brandname= ",brandnameid = '"+str(dfbrandname.iloc[0,0])+"'"
                            sql_log_brandname_id = ",brandname_id = '"+str(dfbrandname.iloc[0,0])+"'"
                    dateassignedtoleeon= record["dateassignedtoleeon"]
                    if str(dateassignedtoleeon) == "" :
                        sql_dateassignedtoleeon = ""
                    else :
                        sql_dateassignedtoleeon = ",dateassignedtoleeon = '"+str(dateassignedtoleeon)+"'"
                    dateassignedtopartner= record["dateassignedtopartner"]
                    if str(dateassignedtopartner) == "" :
                        sql_dateassignedtopartner = ""
                        sql_log_date_assigned_to_partner = ""
                    else :
                        sql_dateassignedtopartner = ",dateassignedtopartner = '"+str(dateassignedtopartner)+"'"
                        sql_log_date_assigned_to_partner = ",date_assigned_to_partner = '"+str(dateassignedtopartner)+"'"
                    end_of_date = record["end_of_date"]
                    if str(end_of_date) == "" :
                        sql_end_of_date = ""
                        sql_log_end_of_date = ""
                    else :
                        sql_end_of_date = ",end_of_date = '"+str(end_of_date)+"'"
                        sql_log_end_of_date = ",end_of_date = '"+str(end_of_date)+"'"
                    initialization_fee= record["initialization_fee"]
                    if str(initialization_fee) == "" or str(initialization_fee) == "0":
                        sql_initialization_fee = ""
                    else :
                        sql_initialization_fee = ",initialization_fee = '"+str(initialization_fee)+"'"
                    maintaining_fee = record["maintaining_fee"]
                    if str(maintaining_fee) == "" or str(maintaining_fee) == "0":
                        sql_maintaining_fee = ""
                    else :
                        sql_maintaining_fee = ",maintaining_fee = '"+str(maintaining_fee)+"'"
                    nice_number_maintain_fee= record["nice_number_maintain_fee"]
                    if str(nice_number_maintain_fee) == "" or str(nice_number_maintain_fee) == "0":
                        sql_nice_number_maintain_fee = ""
                    else :
                        sql_nice_number_maintain_fee = ",nice_number_maintain_fee = '"+str(nice_number_maintain_fee)+"'"
                    level_number = record["level_number"]
                    if str(level_number) == "" :
                        sql_level_number = ""
                        sql_log_level_number = ""
                    else :
                        sql_level_number = ",level_number = '"+str(level_number)+"'"
                        sql_log_level_number = ",level_number = '"+str(level_number)+"'"
                    isdntype= record["isdntype"]
                    if str(isdntype) == "" :
                        sql_isdntype = ""
                        sql_isdnt_ype=""
                    else :
                        if str(isdntype)== "Siptrunk":
                            isdntype = 1
                        elif str(isdntype)== "Proconnect":
                            isdntype = 2
                        elif str(isdntype)== "3C":
                            isdntype = 3
                        else :
                            isdntype = 0
                        sql_isdntype = ",isdntype = '"+str(isdntype)+"'"
                        sql_isdnt_ype=",isdnt_ype = '"+str(isdntype)+"'"
                    # lấy thông tin vos
                    vosip = record["vosip"]
                    if str(vosip) == "" :
                        sql_vosip = ""
                        sql_log_vos_id =""
                    else :
                        sqlvos = "select id from vos where name = '"+str(vosip)+"'"
                        df = pd.read_sql(sqlvos,db)
                        if df.empty:
                            sql_vosip =""
                            sql_log_vos_id=""
                        else :
                            vosip = df.iloc[0,0]
                            sql_vosip =",vosip = '"+str(vosip)+"'"
                            sql_log_vos_id=",vos_id = '"+str(vosip)+"'"
                    # lấy thông tin partner
                    partner = record["nickname"]
                    if str(partner) == "":
                        sql_partner =""
                        sql_log_partner =""
                    else :
                        sqlpartner = "select id,partner_code from partner where nickname = '"+str(partner)+"'"
                        df = pd.read_sql(sqlpartner,db)
                        partnerid = df.iloc[0,0]
                        partner_code= df.iloc[0,1]
                        sql_partner =",partnerid = '"+str(partnerid)+"',partner_code = '"+str(partner_code)+"'"
                        sql_log_partner=",partner_code = '"+str(partner_code)+"'"
                    # lấy thông tin telco
                    telco = record["telco"]
                    if str(telco) == "":
                        sql_telco =""
                        sql_log_telco_id=""
                    else:
                        sqltelco = "select id from telco where name = '"+str(telco)+"'"
                        df = pd.read_sql(sqltelco,db)
                        telcoid= df.iloc[0,0]
                        sql_telco =",telcoid = '"+str(telcoid)+"'"
                        sql_log_telco_id=",telco_id = '"+str(telcoid)+"'"
                    # lấy thông tin numberowner
                    numberowner = record["numberowner"]
                    if str(numberowner) == "":
                        sql_numberowner =""
                        sql_log_numberowner_id=""
                    else :
                        sqlnumberowner = "select id from number_owner where isdn = '"+str(numberowner)+"'"
                        df = pd.read_sql(sqlnumberowner,db)
                        if df.empty:
                            numberownerid= ""
                        else :
                            numberownerid= df.iloc[0,0]
                        sql_numberowner =",numberownerid = '"+str(numberownerid)+"'"
                        sql_log_numberowner_id =",number_owner_id = '"+str(numberownerid)+"'"
                    note= record["note"]
                    if str(note) == "":
                        sql_note =""
                        sql_log_note=""
                    else :
                        sql_note =",note = '"+str(note)+"'"
                        sql_log_note =",note = '"+str(note)+"'"
                    status = record["status"]
                    if str(status) == "":
                        sql_status =""
                    else :
                        sql_status =",status = '"+str(status)+"'"
                    cancel_of_date = record["cancel_of_date"]
                    if str(cancel_of_date) =="":
                        sql_cancel_of_date =""
                    else :
                        sql_cancel_of_date = ",cancel_of_date = '"+str(cancel_of_date)+"'"
                    sqlupdate = sql_blocking_date_1st+sql_blocking_date_2nd+sql_brandname+sql_dateassignedtoleeon+sql_dateassignedtopartner+sql_end_of_date+sql_initialization_fee+sql_nice_number_maintain_fee+sql_isdntype+sql_level_number+sql_maintaining_fee+sql_partner+sql_telco+sql_numberowner+sql_status+sql_vosip+sql_note+sql_cancel_of_date
                    #---------
                    sql_log_update= sql_log_vos_id+sql_log_telco_id+sql_isdnt_ype+sql_log_numberowner_id+sql_log_partner+sql_log_note+sql_log_level_number+sql_log_date_assigned_to_partner+sql_log_end_of_date+sql_cancel_of_date
                    now = datetime.datetime.now()
                    current_time = now.strftime("%Y-%m-%d %H:%M:%S")
                    isdn = isdn.split(",")
                    for i in isdn :
                        sqlcheck= "select * from number_member where isdn ='"+str(i)+"'"
                        df = pd.read_sql(sqlcheck,db)
                        if not df.empty:
                            status_old = df.iloc[0,0]
                            sql_max_id= "SELECT MAX(id) FROM number_log WHERE isdn='"+str(i)+"'"
                            df_max_id =pd.read_sql(sql_max_id,db)
                            max_id = df_max_id.iloc[0,0]
                            # lấy thông tin bản ghi isdn cuối cùng
                            sql_max_id_detail= "SELECT * FROM number_member WHERE id =(SELECT MAX(id) FROM number_member WHERE isdn = '"+str(i)+"')"
                            df_max_id_detail =pd.read_sql(sql_max_id_detail,db)
                            initialization_fee_detail = df_max_id_detail.iloc[0]["initialization_fee"]
                            maintaining_fee_detail = df_max_id_detail.iloc[0]["maintaining_fee"]
                            nice_number_maintain_fee_detail = df_max_id_detail.iloc[0]["nice_number_maintain_fee"]
                            if str(status) == "4" or str(status) == "5":
                                sql_dup= "INSERT INTO number_log(isdn,action,fee,vos_id,prefix,telco_id,isdnt_ype,number_owner_id,brandname_id,level_number,date_assigned_to_partner,end_of_date,note,partner_code,user_action,cancel_of_date) SELECT isdn,'3'`action`,'0'`fee`,vos_id,prefix,telco_id,isdnt_ype,number_owner_id,brandname_id,level_number,date_assigned_to_partner,end_of_date,note,partner_code,'"+str(nameuser)+"'`user_action`,'"+str(cancel_of_date)+"'`cancel_of_date` FROM number_log WHERE isdn = '"+str(i)+"' and id = '"+str(max_id)+"'"
                                cursor.execute(sql_dup)
                                db.commit()
                                sql_dup= "INSERT INTO number_log(isdn,ACTION,fee,vos_id,prefix,telco_id,isdnt_ype,number_owner_id,brandname_id,level_number,date_assigned_to_partner,end_of_date,note,partner_code,cancel_of_date) SELECT isdn,ACTION,fee,vos_id,prefix,telco_id,isdnt_ype,number_owner_id,brandname_id,level_number,date_assigned_to_partner,end_of_date,note,partner_code,'"+str(cancel_of_date)+"'`cancel_of_date` FROM number_log WHERE isdn = '"+str(i)+"' and id = '"+str(max_id)+"'"
                                logging.info(sql_dup)
                                cursor.execute(sql_dup)
                                db.commit()
                            else :
                                sql_dup= "INSERT INTO number_log(isdn,ACTION,fee,vos_id,prefix,telco_id,isdnt_ype,number_owner_id,brandname_id,level_number,date_assigned_to_partner,end_of_date,note,partner_code) SELECT isdn,ACTION,fee,vos_id,prefix,telco_id,isdnt_ype,number_owner_id,brandname_id,level_number,date_assigned_to_partner,end_of_date,note,partner_code  FROM number_log WHERE isdn = '"+str(i)+"' ORDER BY created_time DESC LIMIT 1"
                                logging.info(sql_dup)
                                cursor.execute(sql_dup)
                                db.commit()
                            status = record["status"]
                            if str(status) == "":
                                sql_status =""
                                sql_log_action = ""
                            else :
                                sql_status =",status = '"+str(status)+"'"
                                if str(status) == "2":
                                    if str(initialization_fee) == "0":
                                        action = "4"
                                        fee = initialization_fee_detail
                                    else :
                                        action = "4"
                                        fee = initialization_fee
                                elif str(status) == "3":
                                    if str(initialization_fee) == "0":
                                        action = "5"
                                        fee = initialization_fee_detail
                                    else :
                                        action = "5"
                                        fee = initialization_fee
                                elif str(status) == "0":
                                    action = "0"
                                    fee = 0
                                elif str(status) == "4":
                                    if str(maintaining_fee) == "0":
                                        action = "2"
                                        fee = maintaining_fee_detail
                                    else :
                                        action = "2"
                                        fee = maintaining_fee
                                elif str(status) == "5":
                                    if str(maintaining_fee) == "0":
                                        action = "2"
                                        fee = maintaining_fee_detail
                                    else :
                                        action = "2"
                                        fee = maintaining_fee
                                else :
                                    if str(initialization_fee) == "0":
                                        action = "1"
                                        fee = initialization_fee_detail
                                    else :
                                        action = "1"
                                        fee = initialization_fee
                                sql_log_action = ",fee = '"+str(fee)+"',action = '"+str(action)+"'"
                            sql_create_time_max= "SELECT MAX(id) FROM number_log WHERE isdn='"+str(i)+"'"
                            df_create_time_max =pd.read_sql(sql_create_time_max,db)
                            create_time_max = df_create_time_max.iloc[0,0]
                            sql="update number_member set isdn = '"+str(i)+"'"+sqlupdate+ "where isdn ='"+str(i)+"'"
                            cursor.execute(sql)
                            sql_log="update number_log set user_action = '"+str(nameuser)+"'"+sql_log_update+sql_log_action+"where isdn ='"+str(i)+"' AND id='"+str(create_time_max)+"'"
                            logging.info(sql_log)
                            cursor.execute(sql_log)
                            sqllog = "insert into log (name,api,content,type) value ('"+str(nameuser)+"','updatenumber_member','"+str(record).replace("'","")+"','UPDATE')"
                            cursor.execute(sqllog)
                            db.commit()
                    db.close()
                    return jsonify({'OK': 'OK'})
                else :
                    return jsonify({'NOTOK': 'token hết hạn','code':401}),401
            else :
                return jsonify({'NOTOK': 'khong co quyen'})  
    except Exception as e:
        db.close()
        logging.error(str(traceback.format_exc()))
        return jsonify({'NOTOK': str(e)}),400

@number_member.route('/number_member_log', methods=['GET'])
def number_member_log():
    try :
        logging.info("-----------------------number_member_log-----------------------")
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
            nameuser = data["user"]
            if True:
                currentDate = time.strftime("%Y-%m-%d %H:%M:%S")
                print(currentDate)
                if str(timetoken) >= str(currentDate) :
                    sql = "SELECT a.cancel_of_date,a.user_action,a.created_time,a.fee,a.isdnt_ype,f.name'brandname',a.level_number,a.end_of_date,a.id,a.isdn,a.date_assigned_to_partner,a.cfu,b.name'vosip',c.nickname,d.name'telco',e.isdn'numberowner',a.action FROM number_log a left join (select * from vos) b on a.vos_id=b.id left join (select * from partner) c on a.partner_code=c.partner_code left join (select * from telco) d on a.telco_id=d.id left join (select * from number_owner) e on a.number_owner_id=e.id left join (select * from brandname) f on a.brandname_id=f.id order by a.created_time desc limit 1000"
                    logging.info(sql)
                    df = pd.read_sql(sql,db)
                    df = df.astype(object).mask(df.isna(), np.nan)
                    df = df.fillna("")
                    df = df.replace("0000-00-00 00:00:00","")
                    df['cancel_of_date'] = df['cancel_of_date'].astype('str')
                    df['date_assigned_to_partner'] = df['date_assigned_to_partner'].astype('str')
                    df['date_assigned_to_partner'] = df['date_assigned_to_partner'].str[:10]
                    df['end_of_date'] = df['end_of_date'].astype('str')
                    df['end_of_date'] = df['end_of_date'].str[:10]
                    df['created_time'] = df['created_time'].astype('str')
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
        db.close()
        logging.error(str(traceback.format_exc()))
        return jsonify({'Lỗi': 'Quá nhiều lỗi'})


@number_member.route('/number_member_log_find', methods=['GET'])
def number_member_log_find():
    try :
        logging.info("-----------------------listnumber_member-----------------------")
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
            nameuser = data["user"]
            if True:
                currentDate = time.strftime("%Y-%m-%d %H:%M:%S")
                if str(timetoken) >= str(currentDate) :
                    logging.info(request.args)
                    isdn = request.args.get('isdn')
                    telco = request.args.get('telco')
                    nickname = request.args.get('nickname')
                    action = request.args.get("action")
                    if str(isdn) =="":
                        sql_isdn = ""
                    else :
                        sql_isdn = " and a.isdn = '"+str(isdn)+"'"
                    if str(telco) == "" :
                        sql_telco = ""
                    else :
                        sql_telco = " and a.telco_id = (select id from telco where name = '"+str(telco)+"')"
                    if str(nickname) =="":
                        sql_nickname = ""
                    else :
                        sql_nickname = " and a.partner_code = (select partner_code from partner where nickname = '"+str(nickname)+"')"
                    if str(action) == "":
                        sql_action = ""
                    else :
                        sql_action = " and a.action = '"+str(action)+"'"
                    sql = "SELECT a.cancel_of_date,a.user_action,a.created_time,a.fee,a.isdnt_ype,f.name'brandname',a.level_number,a.end_of_date,a.id,a.isdn,a.date_assigned_to_partner,a.cfu,b.name'vosip',c.nickname,d.name'telco',e.isdn'numberowner',a.action FROM number_log a left join (select * from vos) b on a.vos_id=b.id left join (select * from partner) c on a.partner_code=c.partner_code left join (select * from telco) d on a.telco_id=d.id left join (select * from number_owner) e on a.number_owner_id=e.id left join (select * from brandname) f on a.brandname_id=f.id where a.id is not null"+sql_isdn+sql_telco+sql_nickname+sql_action+" limit 1234"
                    logging.info(sql)
                    df = pd.read_sql(sql,db)
                    df = df.astype(object).mask(df.isna(), np.nan)
                    df = df.fillna("")
                    df = df.replace("0000-00-00 00:00:00","")
                    df['cancel_of_date'] = df['cancel_of_date'].astype('str')
                    df['date_assigned_to_partner'] = df['date_assigned_to_partner'].astype('str')
                    # df['dateassignedtopartner'] = df['dateassignedtopartner'].str[:10]
                    df['end_of_date'] = df['end_of_date'].astype('str')
                    df['created_time'] = df['created_time'].astype('str')
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
        db.close()
        logging.error(str(traceback.format_exc()))
        return jsonify({'Lỗi': 'Quá nhiều lỗi'})