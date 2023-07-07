from flask import Blueprint
from flask import request, jsonify
import json

import requests
from requests.auth import HTTPDigestAuth
import pandas as pd
from pandas import json_normalize
import pymysql
from flask_jsonpify import jsonpify
from datetime import datetime
from function import group
import time
import logging
import re
partnerdetail = Blueprint('partnerdetail', __name__)
@partnerdetail.route('/listpartnerdetail', methods=['GET'])
def listpartnerdetail():
    db = pymysql.connect(host="172.17.0.1",    # your host, usually localhost
                     user="leeoncrm",         # your username
                     passwd="41XmKsO3NBgHPwv",  # your password
                     db="leeon_crm",
                     port=3306
                     )
    sql ="SELECT a.id,a.createdtime,b.nickname'partner',c.name'account',d.name'mapping',e.name'routing',g.name,g.ip,h.name'telco',CASE WHEN a.type ='1' THEN 'callin' WHEN a.type ='2' THEN 'callout' END AS type FROM (SELECT * FROM partnerdetail) a  LEFT JOIN (SELECT * FROM partner) b ON a.partnerid = b.id LEFT JOIN (SELECT * FROM account) c ON a.accountid = c.id LEFT JOIN (SELECT * FROM mapping) d ON a.mappingid = d.id LEFT JOIN (SELECT * FROM routing) e ON a.routingid = e.id LEFT JOIN (SELECT * FROM vos) g ON a.vosid = g.id LEFT JOIN (SELECT * FROM telco) h ON a.telcoid = h.id order by createdtime desc"
    df = pd.read_sql(sql,db)
    df['createdtime'] = df['createdtime'].astype('str')
    json_records = df.to_json(orient ='records')
    data = []
    data = json.loads(json_records)
    context = {'data': data,'code': 'OK'}
    return context
@partnerdetail.route('/listidpartnerdetail', methods=['GET'])
def listidpartnerdetail():
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
        user = data["user"]
        print(user)
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
            print(nickname)
        if True:
            currentDate = time.strftime("%Y-%m-%d %H:%M:%S")
            print('timetoken',timetoken)
            print('hientai',currentDate)
            if str(timetoken) >= str(currentDate) :
                if str(partner) != "":
                    sql ="SELECT c.id,d.name'vosip',a.partnerid,c.name'account',CASE WHEN a.type ='1' THEN 'callin' WHEN a.type ='2' THEN 'callout' END AS type FROM (SELECT * FROM partnerdetail where partnerid in ("+str(nickname)+") and type !=0) a LEFT JOIN (SELECT * FROM account) c ON a.accountid = c.id left join (select * from vos) d on a.vosid = d.id group by c.name,c.id"
                else :
                    sql ="SELECT c.id,d.name'vosip',a.partnerid,c.name'account',CASE WHEN a.type ='1' THEN 'callin' WHEN a.type ='2' THEN 'callout' END AS type FROM (SELECT * FROM partnerdetail where type !=0) a LEFT JOIN (SELECT * FROM account) c ON a.accountid = c.id left join (select * from vos) d on a.vosid = d.id group by c.name,c.id"
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
@partnerdetail.route('/deletepartnerdetail', methods=['DELETE'])
def deletepartnerdetail():
    try :
        db = pymysql.connect(host="172.17.0.1",    # your host, usually localhost
                     user="leeoncrm",         # your username
                     passwd="41XmKsO3NBgHPwv",  # your password
                     db="leeon_crm",
                     port=3306
                     )
    
        record = json.loads(request.data)
        id = record["id"]
        print(id)
        ids = str(id).replace("[","").replace("]","").replace(" ","").split(",")
        print(ids)
        for x in range(len(ids)): 
            sqlcheck="select count(*) from partnerdetail where id = '"+str(ids[x])+"'"
            df = pd.read_sql(sqlcheck,db)
            sl = df.iloc[0,0]
            if int(sl) > 0 :
                sql ="delete from partnerdetail where id = '"+str(ids[x])+"'"
                cursor = db.cursor()
                cursor.execute(sql)
                db.commit()
            else :
                return jsonify({'NOTOK': "Khoong ton tai id " +str(ids[x])}),400
        return jsonify({'OK': 'OK'})       
    except :
        return jsonify({'Lỗi': 'Quá nhiều lỗi'})
@partnerdetail.route('/insertpartnerdetail', methods=['POST'])
def insertpartnerdetail():

        db = pymysql.connect(host="172.17.0.1",    # your host, usually localhost
                     user="leeoncrm",         # your username
                     passwd="41XmKsO3NBgHPwv",  # your password
                     db="leeon_crm",
                     port=3306
                     )
    
        record = json.loads(request.data)
        print(request.data)
        account = record["account"]
        ip = record["ip"]
        print(ip)
        mapping = record["mapping"]
        partner = record["partner"]
        routing = record["routing"]
        telco = record["telco"]
        type = record["type"]
        now = datetime.now()
        if len(mapping) == 0:
                routing =str(routing).split(',')
                for i in routing :
                    if len(account) == 0 :
                        idaccount ='0'
                    else : 
                        sqlaccount ="select id from account where name ='"+str(account)+"' and vosid = (select id from vos where ip ='"+str(ip)+"')"
                        dfaccount = pd.read_sql(sqlaccount,db)
                        if dfaccount.empty :
                            return jsonify({'Lỗi': 'Nhập sai account'})
                        else :
                            idaccount = dfaccount.iloc[0,0]
                    sqlvos ="select id from vos where ip ='"+str(ip)+"'"
                    dfvos = pd.read_sql(sqlvos,db)
                    if dfvos.empty :
                        return jsonify({'Lỗi': 'Nhập sai ip'})
                    else :
                        idvos = dfvos.iloc[0,0]
                    if len(partner) == 0 :
                        idpartner = '0'
                    else :
                        sqlpartner ="select id from partner where nickname ='"+str(partner)+"'"
                        dfpartner = pd.read_sql(sqlpartner,db)
                        idpartner = dfpartner.iloc[0,0]
                    sqlrouting ="select id from routing where name ='"+str(i)+"' and vosid = (select id from vos where ip ='"+str(ip)+"')"
                    dfrouting = pd.read_sql(sqlrouting,db)
                    if dfrouting.empty :
                        idrouting='0'
                    else :
                        idrouting = dfrouting.iloc[0,0]
                    if len(telco) == 0 :
                        idtelco ='0'
                    else :
                        sqltelco ="select id from telco where name ='"+str(telco)+"'"
                        dftelco = pd.read_sql(sqltelco,db)
                        idtelco = dftelco.iloc[0,0]
                    current_time = now.strftime("%Y-%m-%d %H:%M:%S")
                    if type == 'callin' :
                        sql="insert into partnerdetail(partnerid,accountid,routingid,vosid,type,telcoid,createdtime,updatedtime) value ('"+str(idpartner)+"','"+str(idaccount)+"','"+str(idrouting)+"','"+str(idvos)+"','1','"+str(idtelco)+"','"+str(current_time)+"','"+str(current_time)+"')"
                    elif type == 'callout' :
                        sql="insert into partnerdetail(partnerid,accountid,routingid,vosid,type,telcoid,createdtime,updatedtime) value ('"+str(idpartner)+"','"+str(idaccount)+"','"+str(idrouting)+"','"+str(idvos)+"','2','"+str(idtelco)+"','"+str(current_time)+"','"+str(current_time)+"')"
                    else :
                        sql="insert into partnerdetail(partnerid,accountid,routingid,vosid,type,telcoid,createdtime,updatedtime) value ('"+str(idpartner)+"','"+str(idaccount)+"','"+str(idrouting)+"','"+str(idvos)+"','0','"+str(idtelco)+"','"+str(current_time)+"','"+str(current_time)+"')"
                    cursor = db.cursor()
                    cursor.execute(sql)
                    db.commit()
                db.close()
                return jsonify({'OK': 'OK'})
        else :
            if len(routing)== 0 :
                if len(account) == 0:
                    idaccount = '0'
                else :
                    sqlaccount ="select id from account where name ='"+str(account)+"' and vosid = (select id from vos where ip ='"+str(ip)+"')"
                    dfaccount = pd.read_sql(sqlaccount,db)
                    idaccount = dfaccount.iloc[0,0]
                sqlvos ="select id from vos where ip ='"+str(ip)+"'"
                dfvos = pd.read_sql(sqlvos,db)
                idvos = dfvos.iloc[0,0]
                if len(mapping) == 0 :
                    idmapping = '0'
                else :
                    sqlmapping ="select id from mapping where name ='"+str(mapping)+"' and vosid = (select id from vos where ip ='"+str(ip)+"')"
                    dfmapping = pd.read_sql(sqlmapping,db)
                    idmapping = dfmapping.iloc[0,0]
                if len(partner) == 0 :
                    idpartner = '0'
                else :
                    sqlpartner ="select id from partner where nickname ='"+str(partner)+"'"
                    dfpartner = pd.read_sql(sqlpartner,db)
                    idpartner = dfpartner.iloc[0,0]
                if len(telco) == 0 :
                    idtelco = '0'
                else :
                    sqltelco ="select id from telco where name ='"+str(telco)+"'"
                    dftelco = pd.read_sql(sqltelco,db)
                    idtelco = dftelco.iloc[0,0]
                current_time = now.strftime("%Y-%m-%d %H:%M:%S")
                if type == 'callin' :
                    sql="insert into partnerdetail(partnerid,accountid,mappingid,vosid,type,telcoid,createdtime,updatedtime) value ('"+str(idpartner)+"','"+str(idaccount)+"','"+str(idmapping)+"','"+str(idvos)+"','1','"+str(idtelco)+"','"+str(current_time)+"','"+str(current_time)+"')"
                elif type == 'callout' :
                    sql="insert into partnerdetail(partnerid,accountid,mappingid,vosid,type,telcoid,createdtime,updatedtime) value ('"+str(idpartner)+"','"+str(idaccount)+"','"+str(idmapping)+"','"+str(idvos)+"','2','"+str(idtelco)+"','"+str(current_time)+"','"+str(current_time)+"')"
                else :
                    sql="insert into partnerdetail(partnerid,accountid,mappingid,vosid,type,telcoid,createdtime,updatedtime) value ('"+str(idpartner)+"','"+str(idaccount)+"','"+str(idmapping)+"','"+str(idvos)+"','0','"+str(idtelco)+"','"+str(current_time)+"','"+str(current_time)+"')"
                cursor = db.cursor()
                cursor.execute(sql)
                db.commit()
                db.close()
                return jsonify({'OK': 'OK'})
            else :
                routing =str(routing).split(',')
                for i in routing :
                    if len(account) == 0:
                        idaccount = '0'
                    else :
                        sqlaccount ="select id from account where name ='"+str(account)+"'"
                        dfaccount = pd.read_sql(sqlaccount,db)
                        idaccount = dfaccount.iloc[0,0]
                    sqlvos ="select id from vos where ip ='"+str(ip)+"'"
                    dfvos = pd.read_sql(sqlvos,db)
                    idvos = dfvos.iloc[0,0]
                    if len(mapping) == 0 :
                        idmapping = '0'
                    else :
                        sqlmapping ="select id from mapping where name ='"+str(mapping)+"'"
                        dfmapping = pd.read_sql(sqlmapping,db)
                        idmapping = dfmapping.iloc[0,0]
                    sqlrouting ="select id from routing where name ='"+str(i)+"'"
                    dfrouting = pd.read_sql(sqlrouting,db)
                    idrouting = dfrouting.iloc[0,0]
                    if len(partner) == 0 :
                        idpartner = '0'
                    else :
                        sqlpartner ="select id from partner where nickname ='"+str(partner)+"'"
                        dfpartner = pd.read_sql(sqlpartner,db)
                        idpartner = dfpartner.iloc[0,0]
                    if len(telco) == 0 :
                        idtelco = '0'
                    else :
                        sqltelco ="select id from telco where name ='"+str(telco)+"'"
                        dftelco = pd.read_sql(sqltelco,db)
                        idtelco = dftelco.iloc[0,0]
                    current_time = now.strftime("%Y-%m-%d %H:%M:%S")
                    if type == 'callin' :
                        sql="insert into partnerdetail(partnerid,accountid,routingid,mappingid,vosid,type,telcoid,createdtime,updatedtime) value ('"+str(idpartner)+"','"+str(idaccount)+"','"+str(idrouting)+"','"+str(idmapping)+"','"+str(idvos)+"','1','"+str(idtelco)+"','"+str(current_time)+"','"+str(current_time)+"')"
                        cursor = db.cursor()
                        cursor.execute(sql)
                        db.commit()
                    else :
                        sql="insert into partnerdetail(partnerid,accountid,routingid,mappingid,vosid,type,telcoid,createdtime,updatedtime) value ('"+str(idpartner)+"','"+str(idaccount)+"','"+str(idrouting)+"','"+str(idmapping)+"','"+str(idvos)+"','2','"+str(idtelco)+"','"+str(current_time)+"','"+str(current_time)+"')"
                        cursor = db.cursor()
                        cursor.execute(sql)
                        db.commit()
                db.close()
                return jsonify({'OK': 'OK'})

@partnerdetail.route('/updatepartnerdetail', methods=['POST'])
def updatepartnerdetail():

        db = pymysql.connect(host="172.17.0.1",    # your host, usually localhost
                     user="leeoncrm",         # your username
                     passwd="41XmKsO3NBgHPwv",  # your password
                     db="leeon_crm",
                     port=3306
                     )
        record = json.loads(request.data)
        print(record)
        id = record["id"]
        account = record["account"]
        ip = record["ip"]
        mapping = record["mapping"]
        partner = record["partner"]
        routing = record["routing"]
        type = record["type"]
        telco = record["telco"]
        now = datetime.now()
        current_time = now.strftime("%Y-%m-%d %H:%M:%S")
        sqlaccount ="select id from account where name ='"+str(account)+"'"
        dfaccount = pd.read_sql(sqlaccount,db)
        idaccount = dfaccount.iloc[0,0]
        sqlvos ="select id from vos where ip ='"+str(ip)+"'"
        dfvos = pd.read_sql(sqlvos,db)
        idvos = dfvos.iloc[0,0]
        sqlmapping ="select id from mapping where name ='"+str(mapping)+"'"
        dfmapping = pd.read_sql(sqlmapping,db)
        if dfmapping.empty:
            idmapping=""
        else :
            idmapping = dfmapping.iloc[0,0]
        sqlpartner ="select id from partner where nickname ='"+str(partner)+"'"
        dfpartner = pd.read_sql(sqlpartner,db)
        idpartner = dfpartner.iloc[0,0]
        sqlrouting ="select id from routing where name ='"+str(routing)+"'"
        dfrouting = pd.read_sql(sqlrouting,db)
        if dfrouting.empty:
            idrouting=""
        else :
            idrouting = dfrouting.iloc[0,0]
        sqltelco ="select id from telco where name ='"+str(telco)+"'"
        dftelco = pd.read_sql(sqltelco,db)
        if dftelco.empty:
            idtelco=""
        else :
            idtelco = dftelco.iloc[0,0]
        sqlcheck= "select count(*) from partnerdetail where id ='"+str(id)+"'"
        df = pd.read_sql(sqlcheck,db)
        sl = df.iloc[0,0]
        print(sl)
        if int(sl) > 0 :
            if type == 'callin' :
                sql="update partnerdetail set partnerid = '"+str(idpartner)+"',accountid='"+str(idaccount)+"',mappingid='"+str(idmapping)+"',routingid ='"+str(idrouting)+"',vosid='"+str(idvos)+"',type = '1',telcoid = '"+str(idtelco)+"',updatedtime='"+str(current_time)+"' where id = '"+str(id)+"'"
                cursor = db.cursor()
                cursor.execute(sql)
                db.commit()
                return jsonify({'OK': 'OK'})
            else :
                sql="update partnerdetail set partnerid = '"+str(idpartner)+"',accountid='"+str(idaccount)+"',mappingid='"+str(idmapping)+"',routingid ='"+str(idrouting)+"',vosid='"+str(idvos)+"',type = '2',telcoid = '"+str(idtelco)+"',updatedtime='"+str(current_time)+"' where id = '"+str(id)+"'"
                cursor = db.cursor()
                cursor.execute(sql)
                db.commit()
                return jsonify({'OK': 'OK'})
        else :
            return jsonify({'NOTOK': 'Không tồn tại id này'})
        db.close()  
@partnerdetail.route('/listidpartnerdetailreport', methods=['GET'])
def listidpartnerdetailreport():
    db = pymysql.connect(host="172.17.0.1",    # your host, usually localhost
                     user="leeoncrm",         # your username
                     passwd="41XmKsO3NBgHPwv",  # your password
                     db="leeon_crm",
                     port=3306
                     )
    sql ="SELECT c.id,a.createdtime,a.partnerid,c.name'account',CASE WHEN a.type ='1' THEN 'callin' WHEN a.type ='2' THEN 'callout' END AS type FROM (SELECT * FROM partnerdetail where type !=0) a LEFT JOIN (SELECT * FROM account) c ON a.accountid = c.id order by createdtime desc"
    df = pd.read_sql(sql,db)
    df['createdtime'] = df['createdtime'].astype('str')
    json_records = df.to_json(orient ='records')
    data = []
    data = json.loads(json_records)
    context = {'data': data,'code': 'OK'}
    return context
@partnerdetail.route('/listippartnerdetail', methods=['GET'])
def listippartnerdetail():
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
        user = data["user"]
        print(user)
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
            print(nickname)
        if True:
            currentDate = time.strftime("%Y-%m-%d %H:%M:%S")
            print('timetoken',timetoken)
            print('hientai',currentDate)
            if str(timetoken) >= str(currentDate) :
                sql = "SELECT a.id,a.partnerid,c.name'vosip' FROM leeon_crm.partnerdetail a, vos c where a.vosid =c.id group by a.partnerid,c.name"
                # if str(partner) != "":
                #     sql ="SELECT c.id,d.name'vosip',a.partnerid,c.name'account',CASE WHEN a.type ='1' THEN 'callin' WHEN a.type ='2' THEN 'callout' END AS type FROM (SELECT * FROM partnerdetail where partnerid in ("+str(nickname)+") and type !=0) a LEFT JOIN (SELECT * FROM account) c ON a.accountid = c.id left join (select * from vos) d on a.vosid = d.id group by c.name,c.id"
                # else :
                #     sql ="SELECT c.id,d.name'vosip',a.partnerid,c.name'account',CASE WHEN a.type ='1' THEN 'callin' WHEN a.type ='2' THEN 'callout' END AS type FROM (SELECT * FROM partnerdetail where type !=0) a LEFT JOIN (SELECT * FROM account) c ON a.accountid = c.id left join (select * from vos) d on a.vosid = d.id group by c.name,c.id"
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


@partnerdetail.route('/list_ip_partnerdetail', methods=['GET'])
def list_ip_partnerdetail():

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
                    nickname = request.args.get('nickname')
                    sql = "select distinct(c.name) from partner a, partnerdetail b, vos c where a.id = b.partnerid and b.vosid = c.id and a.nickname = '"+str(nickname)+"'"
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
@partnerdetail.route('/list_acount_partnerdetail', methods=['GET'])
def list_acount_partnerdetail():

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
                    nickname = request.args.get('nickname')
                    vosip = request.args.get('vosip')
                    sqlpartnerid = "select id from partner where nickname = '"+str(nickname)+"'"
                    dfpartnerid = pd.read_sql(sqlpartnerid,db)
                    if dfpartnerid.empty:
                        return jsonify({'NOTOK': 'sai nickname'})
                    else :
                        partnerid = dfpartnerid.iloc[0,0]
                    sqlvosid = "select id from vos where name = '"+str(vosip)+"'"
                    dfvosid = pd.read_sql(sqlvosid,db)
                    if dfvosid.empty:
                        return jsonify({'NOTOK': 'sai vosip'})
                    else :
                        vosid = dfvosid.iloc[0,0]
                    sql = "SELECT distinct(b.name) FROM partnerdetail a,account b where a.accountid =b.id and a.partnerid = '"+str(partnerid)+"' and a.vosid ='"+str(vosid)+"'"
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


@partnerdetail.route('/find_partnerdetail', methods=['GET'])
def find_partnerdetail():
    session=datetime.now()
    logging.info(str(session) +"|find_partnerdetail")
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
            logging.info(str(session) +"|find_sender_vender|"+str(data))
            if True:
                currentDate = time.strftime("%Y-%m-%d %H:%M:%S")
                print(currentDate)
                if str(timetoken) >= str(currentDate) :
                    account = request.args.get('account')
                    ip = request.args.get('ip')
                    partner = request.args.get('partner')
                    if str(account) == "" :
                        sqlaccount = ""
                    else :
                        sqlaccount = " and accountid in (select distinct(id) from account where name = '"+str(account)+"')"
                    if str(ip) == "" :
                        sqlip = ""
                    else :
                        sqlip = " and vosid in (select distinct(id) from vos where name = '"+str(ip)+"')"
                    if str(partner) == "" :
                        sqlpartner = ""
                    else :
                        sqlpartner = " and partnerid = (select distinct(id) from partner where nickname = '"+str(partner)+"')"
                    sql ="SELECT a.id,a.createdtime,b.nickname'partner',c.name'account',d.name'mapping',e.name'routing',g.name,g.ip,h.name'telco',CASE WHEN a.type ='1' THEN 'callin' WHEN a.type ='2' THEN 'callout' END AS type FROM (SELECT * FROM partnerdetail where id is not null "+sqlaccount+sqlip+sqlpartner+") a  LEFT JOIN (SELECT * FROM partner) b ON a.partnerid = b.id LEFT JOIN (SELECT * FROM account) c ON a.accountid = c.id LEFT JOIN (SELECT * FROM mapping) d ON a.mappingid = d.id LEFT JOIN (SELECT * FROM routing) e ON a.routingid = e.id LEFT JOIN (SELECT * FROM vos) g ON a.vosid = g.id LEFT JOIN (SELECT * FROM telco) h ON a.telcoid = h.id order by createdtime desc"
                    print(sql)
                    df = pd.read_sql(sql,db)
                    df['createdtime'] = df['createdtime'].astype('str')
                    json_records = df.to_json(orient ='records')
                    data = []
                    data = json.loads(json_records)
                    context = {'data': data,'code': 'OK'}
                    db.close()
                    return context
                else :
                    db.close()
                    logging.error(str(session) +"|response|token hết hạn")
                    return jsonify({'NOTOK': 'token hết hạn','code':401}),401
            else :
                db.close()
                logging.error(str(session) +"|response|Không có quyền")
                return jsonify({'NOTOK': 'khong co quyen'}) 
    except Exception as e:
        logging.error(str(session) +"|response|" +str(e))
        return jsonify({'NOTOK': str(e)}),400