from flask import Blueprint
from flask import request, jsonify
from sqlalchemy import create_engine
import json
import traceback
import requests
from requests.auth import HTTPDigestAuth
import pandas as pd
from pandas import json_normalize
import pymysql
from flask_jsonpify import jsonpify
from datetime import datetime,date, timedelta
import datetime
import bcrypt
import re
from function import group
import time
import logging
permision = Blueprint('permision', __name__)
@permision.route('/listgroup', methods=['GET'])
def listgroup():
    db = pymysql.connect(host="172.17.0.1",    # your host, usually localhost
                     user="leeoncrm",         # your username
                     passwd="41XmKsO3NBgHPwv",  # your password
                     db="leeon_crm",
                     port=3306
                     )
    sql ="SELECT * FROM leeon_crm.group"
    df = pd.read_sql(sql,db)
    json_records = df.to_json(orient ='records')
    data = []
    data = json.loads(json_records)
    context = {'data': data,'code': 'OK'}
    return context
@permision.route('/deletegroup', methods=['DELETE'])
def deletegroup():
    try :
        db = pymysql.connect(host="172.17.0.1",    # your host, usually localhost
                     user="leeoncrm",         # your username
                     passwd="41XmKsO3NBgHPwv",  # your password
                     db="leeon_crm",
                     port=3306
                     )
    
        record = json.loads(request.data)
        id = record["id"]
        sqlcheck="select count(*) from leeon_crm.group where id = '"+str(id)+"'"
        df = pd.read_sql(sqlcheck,db)
        sl = df.iloc[0,0]
        if int(sl) > 0 :
            sql ="delete from leeon_crm.group where id = '"+str(id)+"'"
            cursor = db.cursor()
            cursor.execute(sql)
            db.commit()
            return jsonify({'OK': id})
        else :
            return jsonify({'OK': 'Không có id này'})
    except :
        return jsonify({'Lỗi': 'Quá nhiều lỗi'})
@permision.route('/insertgroup', methods=['POST'])
def insertgroup():
    try :
        db = pymysql.connect(host="172.17.0.1",    # your host, usually localhost
                     user="leeoncrm",         # your username
                     passwd="41XmKsO3NBgHPwv",  # your password
                     db="leeon_crm",
                     port=3306
                     )
        record = json.loads(request.data)
        print(request.data)
        name = record["name"]
        description = record["description"]
        sql ="insert into leeon_crm.group (name,description) value ('"+str(name)+"','"+str(description)+"')"
        cursor = db.cursor()
        cursor.execute(sql)
        db.commit()
        return jsonify({'OK': 'OK'})
    except :
        return jsonify({'Lỗi': 'Quá nhiều lỗi'})
@permision.route('/updategroup', methods=['POST'])
def updategroup():
    try :
        db = pymysql.connect(host="172.17.0.1",    # your host, usually localhost
                     user="leeoncrm",         # your username
                     passwd="41XmKsO3NBgHPwv",  # your password
                     db="leeon_crm",
                     port=3306
                     )
        record = json.loads(request.data)
        id = record["id"]
        name = record["name"]
        description = record["description"]
        sql ="update leeon_crm.group set name ='"+str(name)+"',description ='"+str(description)+"' where id = '"+str(id)+"'"
        cursor = db.cursor()
        cursor.execute(sql)
        db.commit()
        return jsonify({'OK': 'OK'})
    except :
        return jsonify({'Lỗi': 'Quá nhiều lỗi'})
@permision.route('/listuser_group', methods=['GET'])
def listuser_group():
    db = pymysql.connect(host="172.17.0.1",    # your host, usually localhost
                     user="leeoncrm",         # your username
                     passwd="41XmKsO3NBgHPwv",  # your password
                     db="leeon_crm",
                     port=3306
                     )
    sql ="SELECT b.username'username',c.name'group' FROM leeon_crm.user_group a join (select * from user) b on a.userId =b.id join (SELECT * FROM leeon_crm.group) c on a.groupId = c.id"
    df = pd.read_sql(sql,db)
    json_records = df.to_json(orient ='records')
    data = []
    data = json.loads(json_records)
    context = {'data': data,'code': 'OK'}
    return context
@permision.route('/insert_user_group', methods=['POST'])
def insert_user_group():
    try:
        db = pymysql.connect(host="172.17.0.1",    # your host, usually localhost
                        user="leeonstlapi",         # your username
                        passwd="LKJl2i308407888998",  # your password
                        db="leeon_crm",
                        port=3306
                        )
        record = json.loads(request.data)
        print(request.data)
        group = record["group"]
        username = record["username"]
        sql ="insert into leeon_crm.user_group (userId,groupId) value ((select id from user where username = '"+str(username)+"'),(select id from leeon_crm.group where name = '"+str(group)+"'))"
        cursor = db.cursor()
        cursor.execute(sql)
        db.commit()
        return jsonify({'OK': 'OK'})
    except :
        return jsonify({'Lỗi': 'Quá nhiều lỗi'})
@permision.route('/delete_user_group', methods=['DELETE'])
def delete_user_group():
    try :
        db = pymysql.connect(host="172.17.0.1",    # your host, usually localhost
                     user="leeoncrm",         # your username
                     passwd="41XmKsO3NBgHPwv",  # your password
                     db="leeon_crm",
                     port=3306
                     )
    
        record = json.loads(request.data)
        id = record["id"]
        sqlcheck="select count(*) from user_group where id = '"+str(id)+"'"
        df = pd.read_sql(sqlcheck,db)
        sl = df.iloc[0,0]
        if int(sl) > 0 :
            sql ="delete from user_group where id = '"+str(id)+"'"
            cursor = db.cursor()
            cursor.execute(sql)
            db.commit()
            return jsonify({'OK': id})
        else :
            return jsonify({'OK': 'Không có id này'})
    except :
        return jsonify({'Lỗi': 'Quá nhiều lỗi'})
@permision.route('/listper', methods=['GET'])
def listper():
    db = pymysql.connect(host="172.17.0.1",    # your host, usually localhost
                     user="leeoncrm",         # your username
                     passwd="41XmKsO3NBgHPwv",  # your password
                     db="leeon_crm",
                     port=3306
                     )
    sql ="SELECT * FROM permission"
    df = pd.read_sql(sql,db)
    json_records = df.to_json(orient ='records')
    data = []
    data = json.loads(json_records)
    context = {'data': data,'code': 'OK'}
    return context
@permision.route('/delete_per', methods=['DELETE'])
def delete_per():
    try :
        db = pymysql.connect(host="172.17.0.1",    # your host, usually localhost
                     user="leeoncrm",         # your username
                     passwd="41XmKsO3NBgHPwv",  # your password
                     db="leeon_crm",
                     port=3306
                     )
    
        record = json.loads(request.data)
        id = record["id"]
        sqlcheck="select count(*) from permission where id = '"+str(id)+"'"
        df = pd.read_sql(sqlcheck,db)
        sl = df.iloc[0,0]
        if int(sl) > 0 :
            sql ="delete from permission where id = '"+str(id)+"'"
            cursor = db.cursor()
            cursor.execute(sql)
            db.commit()
            return jsonify({'OK': id})
        else :
            return jsonify({'OK': 'Không có id này'})
    except :
        return jsonify({'Lỗi': 'Quá nhiều lỗi'})
@permision.route('/insert_per', methods=['POST'])
def insert_per():
    try:
        db = pymysql.connect(host="172.17.0.1",    # your host, usually localhost
                        user="leeonstlapi",         # your username
                        passwd="LKJl2i308407888998",  # your password
                        db="leeon_crm",
                        port=3306
                        )
        record = json.loads(request.data)
        print(request.data)
        code = record["code"]
        name = record["name"]
        sql ="insert into permission (code,name) value ('"+str(code)+"','"+str(name)+"')"
        cursor = db.cursor()
        cursor.execute(sql)
        db.commit()
        return jsonify({'OK': 'OK'})
    except :
        return jsonify({'Lỗi': 'Quá nhiều lỗi'})
@permision.route('/updateper', methods=['POST'])
def updateper():
    try :
        db = pymysql.connect(host="172.17.0.1",    # your host, usually localhost
                     user="leeoncrm",         # your username
                     passwd="41XmKsO3NBgHPwv",  # your password
                     db="leeon_crm",
                     port=3306
                     )
        record = json.loads(request.data)
        id = record["id"]
        code = record["code"]
        name = record["name"]
        sql ="update permission set code ='"+str(code)+"',name ='"+str(name)+"' where id = '"+str(id)+"'"
        cursor = db.cursor()
        cursor.execute(sql)
        db.commit()
        return jsonify({'OK': 'OK'})
    except :
        return jsonify({'Lỗi': 'Quá nhiều lỗi'})
@permision.route('/listgroup_permission', methods=['GET'])
def listgroup_permission():
    db = pymysql.connect(host="172.17.0.1",    # your host, usually localhost
                     user="leeoncrm",         # your username
                     passwd="41XmKsO3NBgHPwv",  # your password
                     db="leeon_crm",
                     port=3306
                     )
    sql ="SELECT b.name'group',c.name'permission' FROM leeon_crm.group_permission a join (select * from leeon_crm.group) b on a.groupId = b.id join (select * from permission) c on a.permissionId=c.id"
    df = pd.read_sql(sql,db)
    json_records = df.to_json(orient ='records')
    data = []
    data = json.loads(json_records)
    context = {'data': data,'code': 'OK'}
    return context
@permision.route('/delete_group_permission', methods=['DELETE'])
def delete_group_permission():
    try :
        db = pymysql.connect(host="172.17.0.1",    # your host, usually localhost
                     user="leeoncrm",         # your username
                     passwd="41XmKsO3NBgHPwv",  # your password
                     db="leeon_crm",
                     port=3306
                     )
    
        record = json.loads(request.data)
        id = record["id"]
        sqlcheck="select count(*) from group_permission where id = '"+str(id)+"'"
        df = pd.read_sql(sqlcheck,db)
        sl = df.iloc[0,0]
        if int(sl) > 0 :
            sql ="delete from group_permission where id = '"+str(id)+"'"
            cursor = db.cursor()
            cursor.execute(sql)
            db.commit()
            return jsonify({'OK': id})
        else :
            return jsonify({'OK': 'Không có id này'})
    except :
        return jsonify({'Lỗi': 'Quá nhiều lỗi'})
@permision.route('/insert_group_permission', methods=['POST'])
def insert_group_permission():
    try:
        db = pymysql.connect(host="172.17.0.1",    # your host, usually localhost
                        user="leeonstlapi",         # your username
                        passwd="LKJl2i308407888998",  # your password
                        db="leeon_crm",
                        port=3306
                        )
        record = json.loads(request.data)
        print(request.data)
        group = record["group"]
        permission = record["permission"]
        sql ="insert into group_permission (groupId,permissionId) value ((select id from leeon_crm.group where name = '"+str(group)+"'),(select id from permission where name = '"+str(permission)+"'))"
        cursor = db.cursor()
        cursor.execute(sql)
        db.commit()
        return jsonify({'OK': 'OK'})
    except :
        return jsonify({'Lỗi': 'Quá nhiều lỗi'})
@permision.route('/update_group_permission', methods=['POST'])
def update_group_permission():
    try:
        db = pymysql.connect(host="172.17.0.1",    # your host, usually localhost
                     user="leeoncrm",         # your username
                     passwd="41XmKsO3NBgHPwv",  # your password
                     db="leeon_crm",
                     port=3306
                     )
        record = json.loads(request.data)
        id = record["id"]
        group = record["group"]
        permission = record["permission"]
        sql ="update group_permission set groupId =(select id from leeon_crm.group where name = '"+str(group)+"'),permissionId =(select id from permission where name = '"+str(permission)+"') where id = '"+str(id)+"'"
        cursor = db.cursor()
        cursor.execute(sql)
        db.commit()
        return jsonify({'OK': 'OK'})
    except :
        return jsonify({'Lỗi': 'Quá nhiều lỗi'})
@permision.route('/list_user_partner', methods=['GET'])
def list_user_partner():
    db = pymysql.connect(host="172.17.0.1",    # your host, usually localhost
                     user="leeoncrm",         # your username
                     passwd="41XmKsO3NBgHPwv",  # your password
                     db="leeon_crm",
                     port=3306
                     )
    sql ="SELECT b.username, c.nickname'partner' FROM leeon_crm.user_partner a join (select * from user) b on a.userId = b.id join (select * from partner) c on a.partnerId=c.id"
    df = pd.read_sql(sql,db)
    json_records = df.to_json(orient ='records')
    data = []
    data = json.loads(json_records)
    context = {'data': data,'code': 'OK'}
    return context
@permision.route('/delete_user_partner', methods=['DELETE'])
def delete_user_partner():
    try :
        db = pymysql.connect(host="172.17.0.1",    # your host, usually localhost
                     user="leeoncrm",         # your username
                     passwd="41XmKsO3NBgHPwv",  # your password
                     db="leeon_crm",
                     port=3306
                     )
    
        record = json.loads(request.data)
        id = record["id"]
        sqlcheck="select count(*) from user_partner where id = '"+str(id)+"'"
        df = pd.read_sql(sqlcheck,db)
        sl = df.iloc[0,0]
        if int(sl) > 0 :
            sql ="delete from user_partner where id = '"+str(id)+"'"
            cursor = db.cursor()
            cursor.execute(sql)
            db.commit()
            return jsonify({'OK': id})
        else :
            return jsonify({'OK': 'Không có id này'})
    except :
        return jsonify({'Lỗi': 'Quá nhiều lỗi'})
@permision.route('/insert_user_partner', methods=['POST'])
def insert_user_partner():
    try:
        db = pymysql.connect(host="172.17.0.1",    # your host, usually localhost
                        user="leeonstlapi",         # your username
                        passwd="LKJl2i308407888998",  # your password
                        db="leeon_crm",
                        port=3306
                        )
        record = json.loads(request.data)
        print(request.data)
        user = record["user"]
        partner = record["partner"]
        sql ="insert into user_partner (userId,partnerId) value ((select id from user where username = '"+str(user)+"'),(select id from partner where nickname = '"+str(partner)+"'))"
        cursor = db.cursor()
        cursor.execute(sql)
        db.commit()
        return jsonify({'OK': 'OK'})
    except :
        return jsonify({'Lỗi': 'Quá nhiều lỗi'})
@permision.route('/update_user_partner', methods=['POST'])
def update_user_partner():
    try :
        db = pymysql.connect(host="172.17.0.1",    # your host, usually localhost
                     user="leeoncrm",         # your username
                     passwd="41XmKsO3NBgHPwv",  # your password
                     db="leeon_crm",
                     port=3306
                     )
        record = json.loads(request.data)
        id = record["id"]
        user = record["user"]
        partner = record["partner"]
        sql ="update user_partner set userId =(select id from user where username = '"+str(user)+"'),partnerId =(select id from partner where nickname = '"+str(partner)+"') where id = '"+str(id)+"'"
        cursor = db.cursor()
        cursor.execute(sql)
        db.commit()
        return jsonify({'OK': 'OK'})
    except :
        return jsonify({'Lỗi': 'Quá nhiều lỗi'})
@permision.route('/list_user', methods=['GET'])
def list_user():
    session=datetime.datetime.now()
    logging.info(str(session) +"|list_user")
    try:
        db_connection_str = 'mysql+pymysql://leeoncrm:41XmKsO3NBgHPwv@172.17.0.1/leeon_crm'
        db = create_engine(db_connection_str)
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
                    if nameuser =="admin":
                        sql ="SELECT case when isActive = 1 then 'Hoạt Động' else 'Tạm Dừng' end as status, a.id,a.fullname,a.username,a.email,a.phonenumber,c.name'group' FROM leeon_crm.user a left join leeon_crm.user_group b on a.id = b.userId left join leeon_crm.group c on b.groupId =c.id"
                    else :
                        sql ="SELECT case when isActive = 1 then 'Hoạt Động' else 'Tạm Dừng' end as status,a.id,a.fullname,a.username,a.email,a.phonenumber,c.name'group' FROM (select * from leeon_crm.user where username = '"+str(nameuser)+"') a left join leeon_crm.user_group b on a.id = b.userId left join leeon_crm.group c on b.groupId =c.id"
                    df = pd.read_sql(sql,db)
                    Type_new = pd.Series([])
                    for i in range(len(df)):
                        sqlpartner ="select nickname from partner where id in(select partnerId from user_partner where userId = '"+str(df.iloc[i]['id'])+"')"
                        dfpartner = pd.read_sql(sqlpartner,db)
                        dfpartner=dfpartner.values.tolist()
                        Type_new[i]=str(dfpartner).replace("[","").replace("]","").replace("'","")
                    print(Type_new)
                    df.insert(6, "partner", Type_new)
                    json_records = df.to_json(orient ='records')
                    data = []
                    data = json.loads(json_records)
                    logging.info(str(session) +"|response|Thành công")
                    context = {'data': data,'code': 'OK'}
                    return context
                else :
                    logging.error(str(session) +"|response|token hết hạn")
                    return jsonify({'NOTOK': 'token hết hạn','code':401}),401
            else :
                logging.error(str(session) +"|response|Không có quyền")
                return jsonify({'NOTOK': 'khong co quyen'}),400
    except Exception as e:
        logging.error(str(session) +"|response|" +str(e))
        return jsonify({'NOTOK': str(e)}),400
@permision.route('/delete_user', methods=['DELETE'])
def delete_user():
    try :
        db = pymysql.connect(host="172.17.0.1",    # your host, usually localhost
                     user="leeoncrm",         # your username
                     passwd="41XmKsO3NBgHPwv",  # your password
                     db="leeon_crm",
                     port=3306
                     )
    
        record = json.loads(request.data)
        id = record["id"]
        sqlcheck="select count(*) from leeon_crm.user where id = '"+str(id)+"'"
        df = pd.read_sql(sqlcheck,db)
        sl = df.iloc[0,0]
        if int(sl) > 0 :
            sql ="delete from leeon_crm.user where id = '"+str(id)+"'"
            cursor = db.cursor()
            cursor.execute(sql)
            db.commit()
            return jsonify({'OK': id})
        else :
            return jsonify({'OK': 'Không có id này'})
    except :
        return jsonify({'Lỗi': 'Quá nhiều lỗi'})
@permision.route('/update_user', methods=['POST'])
def update_user():

        db = pymysql.connect(host="172.17.0.1",    # your host, usually localhost
                     user="leeoncrm",         # your username
                     passwd="41XmKsO3NBgHPwv",  # your password
                     db="leeon_crm",
                     port=3306
                     )
        cursor = db.cursor()
        record = json.loads(request.data)
        id = record["id"]
        partner = record["partner"]
        fullname = record["fullname"]
        phonenumber = record["phonenumber"]
        username = record["username"]
        email = record["email"]
        isActive = record["status"]
        group = record["group"]
        if str(isActive) == "Hoạt Động" :
            isActive = "1"
        else :
            isActive = "0"
        # password = password.encode('utf-8')
        # hashedPassword = bcrypt.hashpw(password, bcrypt.gensalt())
        #update group
        sqlupdategroup ="update user_group set groupId = (select id from leeon_crm.group where name = '"+str(group)+"') where userId = '"+str(id)+"'"
        cursor.execute(sqlupdategroup)
        #update  partner
        sql_delete_partner = "delete from user_partner where userId = '"+str(id)+"'"
        cursor.execute(sql_delete_partner)
        if str(partner) !="":
                partner =re.split('[,;/ ]+', str(partner))
                for i in partner :
                    insertpartner="insert into user_partner(userId,partnerId) value ('"+str(id)+"',(select id from leeon_crm.partner where nickname = '"+str(i)+"'))"
                    cursor = db.cursor()
                    cursor.execute(insertpartner)
        sql = "update user set isActive = '"+str(isActive)+"', fullname= '"+str(fullname)+"',username='"+str(username)+"',email='"+str(email)+"',phonenumber='"+str(phonenumber)+"',updatedAt=NOW() where id = '"+str(id)+"'"
        print(sql)
        cursor = db.cursor()
        cursor.execute(sql)
        db.commit()
        return jsonify({'OK': 'OK'})
@permision.route('/log', methods=['GET'])
def log():
    db = pymysql.connect(host="172.17.0.1",    # your host, usually localhost
                     user="leeoncrm",         # your username
                     passwd="41XmKsO3NBgHPwv",  # your password
                     db="leeon_crm",
                     port=3306
                     )
    sql ="SELECT * FROM leeon_crm.log order by createdtime desc"
    df = pd.read_sql(sql,db)
    df['createdtime'] = df['createdtime'].astype('str')
    json_records = df.to_json(orient ='records')
    data = []
    data = json.loads(json_records)
    context = {'data': data,'code': 'OK'}
    return context
@permision.route('/findlog', methods=['GET'])
def findlog():
    db = pymysql.connect(host="172.17.0.1",    # your host, usually localhost
                     user="leeoncrm",         # your username
                     passwd="41XmKsO3NBgHPwv",  # your password
                     db="leeon_crm",
                     port=3306
                     )
    starttime = request.args.get('starttime')
    endtime = request.args.get('endtime')
    name = request.args.get('name')
    query = "where"
    if str(name) =="" :
        sqlname = ""
    else :
        sqlname = " name ='"+ str(name) +"' and"
    if str(starttime) =="" :
        sqlstarttime = ""
    else :
        sqlstarttime = " createdtime >='"+ str(starttime) +"' and"
    if str(endtime) =="" :
        sqlendtime = ""
    else :
        sqlendtime = " createdtime <='"+ str(endtime) +" 23:59:59' and "
    query = query+sqlname+sqlstarttime+sqlendtime+" id is not null"
    sql ="SELECT * FROM leeon_crm.log "+str(query)+" order by createdtime desc"
    df = pd.read_sql(sql,db)
    df['createdtime'] = df['createdtime'].astype('str')
    json_records = df.to_json(orient ='records')
    data = []
    data = json.loads(json_records)
    context = {'data': data,'code': 'OK'}
    return context
@permision.route('/change_password', methods=['POST'])
def change_password():

        db = pymysql.connect(host="172.17.0.1",    # your host, usually localhost
                     user="leeoncrm",         # your username
                     passwd="41XmKsO3NBgHPwv",  # your password
                     db="leeon_crm",
                     port=3306
                     )
        record = json.loads(request.data)
        username = record["username"]
        password = record["password"]
        password = password.encode('utf-8')
        hashedPassword = bcrypt.hashpw(password, bcrypt.gensalt())
        print()
        sql = "update user set password='"+str(hashedPassword.decode())+"',updatedAt=NOW() where username = '"+str(username)+"'"
        print(sql)
        cursor = db.cursor()
        cursor.execute(sql)
        db.commit()
        return jsonify({'OK': 'OK'})
@permision.route('/list_user_customer', methods=['GET'])
def list_user_customer():
    session=datetime.datetime.now()
    logging.info(str(session) +"|list_user")
    try:
        db_connection_str = 'mysql+pymysql://leeoncrm:41XmKsO3NBgHPwv@172.17.0.1/leeon_crm'
        db = create_engine(db_connection_str)
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
                    if nameuser =="admin":
                        sql ="SELECT case when isActive = 1 then 'Hoạt Động' else 'Tạm Dừng' end as status,a.partner1,a.partner2,a.partner3, a.id,a.fullname,a.username,a.email,a.phonenumber,c.name'group' FROM user a , leeon_crm.user_group b ,leeon_crm.group c where a.id = b.userId and b.groupId =c.id and a.partner_code is not null"
                    else :
                        sql ="SELECT case when isActive = 1 then 'Hoạt Động' else 'Tạm Dừng' end as status,a.partner1,a.partner2,a.partner3,a.id,a.fullname,a.username,a.email,a.phonenumber,c.name'group' FROM (select * from leeon_crm.user where username = '"+str(nameuser)+"' and partner_code is not null) a left join leeon_crm.user_group b on a.id = b.userId left join leeon_crm.group c on b.groupId =c.id"
                    df = pd.read_sql(sql,db)
                    Type_new = pd.Series([])
                    for i in range(len(df)):
                        sqlpartner ="select nickname from partner where id in(select partnerId from user_partner where userId = '"+str(df.iloc[i]['id'])+"')"
                        dfpartner = pd.read_sql(sqlpartner,db)
                        dfpartner=dfpartner.values.tolist()
                        Type_new[i]=str(dfpartner).replace("[","").replace("]","").replace("'","")
                    print(Type_new)
                    df.insert(6, "partner", Type_new)
                    json_records = df.to_json(orient ='records')
                    data = []
                    data = json.loads(json_records)
                    logging.info(str(session) +"|response|Thành công")
                    context = {'data': data,'code': 'OK'}
                    return context
                else :
                    logging.error(str(session) +"|response|token hết hạn")
                    return jsonify({'NOTOK': 'token hết hạn','code':401}),401
            else :
                logging.error(str(session) +"|response|Không có quyền")
                return jsonify({'NOTOK': 'khong co quyen'}),400
    except Exception as e:
        logging.error(str(session) +"|response|" +str(e))
        return jsonify({'NOTOK': str(e)}),400
@permision.route('/update_user_customer', methods=['POST'])
def update_user_customer():
    try:
        db = pymysql.connect(host="172.17.0.1",    # your host, usually localhost
                     user="leeoncrm",         # your username
                     passwd="41XmKsO3NBgHPwv",  # your password
                     db="leeon_crm",
                     port=3306
                     )
        cursor = db.cursor()
        record = json.loads(request.data)
        id = record["id"]
        fullname = record["fullname"]
        phonenumber = record["phonenumber"]
        username = record["username"]
        email = record["email"]
        isActive = record["status"]
        nickname1 = record["partner1"]
        try:
            nickname2 = record["partner2"]
        except:
            nickname2 = ""
        try:
            nickname3 = record["partner3"]
        except:
            nickname3 = ""
        if str(nickname1) == "":
            partner_code1 = ""
        else :
            sql_get_partner_code = "select partner_code from partner where nickname = '"+str(nickname1)+"'"
            df = pd.read_sql(sql_get_partner_code,db)
            if df.empty:
                partner1 =""
                partner_code1 = ""
            else :
                partner1 =str(df.iloc[0,0])
                partner_code1 = str(df.iloc[0,0])
        if str(nickname2) == "":
            partner_code2 = ""
        else :
            sql_get_partner_code = "select partner_code from partner where nickname = '"+str(nickname2)+"'"
            df = pd.read_sql(sql_get_partner_code,db)
            if df.empty:
                partner2 =""
                partner_code2 = ""
            else :
                partner2 =str(df.iloc[0,0])
                partner_code2 = "-" + str(df.iloc[0,0])
        if str(nickname3) == "":
            partner_code3 = ""
        else :
            sql_get_partner_code = "select partner_code from partner where nickname = '"+str(nickname3)+"'"
            df = pd.read_sql(sql_get_partner_code,db)
            if df.empty:
                partner3 =""
                partner_code3 = ""
            else :
                partner3 =str(df.iloc[0,0])
                partner_code3 = "-" + str(df.iloc[0,0])
        partner_code = partner_code1 + partner_code2 + partner_code3
        if str(isActive) == "Hoạt Động" :
            isActive = "1"
        else :
            isActive = "0"
        # password = password.encode('utf-8')
        # hashedPassword = bcrypt.hashpw(password, bcrypt.gensalt())
        #update group
        sqlupdategroup ="update user_group set groupId = (select id from leeon_crm.group where name = 'CUSTOMER') where userId = '"+str(id)+"'"
        cursor.execute(sqlupdategroup)
        #update  partner
        sql_delete_partner = "delete from user_partner where userId = '"+str(id)+"'"
        cursor.execute(sql_delete_partner)
        sql = "update user set isActive = '"+str(isActive)+"', fullname= '"+str(fullname)+"',username='"+str(username)+"',email='"+str(email)+"',phonenumber='"+str(phonenumber)+"',updatedAt=NOW(),partner_code = '"+str(partner_code)+"',partner1 = '"+str(nickname1)+"',partner2 = '"+str(nickname2)+"',partner3 = '"+str(nickname3)+"' where id = '"+str(id)+"'"
        print(sql)
        cursor = db.cursor()
        cursor.execute(sql)
        db.commit()
        return jsonify({'OK': 'OK'})
    except Exception as e:
        logging.error(str(traceback.format_exc()))
        return jsonify({'NOTOK': str(e)}),400
@permision.route('/saovang_list_user', methods=['GET'])
def saovang_list_user():
    session=datetime.datetime.now()
    logging.info(str(session) +"|list_user")
    try:
        db_connection_str = 'mysql+pymysql://leeoncrm:41XmKsO3NBgHPwv@172.17.0.1/leeon_crm'
        db = create_engine(db_connection_str)
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
                    if nameuser =="admin":
                        sql ="SELECT case when isActive = 1 then 'Hoạt Động' else 'Tạm Dừng' end as status, a.id,a.fullname,a.username,a.email,a.phonenumber,c.name'group' FROM (select * from leeon_crm.user where agency_id = 1) a left join leeon_crm.user_group b on a.id = b.userId left join leeon_crm.group c on b.groupId =c.id"
                    else :
                        sql ="SELECT case when isActive = 1 then 'Hoạt Động' else 'Tạm Dừng' end as status,a.id,a.fullname,a.username,a.email,a.phonenumber,c.name'group' FROM (select * from leeon_crm.user where agency_id = 1 and username = '"+str(nameuser)+"') a left join leeon_crm.user_group b on a.id = b.userId left join leeon_crm.group c on b.groupId =c.id"
                    df = pd.read_sql(sql,db)
                    Type_new = pd.Series([])
                    for i in range(len(df)):
                        sqlpartner ="select nickname from partner where id in(select partnerId from user_partner where userId = '"+str(df.iloc[i]['id'])+"')"
                        dfpartner = pd.read_sql(sqlpartner,db)
                        dfpartner=dfpartner.values.tolist()
                        Type_new[i]=str(dfpartner).replace("[","").replace("]","").replace("'","")
                    print(Type_new)
                    df.insert(6, "partner", Type_new)
                    json_records = df.to_json(orient ='records')
                    data = []
                    data = json.loads(json_records)
                    logging.info(str(session) +"|response|Thành công")
                    context = {'data': data,'code': 'OK'}
                    return context
                else :
                    logging.error(str(session) +"|response|token hết hạn")
                    return jsonify({'NOTOK': 'token hết hạn','code':401}),401
            else :
                logging.error(str(session) +"|response|Không có quyền")
                return jsonify({'NOTOK': 'khong co quyen'}),400
    except Exception as e:
        logging.error(str(session) +"|response|" +str(e))
        return jsonify({'NOTOK': str(e)}),400

@permision.route('/saovang_list_user_customer', methods=['GET'])
def saovang_list_user_customer():
    session=datetime.datetime.now()
    logging.info(str(session) +"|list_user")
    try:
        db_connection_str = 'mysql+pymysql://leeoncrm:41XmKsO3NBgHPwv@172.17.0.1/leeon_crm'
        db = create_engine(db_connection_str)
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
                    if str(group_name) =="ADMIN_USER" or str(group_name) =="SUPER_ADMIN_GROUP":
                        sql ="SELECT case when isActive = 1 then 'Hoạt Động' else 'Tạm Dừng' end as status,a.partner1,a.partner2,a.partner3, a.id,a.fullname,a.username,a.email,a.phonenumber,c.name'group' FROM user a , leeon_crm.user_group b ,leeon_crm.group c where a.id = b.userId and b.groupId =c.id and a.partner_code is not null and a.agency_id = '1'"
                    else :
                        sql ="SELECT case when isActive = 1 then 'Hoạt Động' else 'Tạm Dừng' end as status,a.partner1,a.partner2,a.partner3,a.id,a.fullname,a.username,a.email,a.phonenumber,c.name'group' FROM (select * from leeon_crm.user where username = '"+str(nameuser)+"' and partner_code is not null and agency_id = '1') a left join leeon_crm.user_group b on a.id = b.userId left join leeon_crm.group c on b.groupId =c.id"
                    df = pd.read_sql(sql,db)
                    Type_new = pd.Series([])
                    for i in range(len(df)):
                        sqlpartner ="select nickname from partner where id in(select partnerId from user_partner where userId = '"+str(df.iloc[i]['id'])+"')"
                        dfpartner = pd.read_sql(sqlpartner,db)
                        dfpartner=dfpartner.values.tolist()
                        Type_new[i]=str(dfpartner).replace("[","").replace("]","").replace("'","")
                    print(Type_new)
                    df.insert(6, "partner", Type_new)
                    json_records = df.to_json(orient ='records')
                    data = []
                    data = json.loads(json_records)
                    logging.info(str(session) +"|response|Thành công")
                    context = {'data': data,'code': 'OK'}
                    return context
                else :
                    logging.error(str(session) +"|response|token hết hạn")
                    return jsonify({'NOTOK': 'token hết hạn','code':401}),401
            else :
                logging.error(str(session) +"|response|Không có quyền")
                return jsonify({'NOTOK': 'khong co quyen'}),400
    except Exception as e:
        logging.error(str(session) +"|response|" +str(e))
        return jsonify({'NOTOK': str(e)}),400