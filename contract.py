from flask import Blueprint,send_from_directory,send_file
from flask import request, jsonify,flash
import json

import requests
from requests.auth import HTTPDigestAuth
import pandas as pd
from pandas import json_normalize
import pymysql
from flask_jsonpify import jsonpify
from datetime import datetime
import datetime
import os
from werkzeug.utils import secure_filename
from function import group
import time
import logging
import traceback
contract = Blueprint('contract', __name__)
@contract.route('/listcontracts', methods=['GET'])
def listcontracts():
    session=datetime.datetime.now()
    logging.info(str(session) +"|listcontracts")
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
            logging.info(str(session) +"|listcontracts|"+str(data))
            if True:
                currentDate = time.strftime("%Y-%m-%d %H:%M:%S")
                print(currentDate)
                if str(timetoken) >= str(currentDate) :
                    sql ="SELECT d.name'service',a.begintime,a.expiretime,a.id,a.name,a.contractno,a.contractappendixno,b.nickname, case when a.type =1 then 'Hợp Đồng' else 'Phụ lục' end as type ,a.note,c.file FROM contract a left join partner b on a.partnerid =b.id left join contract_file c on c.contractid =a.id left join service d on a.serviceid=d.id"
                    df = pd.read_sql(sql,db)
                    df['begintime'] = df['begintime'].astype('str')
                    df['expiretime'] = df['expiretime'].astype('str')
                    json_records = df.to_json(orient ='records')
                    data = []
                    data = json.loads(json_records)
                    context = {'data': data,'code': 'OK'}
                    sqllog = "insert into log (name,api,content,type) value ('"+str(nameuser)+"','listcontracts','view','SELECT')"
                    cursor = db.cursor()
                    cursor.execute(sqllog)
                    db.commit()
                    db.close()
                    logging.info(str(session) +"|response | Thành công") 
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
        db.close()
        logging.error(str(session) +"|response|" +str(e))
        return jsonify({'NOTOK': str(e)}),400
@contract.route('/deletecontracts', methods=['DELETE'])
def deletecontracts():
    session=datetime.datetime.now()
    logging.info(str(session) +"|deletecontracts")
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
            logging.info(str(session) +"|deteleaccount|"+str(data))
            if True:
                currentDate = time.strftime("%Y-%m-%d %H:%M:%S")
                print(currentDate)
                if str(timetoken) >= str(currentDate) :
                    record = json.loads(request.data)
                    id = record["id"]
                    sqlcheck= "select count(*) from contract where id ='"+str(id)+"'"
                    df = pd.read_sql(sqlcheck,db)
                    sl = df.iloc[0,0]
                    if int(sl) > 0 :
                        sql="delete from contract where id = '"+str(id)+"'"
                        cursor = db.cursor()
                        cursor.execute(sql)
                        sqllog = "insert into log (name,api,content,type) value ('"+str(nameuser)+"','deletecontracts','id = "+str(id)+"','DELETE')"
                        cursor = db.cursor()
                        cursor.execute(sqllog)
                        db.commit()
                        return jsonify({'OK': 'OK'})
                    else :
                        return jsonify({'OK': 'id không tồn tại'})
                    db.close()
                else :
                    db.close()
                    logging.error(str(session) +"|response|token hết hạn")
                    return jsonify({'NOTOK': 'token hết hạn','code':401}),401
            else :
                db.close()
                logging.error(str(session) +"|response|Không có quyền")
                return jsonify({'NOTOK': 'khong co quyen'}),400
    except Exception as e:
        db.close()
        logging.error(str(session) +"|response|" +str(e))
        return jsonify({'NOTOK': str(e)})
@contract.route('/insertcontracts', methods=['POST'])
def insertcontracts():
    session=datetime.datetime.now()
    logging.info(str(session) +"|insertcontracts")
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
            logging.info(str(session) +"|insertaccount|"+str(data))
            if True:
                currentDate = time.strftime("%Y-%m-%d %H:%M:%S")
                print(currentDate)
                if str(timetoken) >= str(currentDate) :
                    now = datetime.datetime.now()
                    logging.info(str(session) +"|input|"+str(request.form))
                    logging.info(str(session) +"|input|"+str(request.files))
                    current_time = now.strftime("%Y-%m-%d %H:%M:%S")
                    current_time=str(current_time).replace(" ","").replace(":","").replace("-","")
                    contractno = request.form['contractno']
                    nickname = request.form['nickname']
                    note = request.form['note']
                    service = request.form['service']
                    begintime= request.form['begintime']
                    expiretime= request.form['expiretime']
                    if 'file' not in request.files:
                        flash('No file part')
                        logging.error(str(session) +"|response|No file")
                        return jsonify({'NOTOK': 'No file'})
                    file = request.files['file']
                    if file.filename == '':
                        logging.error(str(session) +"|response|No file selected")
                        return jsonify({'NOTOK': 'No file selected'})
                    filename = str(current_time) +"_"+ str(secure_filename(file.filename))
                    UPLOAD_FOLDER = '/home/dungnt/api_crm_v2/fileupload'
                    file.save(os.path.join(UPLOAD_FOLDER, filename))
                    sqlcontract="insert into contract(contractno,partnerid,type,note,createdtime,updatedtime,serviceid,begintime,expiretime) value ('"+str(contractno)+"',(select id from partner where nickname = '"+str(nickname)+"'),'1','"+str(note)+"','"+str(current_time)+"','"+str(current_time)+"',(select id from service where name = '"+str(service)+"'),'"+str(begintime)+"','"+str(expiretime)+"')"
                    logging.info(str(session) +"|insert contract|"+str(sqlcontract))
                    cursor = db.cursor()
                    cursor.execute(sqlcontract)
                    sqlfile ="insert into contract_file(contractid,file) value ((select id from contract where contractno = '"+str(contractno)+"' order by createdtime desc limit 1),'"+str(filename)+"')"
                    logging.info(str(session) +"|insert contract_file|"+str(sqlfile))
                    cursor.execute(sqlfile)
                    sqllog = "insert into log (name,api,content,type) value ('"+str(nameuser)+"','insertcontracts','"+str(request.form).replace("'","")+"','INSERT')"
                    cursor = db.cursor()
                    cursor.execute(sqllog)
                    db.commit()
                    logging.info(str(session) +"|response|Thành công")
                    return jsonify({'OK': 'OK'})
                else :
                    db.close()
                    logging.error(str(session) +"|response|token hết hạn")
                    return jsonify({'NOTOK': 'token hết hạn','code':401}),401
            else :
                db.close()
                logging.error(str(session) +"|response|Không có quyền")
                return jsonify({'NOTOK': 'khong co quyen'}),400
    except Exception as e:
        db.close()
        logging.error(str(session) +"|response|" +str(e))
        return jsonify({'NOTOK': str(e)})
@contract.route('/updatecontract', methods=['POST'])
def updatecontract():
    session=datetime.datetime.now()
    logging.info(str(session) +"|updatecontract")
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
            logging.info(str(session) +"|updatecontract|"+str(data))
            if True:
                currentDate = time.strftime("%Y-%m-%d %H:%M:%S")
                print(currentDate)
                if str(timetoken) >= str(currentDate) :
                    logging.info(str(session) +"|input|"+str(request.form))
                    logging.info(str(session) +"|input|"+str(request.files))
                    now = datetime.datetime.now()
                    current_time = now.strftime("%Y-%m-%d %H:%M:%S")
                    current_time=str(current_time).replace(" ","").replace(":","").replace("-","")
                    contractno = request.form['contractno']
                    nickname = request.form['nickname']
                    note = request.form['note']
                    service = request.form['service']
                    sqlservice ="select * from service where name = '"+str(service)+"'"
                    dfservice = pd.read_sql(sqlservice,db)
                    serviceid = dfservice.iloc[0]['id']
                    begintime= request.form['begintime']
                    expiretime= request.form['expiretime']
                    file_status =1
                    if 'file' not in request.files:
                        flash('No file part')
                        logging.error(str(session) +"|response|No file")
                        file_status = 0
                        # return jsonify({'NOTOK': 'No file'})
                    if file_status == 1:
                        file = request.files['file']
                        if file.filename == '':
                            logging.error(str(session) +"|response|No file selected")
                            file_status = 0
                        # return jsonify({'NOTOK': 'No file selected'})
                        filename = str(current_time) +"_"+ str(secure_filename(file.filename))
                        UPLOAD_FOLDER = '/home/dungnt/api-crm/fileupload'
                        file.save(os.path.join(UPLOAD_FOLDER, filename))
                    id = request.form["id"]
                    sql="update contract set serviceid = '"+str(serviceid)+"',contractno = '"+str(contractno)+"',partnerid = (select id from partner where nickname = '"+str(nickname)+"'),type = '1',note = '"+str(note)+"',updatedtime='"+str(current_time)+"' where id ='"+str(id)+"'"
                    logging.info(str(session) +"|update contract|"+str(sql))
                    cursor = db.cursor()
                    cursor.execute(sql)
                    if file_status == 1:
                        sqlcheck_contract_file = "select * from contract_file where contractid = '"+str(id)+"'"
                        dfsqlcheck_contract_file = pd.read_sql(sqlcheck_contract_file,db)
                        if dfsqlcheck_contract_file.empty:
                            sqlfile ="insert into contract_file(contractid,file) value ('"+str(id)+"','"+str(filename)+"')"
                        else :
                            sqlfile ="update contract_file set file ='"+str(filename)+"' where contractid = '"+str(id)+"'"
                        logging.info(str(session) +"|insert contract_file|"+str(sqlfile))
                        cursor.execute(sqlfile)
                    sqllog = "insert into log (name,api,content,type) value ('"+str(nameuser)+"','updatecontract','"+str(request.form).replace("'","")+"','UPDATE')"
                    cursor = db.cursor()
                    cursor.execute(sqllog)
                    db.commit()
                    db.close()
                    logging.info(str(session) +"|response|Thành công")
                    return jsonify({'OK': 'OK'})
                else :
                    db.close()
                    logging.error(str(session) +"|response|token hết hạn")
                    return jsonify({'NOTOK': 'token hết hạn','code':401}),401
            else :
                db.close()
                logging.error(str(session) +"|response|Không có quyền")
                return jsonify({'NOTOK': 'khong co quyen'}),400
    except Exception as e:
        db.close()
        logging.error(str(session) +"|response|" +str(traceback.format_exc()))
        return jsonify({'NOTOK': str(e)})
@contract.route('/listservice', methods=['GET'])
def listservice():
    session=datetime.datetime.now()
    logging.info(str(session) +"|listservice")
    try :
        db = pymysql.connect(host="172.17.0.1",    # your host, usually localhost
                     user="leeoncrm",         # your username
                     passwd="41XmKsO3NBgHPwv",  # your password
                     db="leeon_crm",
                     port=3306
                     )
        sql ="SELECT id,name FROM leeon_crm.service"
        df = pd.read_sql(sql,db)
        logging.info(str(session) +"|query|"+str(df.values.tolist()))
        json_records = df.to_json(orient ='records')
        data = []
        data = json.loads(json_records)
        logging.info(str(session) +"|response|Thành công")
        context = {'data': data,'code': 'OK'}
        db.close() 
        return context
    except Exception as e:
        db.close()
        logging.error(str(session) +"|response|" +str(e))
        return jsonify({'NOTOK': str(e)})
@contract.route('/download_contract_file', methods=['POST'])
def download_contract_file():
    session=datetime.datetime.now()
    logging.info(str(session) +"|download_contract_file")
    try :
        db = pymysql.connect(host="172.17.0.1",    # your host, usually localhost
                     user="leeoncrm",         # your username
                     passwd="41XmKsO3NBgHPwv",  # your password
                     db="leeon_crm",
                     port=3306
                     )
        record = json.loads(request.data)
        logging.info(str(session) +"|input|"+str(request.data))
        name = record["name"] 
        session=datetime.datetime.now()
        path = os.path.join(os.getcwd(),'fileupload',name)
        logging.info(str(session) +"|path |"+ str(path))
        logging.info(str(session) +"|response | Thành công")
        return send_file(path, as_attachment=True)
    except Exception as e:
        db.close()
        logging.error(str(session) +"|response|" +str(e))
        return jsonify({'NOTOK': str(e)}),400
@contract.route('/insertcontract_append', methods=['POST'])
def insertcontract_append():
    try :
        session=datetime.datetime.now()
        db = pymysql.connect(host="172.17.0.1",    # your host, usually localhost
                     user="leeoncrm",         # your username
                     passwd="41XmKsO3NBgHPwv",  # your password
                     db="leeon_crm",
                     port=3306
                     )
        now = datetime.datetime.now()
        print(request.files)
        print(request.form)
        current_time = now.strftime("%Y-%m-%d %H:%M:%S")
        current_time=str(current_time).replace(" ","").replace(":","").replace("-","")
        contractno = request.form['contractno']
        contractappendixno = request.form['contractappendixno']
        nickname = request.form['nickname']
        note = request.form['note']
        service = request.form['service']
        begintime= request.form['begintime']
        expiretime= request.form['expiretime']
        if 'file' not in request.files:
            flash('No file part')
            return jsonify({'notOK': 'OK'})
        file = request.files['file']
        if file.filename == '':
            flash('No selected file')
            return jsonify({'NOTOK': 'OK'})
        filename = str(current_time) +"_"+ str(secure_filename(file.filename))
        UPLOAD_FOLDER = '/home/dungnt/api-crm/fileupload'
        file.save(os.path.join(UPLOAD_FOLDER, filename))
        sqlcontract="insert into contract(contractno,contractappendixno,partnerid,type,note,createdtime,updatedtime,serviceid,begintime,expiretime) value ('"+str(contractno)+"','"+str(contractappendixno)+"',(select id from partner where nickname = '"+str(nickname)+"'),'2','"+str(note)+"','"+str(current_time)+"','"+str(current_time)+"',(select id from service where name = '"+str(service)+"'),'"+str(begintime)+"','"+str(expiretime)+"')"
        cursor = db.cursor()
        cursor.execute(sqlcontract)
        sqlfile ="insert into contract_file(contractid,file) value ((select id from contract where contractappendixno = '"+str(contractappendixno)+"' order by createdtime desc limit 1),'"+str(filename)+"')"
        cursor.execute(sqlfile)
        db.commit()
        logging.info(str(session) +"|response | Thành công")
        return jsonify({'OK': 'OK'})
    except Exception as e:
        db.close()
        logging.error(str(session) +"|response|" +str(e))
        return jsonify({'NOTOK': str(e)}),400
@contract.route('/listhopdong', methods=['GET'])
def listhopdong():
    try :
        db = pymysql.connect(host="172.17.0.1",    # your host, usually localhost
                     user="leeoncrm",         # your username
                     passwd="41XmKsO3NBgHPwv",  # your password
                     db="leeon_crm",
                     port=3306
                     )
        sql ="SELECT contractno FROM contract where type = '1'"
        df = pd.read_sql(sql,db)
        json_records = df.to_json(orient ='records')
        data = []
        data = json.loads(json_records)
        context = {'data': data,'code': 'OK'}
        db.close() 
        return context
    except :
        db.close()  
        return jsonify({'Lỗi': 'Quá nhiều lỗi'}),400