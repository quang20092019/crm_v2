import base64
import json
import time
from flask import Blueprint
from flask import request, jsonify
import json
import traceback
import requests
from requests.auth import HTTPDigestAuth
import pandas as pd
from pandas import json_normalize
import pymysql
from flask_jsonpify import jsonpify
from datetime import datetime
import email
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from email import encoders
from email.header import Header
from itertools import chain
import logging
def group():
    headers = request.headers
    bearer = headers.get('Authorization')    # Bearer YourTokenHere
    print(bearer)
    if str(bearer) =="None":
        message="chua truyen token"
        return message
    else :
        token = bearer.split()[1]  # YourTokenHere
        base64_bytes = str(token).encode('ascii')
        message_bytes = base64.b64decode(base64_bytes)
        message = message_bytes.decode('ascii')
        message = json.loads(message)
        return message
def connect_db():
    db = pymysql.connect(host="172.17.0.1",    # your host, usually localhost
                     user="leeoncrm",         # your username
                     passwd="41XmKsO3NBgHPwv",  # your password
                     db="leeon_crm",
                     port=3306
                     )
    return db
def sendmail(subject,body):
    try:
        logging.info("send mail")
        logging.info("subject: "+str(subject))
        logging.info("body: "+str(body))
        # to=['nguyen.tiendung22@gmail.com']
        to=['NOC@leeon.vn']
        cc=['nguyen.tiendung22@gmail.com','Ngoc.nt@leeon.vn','vinh.pt@leeon.vn']
        content = MIMEText(body, 'html')
        mailuser='support@leeon.vn'
        mailpassword='L330n#123'
        mailserver='smtp.office365.com'
        msg = MIMEMultipart()
        msg['From'] = 'support@leeon.vn'
        msg['To'] =','.join(to)
        msg['Cc']=','.join(cc)   
        msg['Subject'] = "%s" % Header(subject, 'utf-8')
        toAddress = to + cc
        logging.info("toAddress: "+str(toAddress))
        msg.attach(content)
        mailServer = smtplib.SMTP(mailserver, 25)
        mailServer.ehlo()
        mailServer.starttls()
        mailServer.ehlo()
        mailServer.login(mailuser, mailpassword)
        mailServer.sendmail(mailuser, toAddress, msg.as_string())
        mailServer.quit()
        logging.info("send mail success")
    except Exception as e:
        logging.error("send mail fail")
        logging.error(str(traceback.format_exc()))
def sendmail_update(subject,to,body):
    try:
        cc=['nguyen.tiendung22@gmail.com','Ngoc.nt@leeon.vn','vinh.pt@leeon.vn']
        logging.info("send mail")
        logging.info("to: "+str(to))
        content = MIMEText(body, 'html')
        mailuser='support@leeon.vn'
        mailpassword='L330n#123'
        mailserver='smtp.office365.com'
        msg = MIMEMultipart()
        msg['From'] = 'support@leeon.vn'
        msg['To'] = str(to)
        msg['Cc']=','.join(cc)
        logging.info("cc: "+str(msg['Cc']))
        msg['Subject'] = "%s" % Header(subject, 'utf-8')
        toAddress = str(to) +","+ str(msg['Cc'])
        logging.info("toAddress: "+str(toAddress))
        msg.attach(content)
        mailServer = smtplib.SMTP(mailserver, 25)
        mailServer.ehlo()
        mailServer.starttls()
        mailServer.ehlo()
        mailServer.login(mailuser, mailpassword)
        mailServer.sendmail(mailuser, toAddress, msg.as_string())
        mailServer.quit()
        logging.info("send mail success")
    except Exception as e:
        logging.error("send mail fail")
        logging.error(str(traceback.format_exc()))