import flask
from flask_cors import CORS
from partner import partner
from report import report
from other import other
from mapping import mapping
from account import account
from ip import ip
from telco import telco
from routing import routing
from partnerdetail import partnerdetail
from redash import redash
from login import login
from vendor import vendor
from vendor_contact import vendor_contact
from vendor_package import vendor_package
from number_owner import number_owner
from number_member import number_member
from contact import contact
from permision import permision
from ccu import ccu
from login_saovang import login_saovang
from brandname import brandname
from contract import contract
from sms_partner import sms_partner
from sms_otp import sms_otp
from sms_brandname import sms_brandname
from sms_adv import sms_adv
from service_config import service_config
from report_v2 import report_v2
from report_exception import report_exception
from invoice import invoice
from daily import daily
import datetime
import logging
from ticket import ticket
import multiprocessing
from flask import request, jsonify
session=datetime.datetime.now()
logfile = "log/api-log_" +str(session)[0:10]+".txt"
logging.basicConfig(filename=logfile,
                    filemode='a',
                    format='%(asctime)s,%(msecs)d %(name)s %(process)d %(levelname)s %(message)s',
                    datefmt='%H:%M:%S',
                    level=logging.DEBUG)
app = flask.Flask(__name__)
app.register_blueprint(partner)
app.register_blueprint(report)
app.register_blueprint(report_exception)
app.register_blueprint(other)
app.register_blueprint(mapping)
app.register_blueprint(ticket)
app.register_blueprint(account)
app.register_blueprint(login_saovang)
app.register_blueprint(ip)
app.register_blueprint(telco)
app.register_blueprint(routing)
app.register_blueprint(partnerdetail)
app.register_blueprint(redash)
app.register_blueprint(login)
app.register_blueprint(vendor)
app.register_blueprint(vendor_contact)
app.register_blueprint(vendor_package)
app.register_blueprint(number_owner)
app.register_blueprint(number_member)
app.register_blueprint(report_v2)
app.register_blueprint(contact)
app.register_blueprint(ccu)
app.register_blueprint(permision)
app.register_blueprint(brandname)
app.register_blueprint(contract)
app.register_blueprint(sms_partner)
app.register_blueprint(sms_brandname)
app.register_blueprint(sms_adv)
app.register_blueprint(sms_otp)
app.register_blueprint(service_config)
app.register_blueprint(invoice)
app.register_blueprint(daily)
CORS(app)
UPLOAD_FOLDER = '/home/dungnt/flask/'
app.secret_key = "secret key"
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
# app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024
app.config["DEBUG"] = True
@app.route("/")
def hello():
    return "Hello World!"
if __name__ == "__main__":
    app.run(host="0.0.0.0",port="5000")
