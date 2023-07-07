[supervisor]
systemctl stop supervisord
systemctl start supervisord
[sms-job]
supervisorctl start api_crm
supervisorctl stop api_crm
supervisorctl restart api_crm
supervisorctl status api_crm
[api]
supervisorctl start api_crm
supervisorctl stop api_crm
supervisorctl restart api_crm

[run command test]
/home/dungnt/api_crm/bin/gunicorn -c /home/dungnt/api_crm/gunicorn_config.py api:app