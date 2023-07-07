from datetime import datetime
import datetime
a= "2023-01-06"
now = datetime.datetime.strptime(a, '%Y-%m-%d')
thang_now = now.month
thang_last = thang_now - 1
if thang_last == 0:
    thang_last = 12
last = now.replace(month=thang_last)
print(last)
