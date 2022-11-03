from msilib.schema import Error
import sys
import os 
import requests 
import cx_Oracle 
import psycopg2
from configparser import ConfigParser
 
sys.setrecursionlimit(3000)
os.putenv("NLS_LANG", "KOREAN_KOREA.KO16KSC5601") 

# 0. Read Config
# Oracle Configurations
config = ConfigParser()
#config.load('config.ini')
config.read('config.ini')

"""
url = config['db_config']['url']
id = config['db_config']['id']
pwd = config['db_config']['pwd']

# SFDC Configurations
url_token = config['sfdc_config']['clurl_token']
client_id=config['sfdc_config']['client_id']
client_secret=config['sfdc_config']['client_secret']
redirect_url=config['sfdc_config']['redirect_url']
password=config['sfdc_config']['password']
username=config['sfdc_config']['username']
grant_type=config['sfdc_config']['grant_type']
"""


# 1. Create Token

urlT = "https://ison-220103-606-demo.my.salesforce.com/services/oauth2/token"
formData = { 
    "client_id":"3MVG9p1Q1BCe9GmDz7UJYR_LmWKt.XR56989uI8M_Av7iDxaHaT8m2ZstAcoy95Bh6g7.UfNhElip8gJfwj3a",
    "client_secret":"6DF916E50C83AD97702B90532BC1E618F2493F65FC2CEC00532CFB877A5090CF",
    "redirect_url":"https://login.salesforce.com/services/oauth2/success",
    "password":"msk@1147eqPGx9b6bVoN2NEWMgN0lOLCo",
    "username":"mskang@ioss.demo",
    "grant_type":"password"
}

responseT = requests.post(urlT, 
                         data=formData,  
                         timeout=10000
                         )
print("----------------------------response.request.body-----------------------------")
sfdcToken = responseT.json()['access_token']


# 2. Select Oracle Blob
def output_type_handler(cursor, name, default_type, size, precision, scale):
    if default_type == cx_Oracle.DB_TYPE_CLOB:
        return cursor.var(cx_Oracle.DB_TYPE_LONG, arraysize=cursor.arraysize)
    if default_type == cx_Oracle.DB_TYPE_BLOB:
        return cursor.var(cx_Oracle.DB_TYPE_LONG_RAW, arraysize=cursor.arraysize)

con = cx_Oracle.connect("salestest", "salestest", "121.130.30.18:1521/orcl", encoding="UTF-8")
cursor = con.cursor()
nKey = 1
con.outputtypehandler = output_type_handler
#cursor.execute("select bb from BLOB_TBL where id = :1", [nKey])

# --- Read Oracle Data
cursor.execute("select file_name, file_blob, seq  from attachments where seq = 1") 
filename = ""
binaryData = ""
count=0
blob_data = cursor.fetchall()
url = "https://ison-220103-606-demo.my.salesforce.com/services/data/v55.0/connect/files/users/me"
header = {"Authorization":"OAuth "+sfdcToken+""}

try:
    for i in blob_data:  
        count += 1
        filename = i[0]+""
        binaryData = i[1]
        seq = i[2]
        jsonData = "{ \"title\":\"" + filename + "\"}"
# 3. Insert File in SFDC
        response = requests.post(url,  
                            json={"json": jsonData }, 
                            files = {"fileData" : (filename, binaryData, 'application/octet-stream')},
                            headers=header,
                            timeout=10000
                            )
        print("ContentDocumentID:"+response.json()['id']+", Attachments Seq:" + str(i[2])) 
# 4. Insert Transfered Data in PostgresDB
        # Postgresql : SFDC로 이관된 파일의 정보를 tranfer_file_info 테이블에 저장한다.
        contentDocumentid = response.json()['id']
        connection = psycopg2.connect(
        host="ec2-54-159-22-90.compute-1.amazonaws.com",
        dbname="d861bee0ubnc3h",
        user = 'tldbzpuvqkkzkn',
        password = '6e6841424b971cedbbd5d6f98414e90c85b73e6dc789c1533f97945ca744828d')
        postcursor = connection.cursor()
        postcursor.execute("INSERT INTO public.tranfer_file_info \
                            (seq, contentdocumentid, filename, externalid) \
                            VALUES(nextval('trans_seq'), %s, %s, %s)", (contentDocumentid, filename, seq, seq))
        connection.commit()
except Exception as e:
# 5. IF Exception, Rollback ContentDocument Data in SFDC
    # if occure exception, delete contentDocument data.
    delUrl = "https://ison-220103-606-demo.my.salesforce.com/services/data/v50.0/sobjects/ContentDocument/"+contentDocumentid
    response2 = requests.delete(url=delUrl, headers=header, timeout=10000)
    print("Occur Custom Exception_Rollback:"+str(response2.content))
    print("Occur Custom Exception ", e)
finally:
# 6. Finally Close DB Connection and Cursor
    if con:
        cursor.close()
        con.close()
        print("Oracle connection is closed")
    if connection:
        postcursor.close()
        connection.close()
        print("PostgreSQL connection is closed")


