import cx_Oracle
import pandas as pd
import smtplib
import oracledb as cx_Oracle 
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from ProdConf import username, password, host, port, service_name


from datetime import datetime

current_date = datetime.now()
formatted_date = current_date.strftime("%d-%m-%y")

cx_Oracle.init_oracle_client(lib_dir="C:\oracle_instant_client")
#cx_Oracle.init_oracle_client(lib_dir="/home/oracle/app/oracle/product/19.0.0/client_1/lib")

dns = cx_Oracle.makedsn(host, port, service_name=service_name)
connectionString = f"{username}/{password}@{dns}"

CC_EMAILS    = ["Jeethraj.Poojary@tecnotree.com"]
SENDER_EMAIL = "alertnotification@tecnotree.com"
#SMTP_SERVER = "smtp.office365.com"
SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587
SMTP_USERNAME = "testuserzain@gmail.com"
SMTP_PASSWORD = "wrealiqlebpvbduy"

try:
    connection = cx_Oracle.connect(connectionString)
    cursor = connection.cursor()
    query1 =f"""SELECT D.STATUS "STATUS", D.TABLESPACE_NAME "NAME", 
            TO_CHAR(NVL(A.BYTES / 1024 / 1024 /1024, 0),'99,999,990.90') "SIZE (GB)", 
            TO_CHAR(NVL(A.BYTES - NVL(F.BYTES, 0), 0)/1024/1024 /1024,'99999999.99') "USED (GB)", 
            TO_CHAR(NVL(F.BYTES / 1024 / 1024 /1024, 0),'99,999,990.90') "FREE (GB)", 
            TO_CHAR(NVL((A.BYTES - NVL(F.BYTES, 0)) / A.BYTES * 100, 0), '990.00') "(USED) %"
            FROM SYS.DBA_TABLESPACES D, 
            (SELECT TABLESPACE_NAME, SUM(BYTES) BYTES FROM DBA_DATA_FILES GROUP BY TABLESPACE_NAME) A, 
            (SELECT TABLESPACE_NAME, SUM(BYTES) BYTES FROM DBA_FREE_SPACE GROUP BY TABLESPACE_NAME) F WHERE 
            D.TABLESPACE_NAME = A.TABLESPACE_NAME(+) AND D.TABLESPACE_NAME = F.TABLESPACE_NAME(+) AND NOT 
            (D.EXTENT_MANAGEMENT LIKE 'LOCAL' AND D.CONTENTS LIKE 'TEMPORARY') AND D.TABLESPACE_NAME='USERS' 
            UNION ALL 
            SELECT D.STATUS 
            "STATUS", D.TABLESPACE_NAME "NAME", 
            TO_CHAR(NVL(A.BYTES / 1024 / 1024 /1024, 0),'99,999,990.90') "SIZE (GB)", 
            TO_CHAR(NVL(T.BYTES,0)/1024/1024 /1024,'99999999.99') "USED (GB)",
            TO_CHAR(NVL((A.BYTES -NVL(T.BYTES, 0)) / 1024 / 1024 /1024, 0),'99,999,990.90') "FREE (GB)", 
            TO_CHAR(NVL(T.BYTES / A.BYTES * 100, 0), '990.00') "(USED) %" 
            FROM SYS.DBA_TABLESPACES D, 
            (SELECT TABLESPACE_NAME, SUM(BYTES) BYTES FROM DBA_TEMP_FILES GROUP BY TABLESPACE_NAME) A, 
            (SELECT TABLESPACE_NAME, SUM(BYTES_CACHED) BYTES FROM V$TEMP_EXTENT_POOL GROUP BY TABLESPACE_NAME) T 
            WHERE D.TABLESPACE_NAME = A.TABLESPACE_NAME(+) AND D.TABLESPACE_NAME = T.TABLESPACE_NAME(+) AND  
            D.EXTENT_MANAGEMENT LIKE 'LOCAL' AND D.CONTENTS LIKE 'TEMPORARY' AND D.TABLESPACE_NAME='USERS'"""

    cursor.execute(query1)
    query1_result = cursor.fetchall()

    for row in query1_result:
        status, name, size_gb, used_gb, free_gb, used_percentage = row
        used_percentage = float(used_percentage) 
        if 60 <= used_percentage < 65:
            recipient_email = ["Jeethraj.Poojary@tecnotree.com"]
            recipient_name ='Jeeth'
        elif 66 <= used_percentage < 70: 
            recipient_email = ["Sheetal.Chandran@tecnotree.com"]
            recipient_name ='Sheetal'
        elif used_percentage >= 70:
            recipient_email = ["Jeethraj.Poojary@tecnotree.com"]
            recipient_name ='Jeeth'
        else:
            continue  

        subject = f"Warning: {name} Table Space is {used_percentage}%"
        message= f"Hello {recipient_name},\n\n" \
              f"The Used Percentage for {name} is {used_percentage}%. This requires your attention.\n" \
              f"Please take the necessary actions.\n\n" \
              f"Thanks and Regards,\nZain South Sudan SI Team"
        msg = MIMEText(message)
        msg['Subject'] = subject
        msg['From'] = SMTP_USERNAME
        msg['To'] = ",".join(recipient_email)
        msg['CC']= ", ".join(CC_EMAILS)
        server = smtplib.SMTP(SMTP_SERVER, SMTP_PORT)
        server.starttls()
        server.login(SMTP_USERNAME, SMTP_PASSWORD)    
        server.sendmail(SMTP_USERNAME, recipient_email+ CC_EMAILS, msg.as_string())
        server.quit()
        print(f"Email sent to {recipient_email}")


except Exception as e:
    print(f"Error: {str(e)}")

finally:
    cursor.close()
    connection.close()