
import cx_Oracle
import pandas as pd
import smtplib
import oracledb as cx_Oracle 
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from ConfData import username, password, host, port, service_name


from datetime import datetime

current_date = datetime.now()
formatted_date = current_date.strftime("%d-%m-%y")

cx_Oracle.init_oracle_client(lib_dir="C:\oracle_instant_client")

dns = cx_Oracle.makedsn(host, port, service_name=service_name)
connectionString = f"{username}/{password}@{dns}"

RECIPIENT_EMAIL = ["Gunasekar.Ravi@tecnotree.com"]
RECIPIENT_NAME = 'Guna'
SENDER_EMAIL = "DailyReports@gmail.com"
CC_EMAILS    = CC_EMAILS = ["jeethraj.poojary@tecnotree.com","sheetal.chandran@tecnotree.com"]

SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587
SMTP_USERNAME = "testuserzain@gmail.com"
SMTP_PASSWORD = "testuserzain@123"
SMTP_PASSWORD = "wrealiqlebpvbduy"

try:
    connection = cx_Oracle.connect(connectionString)
    cursor = connection.cursor()
    query1 =f"""SELECT * FROM CUP_STATUS_REPORT 
                WHERE TRUNC(PROCESSED_DATE_DT) = TRUNC(SYSDATE) 
                AND FILE_NAME_V LIKE 'cdr_0%' 
                AND PROCESSED_DATE_DT > SYSDATE - (0/24)  -- Within the last hour
                ORDER BY PROCESSED_DATE_DT DESC"""

    cursor.execute(query1)
    records = cursor.fetchall()

    if not records:
        subject = f"Warning: Uploader is Receving CDR/EDR"
        message= f"Hello {RECIPIENT_NAME},\n\n" \
              f"Uploader has not received any CDR/EDR in the last hour.\n" \
              f"Please investigate.\n\n" \
              f"Thanks and Regards,\nZain South Sudan SI Team"
        msg = MIMEText(message)
        msg['Subject'] = subject
        msg['From'] = SMTP_USERNAME
        msg['To'] = ", ".join(RECIPIENT_EMAIL)
        msg['CC']= ",  ".join(CC_EMAILS)
        server = smtplib.SMTP(SMTP_SERVER, SMTP_PORT)
        server.starttls()   
        server.sendmail(SMTP_USERNAME, RECIPIENT_EMAIL + CC_EMAILS, msg.as_string())
        server.quit()
        print(f"Email sent to {RECIPIENT_EMAIL}")


except Exception as e:
    print(f"Error: {str(e)}")

finally:
    cursor.close()
    connection.close()