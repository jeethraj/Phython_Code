import cx_Oracle
import pandas as pd
import smtplib
import oracledb as cx_Oracle 
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
#from ConfData import username, password, host, port, service_name
from ProdConf import username, password, host, port, service_name
from datetime import datetime, time, timedelta


time_in_header=pd.Timestamp.now().strftime('%Y-%m-%d')

RECIPIENT_EMAIL = ["jeethraj.poojary@tecnotree.com"]
SENDER_EMAIL = "DailyReports@gmail.com"
CC_EMAILS    = CC_EMAILS = ["jeethraj.poojary@tecnotree.com", "gunasekar.ravi@tecnotree.com","sheetal.chandran@tecnotree.com"]
#CC_EMAILS    = CC_EMAILS = ["jeethraj.poojary@tecnotree.com"]


SMTP_SERVER = "smtp.office365.com"
SMTP_PORT = 587  # TLS port
SMTP_USERNAME = "Tecno.Tree@ss.zain.com"  # Your Office 365 email address
SMTP_PASSWORD = "Zain@2022"  # Your Office 365 password

cx_Oracle.init_oracle_client(lib_dir="C:\oracle_instant_client")
#cx_Oracle.init_oracle_client(lib_dir="/home/oracle/app/oracle/product/19.0.0/client_1/lib")
dns = cx_Oracle.makedsn(host, port, service_name=service_name)
#print(f"{dns=}")
connectionString = f"{username}/{password}@{dns}"

QUERY1_HEADERS = ["SERVICE_ID_V", "STATUS_V","ACTION_DATE_DT"]

try:
    # Create a database connection,
    connection = cx_Oracle.connect(connectionString)
    #connection = cx_Oracle.connect(username , password, dns)
    cursor = connection.cursor()

    ProvRejection="SELECT SERVICE_ID_V,STATUS_V,ACTION_DATE_DT FROM CB_SUBS_PROVISIONING WHERE STATUS_V='R' AND TRUNC(ACTION_DATE_DT)=TO_DATE(SYSDATE)"

    cursor.execute(ProvRejection)
    ProvRejectionRes = cursor.fetchall()

    cursor.close()
    connection.close()

    df1 = pd.DataFrame(ProvRejectionRes, columns=QUERY1_HEADERS)
    df_html = df1.to_html(classes='table table-striped', index=False)
    
    EMAIL_SUBJECT = "Provisioning Rejection - " + f"{time_in_header}"

    EMAIL_BODY = f"""
            <html>
            <body>
                <p>Hi Team,</p>
                <p>There are some rejection in Provisioning. Please check</p>
                </br>
                <p>{df_html}</p>
                """

    msg = MIMEMultipart()
    msg['From'] = SMTP_USERNAME
    msg['To'] = ", ".join(RECIPIENT_EMAIL)
    msg['CC'] = ", ".join(CC_EMAILS)
    msg['Subject'] = EMAIL_SUBJECT
    msg.attach(MIMEText(EMAIL_BODY, 'html'))

    
    # Send the email
    with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
        server.starttls()
        server.login(SMTP_USERNAME, SMTP_PASSWORD)
        server.sendmail(SMTP_USERNAME, RECIPIENT_EMAIL + CC_EMAILS, msg.as_string())


    print("Email sent successfully")
except Exception as e:
    print(f"Error sending email: {str(e)}")    