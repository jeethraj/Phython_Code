import cx_Oracle
import pandas as pd
import smtplib
import oracledb as cx_Oracle 
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
#from ConfData import username, password, host, port, service_name
from ProdConf import username, password, host, port, service_name


from datetime import datetime

current_date = datetime.now()
formatted_date = current_date.strftime("%d-%m-%y")

# Initialize cx_Oracle to use Oracle client libraries
cx_Oracle.init_oracle_client(lib_dir="C:\oracle_instant_client")

dns = cx_Oracle.makedsn(host, port, service_name=service_name)
#print(f"{dns=}")
connectionString = f"{username}/{password}@{dns}"

# Email settings
#RECIPIENT_EMAIL = ["sheetal.chandran@tecnotree.com"]
RECIPIENT_EMAIL = ["jeethraj.poojary@tecnotree.com"]
SENDER_EMAIL = "DailyReports@gmail.com"
CC_EMAILS    = CC_EMAILS = ["jeethraj.poojary@tecnotree.com", "gunasekar.ravi@tecnotree.com"]
SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587
SMTP_USERNAME = "testuserzain@gmail.com"
#SMTP_PASSWORD = "testuserzain@123"
SMTP_PASSWORD = "wrealiqlebpvbduy"

# Query results output files
QUERY1_OUTPUT = f"Onboardings_{formatted_date}.csv"
QUERY2_OUTPUT = f"CDR_{formatted_date}.csv"
QUERY3_OUTPUT = f"EDR_{formatted_date}.csv"

# Headers for each query result
QUERY1_HEADERS = ["USERNAME", "STATUS", "PROVISION", "CUSTOMER_NUMBER", "ACTION_DATE_DT", "OFFER_DESC_V"]
QUERY2_HEADERS = ["CUSTOMER_NUMBER", "CALL_TYPE_LABEL", "TOTAL_CALLS"]
QUERY3_HEADERS = ["CUSTOMER_NUMBER", "EVENT_TYPE_V_LABEL", "TOTAL_CALLS"]

try:
    # Create a database connection
    connection = cx_Oracle.connect(connectionString)
    #connection = cx_Oracle.connect(username , password, dns)
    cursor = connection.cursor()

    # Query 1
    query1 = """
        SELECT
            USERNAME,
            STATUS,
            PROVISION,
            CUSTOMER_NUMBER,
            TO_CHAR(ACTION_DATE_DT, 'YY-MM-DD HH:MI:SS.FF6 AM') AS ACTION_DATE_FORMATTED,
            CASE
                WHEN COUNT(OFFER_DESC_V) > 0
                THEN LISTAGG(REPLACE(OFFER_DESC_V, '~1.0', ''), ', ') WITHIN GROUP (ORDER BY REPLACE(OFFER_DESC_V, '~1.0', '') DESC)
                ELSE NULL
            END AS OFFER_DESC_V
        FROM (
            SELECT DISTINCT
                b.USER_NAME_V AS USERNAME,
                a.STATUS_V AS STATUS,
                a.ACTION_CODE_V AS PROVISION,
                a.SERVICE_ID_V AS CUSTOMER_NUMBER,
                a.ACTION_DATE_DT,
                d.OFFER_DESC_V
            FROM
                CB_SUBS_PROVISIONING a
                JOIN CB_USERS b ON a.USER_CODE_N = b.USER_CODE_N
                LEFT JOIN CB_SUBS_OFFER_DETAILS c ON a.ACCOUNT_LINK_CODE_N = c.ACCOUNT_LINK_CODE_N
                LEFT JOIN CB_OFFERS d ON c.offer_code_v = d.offer_code_V
            WHERE
                TRUNC(a.ACTION_DATE_DT) =  TRUNC(SYSDATE)
                AND a.ACTION_CODE_V NOT IN ('SPCK')
                AND a.ACTION_CODE_V IN ('DURP')
                AND a.STATUS_V IN ('Q', 'E', 'R')
        )
        GROUP BY USERNAME, STATUS, PROVISION, CUSTOMER_NUMBER, ACTION_DATE_DT
        ORDER BY ACTION_DATE_DT DESC
    """

    cursor.execute(query1)
    query1_result = cursor.fetchall()

    # Query 2
    query2 = """
        SELECT
            CUSTOMER_NUMBER,
            CALL_TYPE_LABEL,
            COALESCE(SUM(CALL_COUNT), 0) AS TOTAL_CALLS
        FROM (
            SELECT
                a.SERVICE_ID_V AS CUSTOMER_NUMBER,
                b.CALL_TYPE_V,
                COUNT(b.CALL_TYPE_V) AS CALL_COUNT,
                CASE
                    WHEN b.CALL_TYPE_V = '18' THEN 'DATA'
                    WHEN b.CALL_TYPE_V = '001' THEN 'VOICE OUTGOING'
                    WHEN b.CALL_TYPE_V = '002' THEN 'VOICE INCOMING'
                    WHEN b.CALL_TYPE_V = '030' THEN 'SMS INCOMING'
                    WHEN b.CALL_TYPE_V = '031' THEN 'SMS OUTGOING'
                END AS CALL_TYPE_LABEL
            FROM CB_SUBS_PROVISIONING a
            LEFT JOIN CB_PREPAID_UPLOAD_ALL_CDRS b ON b.SERVICE_IDENTIFIER_V = a.SERVICE_ID_V
            WHERE TRUNC(a.ACTION_DATE_DT) BETWEEN TO_DATE('20230808', 'yyyymmdd') AND TRUNC(SYSDATE)
            AND a.ACTION_CODE_V IN ('DURP')
            GROUP BY a.SERVICE_ID_V, b.CALL_TYPE_V
        )
        GROUP BY CUSTOMER_NUMBER, CALL_TYPE_LABEL
        ORDER BY CALL_TYPE_LABEL
    """

    cursor.execute(query2)
    query2_result = cursor.fetchall()

    # Query 3
    query3 = """
        SELECT
            CUSTOMER_NUMBER,
            EVENT_TYPE_V_LABEL,
            COALESCE(SUM(EVENT_COUNT), 0) AS TOTAL_CALLS
        FROM (
            SELECT
                a.SERVICE_ID_V AS CUSTOMER_NUMBER,
                b.EVENT_TYPE_V,
                COUNT(b.EVENT_TYPE_V) AS EVENT_COUNT,
                CASE
                    WHEN b.EVENT_TYPE_V = '40' THEN 'RECHARGE'
                    WHEN b.EVENT_TYPE_V = '39' THEN 'BALANCE CHANGE'
                    WHEN b.EVENT_TYPE_V = '6' THEN 'OFFER ACTIVATION'
                    ELSE b.EVENT_TYPE_V
                END AS EVENT_TYPE_V_LABEL
            FROM CB_SUBS_PROVISIONING a
            LEFT JOIN CB_PREPAID_UPLOAD_ALL_EDRS b ON b.CHARGING_PARTY_NUMBER_V = a.SERVICE_ID_V
            WHERE TRUNC(a.ACTION_DATE_DT) BETWEEN TO_DATE('20230808', 'yyyymmdd') AND TRUNC(SYSDATE)
            AND a.ACTION_CODE_V IN ('DURP')
            GROUP BY a.SERVICE_ID_V, b.EVENT_TYPE_V
        )
        GROUP BY CUSTOMER_NUMBER, EVENT_TYPE_V_LABEL
        ORDER BY EVENT_TYPE_V_LABEL
    """

    cursor.execute(query3)
    query3_result = cursor.fetchall()
    
    query4 ="SELECT COUNT(1) FROM GSM_SERVICE_MAST Where TRUNC(ACTIVATION_DATE_D)=TRUNC(SYSDATE)"
    cursor.execute(query4)
    query4_result = cursor.fetchall()
    TotalOnboardings = query4_result[0][0]

    query5 = "SELECT COUNT(*) FROM CB_PREPAID_UPLOAD_ALL_EDRS WHERE  TRUNC(EVENT_DATE_TIME_DT)=TO_DATE(SYSDATE) AND EVENT_TYPE_V=6 and ATTRIBUTE1_V !=10024"
    cursor.execute(query5)
    query5_result = cursor.fetchall()
    TotalOffAct = query5_result[0][0]
     
    query6 = "SELECT COUNT(*) FROM CB_PREPAID_UPLOAD_ALL_EDRS WHERE TRUNC(EVENT_DATE_TIME_DT)=TO_DATE(SYSDATE) AND EVENT_TYPE_V=6 and ATTRIBUTE1_V=10024"
    cursor.execute(query6)
    query6_result = cursor.fetchall()
    TotalDeafaultOffer = query6_result[0][0]
    
    
    # Close the database connection
    cursor.close()
    connection.close()
    
    # Convert query results to DataFrames
    df1 = pd.DataFrame(query1_result, columns=QUERY1_HEADERS)
    df2 = pd.DataFrame(query2_result, columns=QUERY2_HEADERS)
    df3 = pd.DataFrame(query3_result, columns=QUERY3_HEADERS)

    # Save DataFrames to CSV files
    df1.to_csv(QUERY1_OUTPUT, index=False)
    df2.to_csv(QUERY2_OUTPUT, index=False)
    df3.to_csv(QUERY3_OUTPUT, index=False)

    # Compose the email
    EMAIL_SUBJECT = "Daily Report - " + pd.Timestamp.now().strftime('%Y-%m-%d')
    EMAIL_BODY = f"""
            <html>
            <body>
                <p>Hi Sheetal,</p>
                <p>I hope this email finds you well. Attached to this email, you will find the daily reports for today, based on the queries we have been working on.</p>
                <p>Please find the below Summary:</p>

                <table style="border-collapse: collapse; width: 300px; margin: 20px 0;">
    <tr style="background-color: #F5B179; color: white;">
        <th style="padding: 2px; border: 2px solid #ddd;">Category</th>
        <th style="padding: 2px; border: 2px solid #ddd;">Value</th>
    </tr>
    <tr>
        <td style="padding: 2px; border: 2px solid #ddd;">Total Onboardings</td>
        <td style="padding: 2px; border: 2px solid #ddd;">{TotalOnboardings}</td><tr>
        <td style="padding: 2px; border: 2px solid #ddd;">Total Offer Activation</td>
        <td style="padding: 2px; border: 2px solid #ddd;">{TotalOffAct}</td><tr>
        <td style="padding: 2px; border: 2px solid #ddd;">Total Default Offer Activation</td>
        <td style="padding: 2px; border: 2px solid #ddd;">{TotalDeafaultOffer}</td>
    </tr>
</table>


            </body>
            </html>
            """
    
    msg = MIMEMultipart()
    msg['From'] = SENDER_EMAIL
    msg['To'] = ", ".join(RECIPIENT_EMAIL)
    msg['CC'] = ", ".join(CC_EMAILS)
    msg['Subject'] = EMAIL_SUBJECT
    msg.attach(MIMEText(EMAIL_BODY, 'html'))

    # Attach CSV files to the email
    for file in [QUERY1_OUTPUT, QUERY2_OUTPUT, QUERY3_OUTPUT]:
        with open(file, 'rb') as attachment:
            part = MIMEApplication(attachment.read(), Name=file)
        part['Content-Disposition'] = f'attachment; filename="{file}"'
        msg.attach(part)

    # Send the email
    with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
        server.starttls()
        server.login(SMTP_USERNAME, SMTP_PASSWORD)
        server.sendmail(SENDER_EMAIL, RECIPIENT_EMAIL + CC_EMAILS, msg.as_string())

    # Cleanup: Remove CSV files
    import os
    for file in [QUERY1_OUTPUT, QUERY2_OUTPUT, QUERY3_OUTPUT]:
        os.remove(file)
    print("Email sent successfully")
except Exception as e:
    print(f"Error sending email: {str(e)}")