import pymysql
import pandas as pd
from ProdConf import db_config
from ProdConf import username, password, host, port, service_name
import pysftp 
import os
from datetime import datetime, time, timedelta

current_date = datetime.now()
date = current_date.strftime('%Y-%m-%d')

mysql_cursor = None
mysql_connection = None

try:
    # Connect to MySQL
    mysql_connection = pymysql.connect(**db_config)
    mysql_cursor = mysql_connection.cursor()

    subOfferWalletDump = """SELECT Z.*,
                        (SELECT TARIFF_ID_N FROM TARIFF_MASTER WHERE PRODUCT_ID_N = Z.BASE_PRODUCT_ID_N) BASE_TARIFF_ID_N
                        FROM 
                        (SELECT (SELECT CONNECTION_ID_V FROM CONNECTION_ACCOUNTS A, CONNECTIONS B 
                        WHERE A.ACCOUNT_ID_N = B.ACCOUNT_ID_N
                        AND A.PAYER_ID_0_N = X.PAYER_ID_N AND A.ACCOUNT_STATUS_N = 1 ORDER BY B.START_DATE_DT DESC LIMIT 1)ACCOUNTID       
                        , X.WALLET_ID_N DA_ID
                        , (SELECT DESCRIPTION_V FROM WALLET_MASTER WHERE WALLET_ID_N = X.WALLET_ID_N) WALLET_DESCRIPTION
                        , Y.REDIS_BALANCE_N DA_VALUE
                        , EXPIRY_DATE_D DA_EXPIRY_DATE
                        ,(SELECT A.PRODUCT_ID_N FROM PAYER_TARIFFS A, PRODUCT_MASTER B
                        WHERE A.PRODUCT_ID_N =B.PRODUCT_ID_N AND B.PRODUCT_TYPE_V = 'B' AND X.PAYER_ID_N = A.PAYER_ID_N  ORDER BY A.START_DATE_DT DESC LIMIT 1)BASE_PRODUCT_ID_N
                        ,(SELECT B.DESCRIPTION_V FROM PAYER_TARIFFS A, PRODUCT_MASTER B
                        WHERE A.PRODUCT_ID_N =B.PRODUCT_ID_N AND B.PRODUCT_TYPE_V = 'B' AND X.PAYER_ID_N = A.PAYER_ID_N ORDER BY A.START_DATE_DT DESC LIMIT 1)BASE_PRODUCT_DESCRIPTION            
                        FROM PAYER_WALLETS X, PAYER_WALLETS_DUMP Y
                        WHERE X.PAYER_ID_N = Y.PAYER_ID_N 
                        AND X.WALLET_ID_N = Y.WALLET_ID_N
                        ) AS Z"""
    print("Query Execution started")
    mysql_cursor.execute(subOfferWalletDump)
    mysql_data = mysql_cursor.fetchall()
    print("Query Execution Completed")
    columns =['ACCOUNTID', 'DA_ID', 'WALLET_DESCRIPTION', 'DA_VALUE', 'DA_EXPIRY_DATE', 'BASE_PRODUCT_ID_N', 'BASE_PRODUCT_DESCRIPTION', 'BASE_TARIFF_ID_N']
    df =pd.DataFrame(mysql_data,columns=columns)

    csv_filename= f"SubscriberWalletDump_{date}.csv"
    downloads_folder = os.path.join(os.path.expanduser("~"), "C:\\Users\\poojaje\\Downloads\\OFFER_DUMP")
    csv_filename_path = os.path.join(downloads_folder, csv_filename)


    df.to_csv(csv_filename_path, index=False)
    csv_filename_path = f"C:\\Users\\poojaje\\Downloads\\OFFER_DUMP\\{csv_filename}"
    print("File Generated Successfully")

    #SFTP connection 3PP
    sftp_host = '172.16.112.103'
    sftp_port = 22  # Default SFTP port
    sftp_username = ''
    sftp_password = ''
    sftp_remote_dir = '/data01/data_lz/ftpin/WALLET_DUMP/'

    # Connect to SFTP server
    
    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None

    with pysftp.Connection(sftp_host, username=sftp_username, password=sftp_password, port=sftp_port) as sftp:
        # Change to the remote directory on the SFTP server
        sftp.chdir(sftp_remote_dir)

         # Upload the CSV file to the SFTP server
        sftp.put(csv_filename_path)

        print(f"CSV file '{csv_filename_path}' uploaded to SFTP successfully.")


except Exception as e:
    print(f"Error: {str(e)}")

finally:
    if mysql_cursor is not None:
        mysql_cursor.close()
    if mysql_connection is not None:
        mysql_connection.close()